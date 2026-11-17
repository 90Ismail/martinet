#include "pouls/kafka.h"

#include <librdkafka/rdkafkacpp.h>

#include <cstdio>

namespace pouls {
namespace {

bool Set(RdKafka::Conf* c, const std::string& k, const std::string& v, std::string* err) {
  if (c->set(k, v, *err) != RdKafka::Conf::CONF_OK) {
    *err = k + ": " + *err;
    return false;
  }
  return true;
}

class RebalanceLogger : public RdKafka::RebalanceCb {
 public:
  void rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
                    std::vector<RdKafka::TopicPartition*>& parts) override {
    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      std::fprintf(stderr, "pouls: assigned %zu partitions\n", parts.size());
      consumer->assign(parts);
    } else {
      std::fprintf(stderr, "pouls: revoked %zu partitions\n", parts.size());
      consumer->unassign();
    }
  }
};

RebalanceLogger g_rebalance;

}  // namespace

std::unique_ptr<KafkaIO> KafkaIO::Create(const KafkaConfig& cfg, std::string* err) {
  std::unique_ptr<KafkaIO> io(new KafkaIO());
  io->cfg_ = cfg;

  std::unique_ptr<RdKafka::Conf> cc(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (!Set(cc.get(), "bootstrap.servers", cfg.brokers, err) || !Set(cc.get(), "group.id", cfg.group, err) ||
      !Set(cc.get(), "auto.offset.reset", "earliest", err) ||
      // Health is advisory and self-correcting within one window, so
      // at-least-once with periodic auto-commit is an acceptable trade-off
      // here (see README "Trade-offs").
      !Set(cc.get(), "enable.auto.commit", "true", err) ||
      !Set(cc.get(), "auto.commit.interval.ms", "5000", err)) {
    return nullptr;
  }
  if (cc->set("rebalance_cb", &g_rebalance, *err) != RdKafka::Conf::CONF_OK) return nullptr;
  io->consumer_.reset(RdKafka::KafkaConsumer::create(cc.get(), *err));
  if (!io->consumer_) return nullptr;
  if (auto e = io->consumer_->subscribe({cfg.attempts_topic}); e != RdKafka::ERR_NO_ERROR) {
    *err = RdKafka::err2str(e);
    return nullptr;
  }

  std::unique_ptr<RdKafka::Conf> pc(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (!Set(pc.get(), "bootstrap.servers", cfg.brokers, err) || !Set(pc.get(), "enable.idempotence", "true", err) ||
      !Set(pc.get(), "linger.ms", "20", err) || !Set(pc.get(), "compression.type", "lz4", err)) {
    return nullptr;
  }
  io->producer_.reset(RdKafka::Producer::create(pc.get(), *err));
  if (!io->producer_) return nullptr;
  return io;
}

KafkaIO::~KafkaIO() {
  if (producer_) producer_->flush(5000);
  if (consumer_) consumer_->close();
}

bool KafkaIO::Poll(int timeout_ms, Consumed* out) {
  producer_->poll(0);  // serve delivery reports
  std::unique_ptr<RdKafka::Message> m(consumer_->consume(timeout_ms));
  switch (m->err()) {
    case RdKafka::ERR_NO_ERROR:
      out->partition = m->partition();
      out->offset = m->offset();
      out->payload.assign(static_cast<const char*>(m->payload()), m->len());
      return true;
    case RdKafka::ERR__TIMED_OUT:
    case RdKafka::ERR__PARTITION_EOF:
      return false;
    default:
      std::fprintf(stderr, "pouls: consume error: %s\n", m->errstr().c_str());
      return false;
  }
}

bool KafkaIO::Produce(const std::string& key, const std::string& value) {
  auto e = producer_->produce(cfg_.health_topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
                              const_cast<char*>(value.data()), value.size(), key.data(), key.size(), 0, nullptr);
  if (e != RdKafka::ERR_NO_ERROR) {
    produce_errors_++;
    std::fprintf(stderr, "pouls: produce error: %s\n", RdKafka::err2str(e).c_str());
    return false;
  }
  return true;
}

void KafkaIO::Flush(int timeout_ms) { producer_->flush(timeout_ms); }

}  // namespace pouls
