#pragma once

#include <memory>
#include <cstdint>
#include <string>

namespace RdKafka {
class KafkaConsumer;
class Producer;
}  // namespace RdKafka

namespace pouls {

struct KafkaConfig {
  std::string brokers = "localhost:9092";
  std::string group = "pouls";
  std::string attempts_topic = "attempts";
  std::string health_topic = "endpoint-health";
};

struct Consumed {
  int32_t partition = 0;
  int64_t offset = 0;
  std::string payload;
};

// Thin RAII wrapper over librdkafka's C++ API: one consumer for `attempts`,
// one idempotent producer for `endpoint-health`.
class KafkaIO {
 public:
  static std::unique_ptr<KafkaIO> Create(const KafkaConfig& cfg, std::string* err);
  ~KafkaIO();

  // Returns true and fills *out if a message arrived within timeout_ms.
  bool Poll(int timeout_ms, Consumed* out);
  // Keyed by endpoint: the health topic is log-compacted, so Kafka keeps the
  // latest record per endpoint forever (a changelog, DDIA ch. 11).
  bool Produce(const std::string& key, const std::string& value);
  void Flush(int timeout_ms);
  uint64_t produce_errors() const { return produce_errors_; }

 private:
  KafkaIO() = default;
  KafkaConfig cfg_;
  std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
  std::unique_ptr<RdKafka::Producer> producer_;
  uint64_t produce_errors_ = 0;
};

}  // namespace pouls
