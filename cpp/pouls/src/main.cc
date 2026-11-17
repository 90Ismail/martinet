// pouls: consumes the `attempts` log, computes per-endpoint failure rates in
// event-time tumbling windows, runs the circuit-breaker state machine, and
// publishes EndpointHealth to the compacted `endpoint-health` topic that the
// Go couriers read.

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "martinet/v1/martinet.pb.h"
#include "pouls/breaker.h"
#include "pouls/health_server.h"
#include "pouls/kafka.h"
#include "pouls/window.h"

namespace {

volatile std::sig_atomic_t g_stop = 0;
void OnSignal(int) { g_stop = 1; }

std::string Env(const char* k, const char* def) {
  const char* v = std::getenv(k);
  return v && *v ? v : def;
}
int64_t EnvInt(const char* k, int64_t def) { return std::stoll(Env(k, std::to_string(def).c_str())); }

int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

}  // namespace

int main() {
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  pouls::KafkaConfig kc;
  kc.brokers = Env("KAFKA_BROKERS", "localhost:9092");
  kc.group = Env("POULS_GROUP", "pouls");
  kc.attempts_topic = Env("ATTEMPTS_TOPIC", "attempts");
  kc.health_topic = Env("HEALTH_TOPIC", "endpoint-health");

  const int64_t window_ms = EnvInt("POULS_WINDOW_MS", 10'000);
  const int64_t lateness_ms = EnvInt("POULS_ALLOWED_LATENESS_MS", 5'000);
  const int64_t idle_ms = EnvInt("POULS_IDLE_PARTITION_MS", 15'000);

  pouls::BreakerConfig bc;
  bc.failure_threshold = std::stod(Env("POULS_FAILURE_THRESHOLD", "0.5"));
  bc.min_attempts = static_cast<uint64_t>(EnvInt("POULS_MIN_ATTEMPTS", 20));
  bc.base_open_ms = EnvInt("POULS_OPEN_MS", 30'000);

  std::string err;
  auto kafka = pouls::KafkaIO::Create(kc, &err);
  if (!kafka) {
    std::fprintf(stderr, "pouls: kafka: %s\n", err.c_str());
    return 1;
  }

  pouls::WatermarkTracker wm(lateness_ms, idle_ms);
  pouls::TumblingWindows windows(window_ms);
  pouls::BreakerTable breakers(bc);
  std::atomic<int64_t> watermark{INT64_MIN};
  std::atomic<uint64_t> late{0};

  pouls::HealthServer grpc_server(&breakers, &watermark, &late);
  std::string addr = Env("POULS_GRPC_ADDR", "0.0.0.0:7071");
  if (!grpc_server.Start(addr)) {
    std::fprintf(stderr, "pouls: cannot listen on %s\n", addr.c_str());
    return 1;
  }
  std::fprintf(stderr, "pouls: window=%lldms lateness=%lldms grpc=%s brokers=%s\n",
               static_cast<long long>(window_ms), static_cast<long long>(lateness_ms), addr.c_str(),
               kc.brokers.c_str());

  auto publish = [&](const pouls::Health& h) {
    std::string bytes;
    if (!pouls::ToProto(h, late.load()).SerializeToString(&bytes)) return;
    kafka->Produce(h.endpoint, bytes);
  };

  uint64_t consumed = 0, bad = 0;
  int64_t last_tick = 0;
  martinet::v1::DeliveryAttempt a;
  pouls::Consumed msg;
  while (!g_stop) {
    if (kafka->Poll(200, &msg)) {
      consumed++;
      if (!a.ParseFromString(msg.payload)) {
        bad++;  // poison pill: skip, never block the partition
        continue;
      }
      int64_t now = NowMs();
      wm.Observe(msg.partition, a.attempted_at_ms(), now);
      // Skipped-by-breaker attempts advance time but are not evidence about
      // the endpoint, so they do not count towards its failure rate.
      if (a.outcome() != martinet::v1::ATTEMPT_OUTCOME_CIRCUIT_OPEN) {
        pouls::AttemptSample s;
        s.endpoint = a.endpoint();
        s.event_time_ms = a.attempted_at_ms();
        s.failure = a.outcome() != martinet::v1::ATTEMPT_OUTCOME_SUCCESS;
        s.latency_ms = static_cast<double>(a.latency_ms());
        if (!windows.Add(s)) late = windows.late_events();
      }
    }

    int64_t now = NowMs();
    if (now - last_tick < 250) continue;
    last_tick = now;
    int64_t w = wm.Current(now);
    watermark = w;
    for (const auto& r : windows.Close(w)) {
      pouls::Health h = breakers.Apply(r);
      publish(h);
      if (h.state != pouls::State::kClosed) {
        std::fprintf(stderr, "pouls: %s %s rate=%.2f n=%llu\n", h.endpoint.c_str(), pouls::StateName(h.state),
                     h.failure_rate, static_cast<unsigned long long>(h.attempts));
      }
    }
    for (const auto& h : breakers.Tick(w)) {
      std::fprintf(stderr, "pouls: %s -> HALF_OPEN\n", h.endpoint.c_str());
      publish(h);
    }
  }

  std::fprintf(stderr, "pouls: shutting down (consumed=%llu bad=%llu late=%llu)\n",
               static_cast<unsigned long long>(consumed), static_cast<unsigned long long>(bad),
               static_cast<unsigned long long>(late.load()));
  grpc_server.Shutdown();
  kafka->Flush(5000);
  return 0;
}
