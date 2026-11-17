#pragma once

// Per-endpoint circuit breaker, driven by windowed failure rates.
//
//   CLOSED --(failure_rate >= threshold, enough samples)--> OPEN
//   OPEN   --(open_until reached)-----------------------------> HALF_OPEN
//   HALF_OPEN --(window healthy)--> CLOSED
//   HALF_OPEN --(window still failing)--> OPEN, cool-down doubled (capped)
//
// All transitions are evaluated against event time (the watermark), not the
// wall clock, so replaying the attempts log reproduces the same decisions.

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "pouls/window.h"

namespace pouls {

enum class State { kClosed, kOpen, kHalfOpen };
const char* StateName(State s);

struct BreakerConfig {
  double failure_threshold = 0.5;
  uint64_t min_attempts = 20;
  int64_t base_open_ms = 30'000;
  int64_t max_open_ms = 10 * 60'000;
};

struct Health {
  std::string endpoint;
  State state = State::kClosed;
  double failure_rate = 0;
  uint64_t attempts = 0;
  uint64_t failures = 0;
  int64_t window_start_ms = 0;
  int64_t window_end_ms = 0;
  int64_t open_until_ms = 0;
  int64_t open_ms = 0;  // current cool-down length
  double p99_latency_ms = 0;
};

class BreakerTable {
 public:
  explicit BreakerTable(BreakerConfig cfg) : cfg_(cfg) {}

  // Applies one closed window. Returns the new health record (always emitted
  // so dashboards see fresh failure rates, even without a state change).
  Health Apply(const WindowResult& w);

  // Time-based transitions (OPEN -> HALF_OPEN). Returns records that changed.
  std::vector<Health> Tick(int64_t watermark_ms);

  bool Get(const std::string& endpoint, Health* out) const;
  std::vector<Health> All() const;

 private:
  BreakerConfig cfg_;
  mutable std::mutex mu_;  // read by the gRPC thread
  std::unordered_map<std::string, Health> table_;
};

}  // namespace pouls
