#include "pouls/breaker.h"

#include <algorithm>

namespace pouls {

const char* StateName(State s) {
  switch (s) {
    case State::kClosed: return "CLOSED";
    case State::kOpen: return "OPEN";
    case State::kHalfOpen: return "HALF_OPEN";
  }
  return "?";
}

Health BreakerTable::Apply(const WindowResult& w) {
  std::lock_guard<std::mutex> l(mu_);
  Health& h = table_[w.endpoint];
  h.endpoint = w.endpoint;
  h.failure_rate = w.failure_rate();
  h.attempts = w.attempts;
  h.failures = w.failures;
  h.window_start_ms = w.start_ms;
  h.window_end_ms = w.end_ms;
  h.p99_latency_ms = w.p99_latency_ms;

  bool failing = w.failure_rate() >= cfg_.failure_threshold;
  switch (h.state) {
    case State::kClosed:
      if (failing && w.attempts >= cfg_.min_attempts) {
        h.state = State::kOpen;
        h.open_ms = cfg_.base_open_ms;
        h.open_until_ms = w.end_ms + h.open_ms;
      }
      break;
    case State::kOpen:
      // Couriers skip open endpoints, so windows here are mostly empty;
      // nothing to decide until the cool-down expires (see Tick).
      break;
    case State::kHalfOpen:
      // Half-open lets a trickle of probes through; any sample counts.
      if (w.attempts == 0) break;
      if (failing) {
        h.state = State::kOpen;
        h.open_ms = std::min(h.open_ms * 2, cfg_.max_open_ms);
        h.open_until_ms = w.end_ms + h.open_ms;
      } else {
        h.state = State::kClosed;
        h.open_ms = 0;
        h.open_until_ms = 0;
      }
      break;
  }
  return h;
}

std::vector<Health> BreakerTable::Tick(int64_t watermark_ms) {
  std::lock_guard<std::mutex> l(mu_);
  std::vector<Health> changed;
  for (auto& [ep, h] : table_) {
    if (h.state == State::kOpen && watermark_ms >= h.open_until_ms) {
      h.state = State::kHalfOpen;
      changed.push_back(h);
    }
  }
  return changed;
}

bool BreakerTable::Get(const std::string& endpoint, Health* out) const {
  std::lock_guard<std::mutex> l(mu_);
  auto it = table_.find(endpoint);
  if (it == table_.end()) return false;
  *out = it->second;
  return true;
}

std::vector<Health> BreakerTable::All() const {
  std::lock_guard<std::mutex> l(mu_);
  std::vector<Health> v;
  v.reserve(table_.size());
  for (const auto& [_, h] : table_) v.push_back(h);
  std::sort(v.begin(), v.end(), [](const Health& a, const Health& b) { return a.endpoint < b.endpoint; });
  return v;
}

}  // namespace pouls
