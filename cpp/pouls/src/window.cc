#include "pouls/window.h"

#include <algorithm>
#include <climits>
#include <cmath>

namespace pouls {

// ---------------------------------------------------------------- histogram

int LatencyHistogram::BucketFor(double ms) {
  if (ms <= 1) return 0;
  // 4 buckets per power of two -> ~19% relative error at worst.
  int b = static_cast<int>(std::log2(ms) * 4.0);
  return std::clamp(b, 0, kBuckets - 1);
}

double LatencyHistogram::BucketUpper(int b) { return std::pow(2.0, (b + 1) / 4.0); }

void LatencyHistogram::Record(double ms) {
  buckets_[BucketFor(ms)]++;
  count_++;
}

double LatencyHistogram::Quantile(double q) const {
  if (count_ == 0) return 0;
  uint64_t rank = static_cast<uint64_t>(std::ceil(q * count_));
  uint64_t seen = 0;
  for (int b = 0; b < kBuckets; b++) {
    seen += buckets_[b];
    if (seen >= rank) return BucketUpper(b);
  }
  return BucketUpper(kBuckets - 1);
}

// ---------------------------------------------------------------- watermark

void WatermarkTracker::Observe(int32_t partition, int64_t event_time_ms, int64_t wall_ms) {
  auto [it, inserted] = parts_.try_emplace(partition, PartitionState{event_time_ms, wall_ms});
  if (!inserted) {
    it->second.max_event_ms = std::max(it->second.max_event_ms, event_time_ms);
    it->second.last_seen_wall_ms = wall_ms;
  }
}

int64_t WatermarkTracker::Current(int64_t wall_ms) const {
  int64_t low = INT64_MAX;
  int64_t newest_idle = INT64_MIN;
  for (const auto& [p, st] : parts_) {
    if (wall_ms - st.last_seen_wall_ms > idle_timeout_) {
      newest_idle = std::max(newest_idle, st.max_event_ms);
      continue;
    }
    low = std::min(low, st.max_event_ms);
  }
  if (low == INT64_MAX) {
    // Every partition is idle: let time advance with the wall clock so open
    // windows (and open circuit breakers) do not get stuck.
    if (newest_idle == INT64_MIN) return INT64_MIN;
    return std::max(newest_idle, wall_ms - idle_timeout_) - lateness_;
  }
  return low - lateness_;
}

// ---------------------------------------------------------------- windows

int64_t TumblingWindows::WindowStart(int64_t t) const {
  int64_t r = t % window_ms_;
  if (r < 0) r += window_ms_;
  return t - r;
}

bool TumblingWindows::Add(const AttemptSample& s) {
  int64_t start = WindowStart(s.event_time_ms);
  if (start < closed_until_) {
    late_++;
    return false;
  }
  Acc& a = windows_[start][s.endpoint];
  a.attempts++;
  if (s.failure) a.failures++;
  a.latency.Record(s.latency_ms);
  return true;
}

std::vector<WindowResult> TumblingWindows::Close(int64_t watermark_ms) {
  std::vector<WindowResult> out;
  if (watermark_ms == INT64_MIN) return out;
  while (!windows_.empty()) {
    auto it = windows_.begin();
    int64_t start = it->first;
    int64_t end = start + window_ms_;
    if (end > watermark_ms) break;
    for (auto& [endpoint, acc] : it->second) {
      WindowResult r;
      r.endpoint = endpoint;
      r.start_ms = start;
      r.end_ms = end;
      r.attempts = acc.attempts;
      r.failures = acc.failures;
      r.p99_latency_ms = acc.latency.Quantile(0.99);
      out.push_back(std::move(r));
    }
    windows_.erase(it);
  }
  closed_until_ = std::max(closed_until_, WindowStart(watermark_ms));
  // Order by (window, endpoint) so output is deterministic for tests/replays.
  std::sort(out.begin(), out.end(), [](const WindowResult& a, const WindowResult& b) {
    return a.start_ms != b.start_ms ? a.start_ms < b.start_ms : a.endpoint < b.endpoint;
  });
  return out;
}

}  // namespace pouls
