#pragma once

// Event-time windowing (DDIA ch. 11, "Reasoning About Time").
//
// Attempts arrive out of order: workers run on different pods, Kafka
// partitions are consumed at different speeds, and a pod can stall for a GC
// pause or a rebalance. So pouls buckets attempts by the time they *happened*
// (attempted_at_ms), not the time pouls saw them, and decides a window is
// complete using a watermark:
//
//   watermark = min over active partitions (max event time seen) - lateness
//
// A window [start, end) is emitted once watermark >= end. Events that arrive
// for an already-emitted window are "late": counted, not silently merged.

#include <array>
#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

namespace pouls {

struct AttemptSample {
  std::string endpoint;
  int64_t event_time_ms = 0;
  bool failure = false;
  double latency_ms = 0;
};

// Fixed log-scale latency histogram: 1ms .. ~65s in 64 buckets. Mergeable
// and O(1) memory per window, unlike keeping every sample.
class LatencyHistogram {
 public:
  void Record(double ms);
  double Quantile(double q) const;
  uint64_t count() const { return count_; }

 private:
  static constexpr int kBuckets = 64;
  static int BucketFor(double ms);
  static double BucketUpper(int b);
  std::array<uint64_t, kBuckets> buckets_{};
  uint64_t count_ = 0;
};

struct WindowResult {
  std::string endpoint;
  int64_t start_ms = 0;
  int64_t end_ms = 0;
  uint64_t attempts = 0;
  uint64_t failures = 0;
  double p99_latency_ms = 0;
  double failure_rate() const { return attempts ? static_cast<double>(failures) / attempts : 0.0; }
};

// Tracks per-partition progress. Partitions that have been idle longer than
// idle_timeout_ms are ignored, otherwise one quiet partition would hold the
// watermark back forever.
class WatermarkTracker {
 public:
  WatermarkTracker(int64_t allowed_lateness_ms, int64_t idle_timeout_ms)
      : lateness_(allowed_lateness_ms), idle_timeout_(idle_timeout_ms) {}

  void Observe(int32_t partition, int64_t event_time_ms, int64_t wall_ms);
  // Returns INT64_MIN until at least one partition has been observed.
  int64_t Current(int64_t wall_ms) const;

 private:
  struct PartitionState {
    int64_t max_event_ms;
    int64_t last_seen_wall_ms;
  };
  int64_t lateness_;
  int64_t idle_timeout_;
  std::unordered_map<int32_t, PartitionState> parts_;
};

class TumblingWindows {
 public:
  explicit TumblingWindows(int64_t window_ms) : window_ms_(window_ms) {}

  // Returns false (and counts it) if the event's window was already emitted.
  bool Add(const AttemptSample& s);

  // Emits and forgets every window with end <= watermark.
  std::vector<WindowResult> Close(int64_t watermark_ms);

  int64_t WindowStart(int64_t t) const;
  uint64_t late_events() const { return late_; }
  size_t open_windows() const { return windows_.size(); }

 private:
  struct Acc {
    uint64_t attempts = 0;
    uint64_t failures = 0;
    LatencyHistogram latency;
  };
  int64_t window_ms_;
  int64_t closed_until_ = INT64_MIN;  // everything before this is emitted
  uint64_t late_ = 0;
  // ordered by window start so Close() walks from the oldest window
  std::map<int64_t, std::unordered_map<std::string, Acc>> windows_;
};

}  // namespace pouls
