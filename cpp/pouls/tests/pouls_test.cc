#include <gtest/gtest.h>

#include <climits>

#include "pouls/breaker.h"
#include "pouls/window.h"

using namespace pouls;

namespace {
AttemptSample S(const std::string& ep, int64_t t, bool fail, double lat = 10) {
  return AttemptSample{ep, t, fail, lat};
}
}  // namespace

TEST(Windows, BucketsByEventTimeNotArrivalOrder) {
  TumblingWindows w(1000);
  // Arrive out of order across two windows.
  EXPECT_TRUE(w.Add(S("a", 1500, false)));
  EXPECT_TRUE(w.Add(S("a", 200, true)));
  EXPECT_TRUE(w.Add(S("a", 1999, true)));
  EXPECT_TRUE(w.Add(S("a", 999, false)));

  auto out = w.Close(1000);  // watermark at end of first window
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].start_ms, 0);
  EXPECT_EQ(out[0].attempts, 2u);
  EXPECT_EQ(out[0].failures, 1u);

  out = w.Close(2000);
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].start_ms, 1000);
  EXPECT_DOUBLE_EQ(out[0].failure_rate(), 0.5);
}

TEST(Windows, LateEventsAreCountedNotMerged) {
  TumblingWindows w(1000);
  w.Add(S("a", 100, false));
  w.Close(1000);
  EXPECT_FALSE(w.Add(S("a", 900, true)));  // window [0,1000) already emitted
  EXPECT_EQ(w.late_events(), 1u);
  EXPECT_TRUE(w.Add(S("a", 1100, true)));
}

TEST(Windows, NegativeTimestampsBucketCorrectly) {
  TumblingWindows w(1000);
  EXPECT_EQ(w.WindowStart(-1), -1000);
  EXPECT_EQ(w.WindowStart(0), 0);
}

TEST(Watermark, MinAcrossPartitionsMinusLateness) {
  WatermarkTracker t(/*lateness=*/500, /*idle=*/10'000);
  EXPECT_EQ(t.Current(0), INT64_MIN);
  t.Observe(0, 5000, 0);
  t.Observe(1, 3000, 0);
  EXPECT_EQ(t.Current(0), 2500);  // slow partition holds it back
  t.Observe(1, 6000, 0);
  EXPECT_EQ(t.Current(0), 4500);
}

TEST(Watermark, IdlePartitionDoesNotStallProgress) {
  WatermarkTracker t(0, 1000);
  t.Observe(0, 100, 0);    // partition 0 goes quiet
  t.Observe(1, 9000, 1500);
  EXPECT_EQ(t.Current(1600), 9000);
}

TEST(Histogram, P99WithinBucketError) {
  LatencyHistogram h;
  for (int i = 1; i <= 1000; i++) h.Record(i);
  double p99 = h.Quantile(0.99);
  EXPECT_GT(p99, 990 * 0.8);
  EXPECT_LT(p99, 990 * 1.25);
}

TEST(Breaker, OpensHalfOpensAndCloses) {
  BreakerConfig c;
  c.min_attempts = 10;
  c.base_open_ms = 5000;
  BreakerTable b(c);

  WindowResult bad{"api.example.com", 0, 1000, 20, 15, 50};
  Health h = b.Apply(bad);
  EXPECT_EQ(h.state, State::kOpen);
  EXPECT_EQ(h.open_until_ms, 6000);

  EXPECT_TRUE(b.Tick(5999).empty());
  auto changed = b.Tick(6000);
  ASSERT_EQ(changed.size(), 1u);
  EXPECT_EQ(changed[0].state, State::kHalfOpen);

  WindowResult good{"api.example.com", 6000, 7000, 3, 0, 20};
  EXPECT_EQ(b.Apply(good).state, State::kClosed);
}

TEST(Breaker, HalfOpenFailureDoublesCooldown) {
  BreakerConfig c;
  c.min_attempts = 1;
  c.base_open_ms = 1000;
  c.max_open_ms = 3000;
  BreakerTable b(c);
  b.Apply({"e", 0, 1000, 5, 5, 0});
  b.Tick(2000);
  Health h = b.Apply({"e", 2000, 3000, 1, 1, 0});
  EXPECT_EQ(h.state, State::kOpen);
  EXPECT_EQ(h.open_ms, 2000);
  b.Tick(5000);
  h = b.Apply({"e", 5000, 6000, 1, 1, 0});
  EXPECT_EQ(h.open_ms, 3000);  // capped
}

TEST(Breaker, NeedsMinimumSampleSize) {
  BreakerConfig c;
  c.min_attempts = 20;
  BreakerTable b(c);
  // 100% failure over 3 attempts is noise, not an outage.
  EXPECT_EQ(b.Apply({"e", 0, 1000, 3, 3, 0}).state, State::kClosed);
}
