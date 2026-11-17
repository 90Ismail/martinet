// carnet_bench: write/read throughput and the effect of bloom filters.
//
//   ./carnet_bench [num_keys] [value_bytes]
//
// Prints ops/sec for sequential writes, random writes, random reads of
// present keys, and random reads of absent keys (bloom filter path).

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <random>
#include <string>

#include "carnet/db.h"

using Clock = std::chrono::steady_clock;

namespace {

std::string Key(uint64_t i) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%016llu", static_cast<unsigned long long>(i));
  return buf;
}

template <typename F>
double Run(const char* name, uint64_t n, F&& f) {
  auto t0 = Clock::now();
  for (uint64_t i = 0; i < n; i++) f(i);
  double secs = std::chrono::duration<double>(Clock::now() - t0).count();
  double ops = n / secs;
  std::printf("%-22s %10llu ops  %8.2f s  %12.0f ops/s  %7.2f us/op\n", name,
              static_cast<unsigned long long>(n), secs, ops, 1e6 / ops);
  return ops;
}

}  // namespace

int main(int argc, char** argv) {
  uint64_t n = argc > 1 ? std::stoull(argv[1]) : 200000;
  size_t vsize = argc > 2 ? std::stoull(argv[2]) : 100;
  std::string dir = (std::filesystem::temp_directory_path() / "carnet-bench").string();
  std::filesystem::remove_all(dir);

  carnet::Options opts;
  std::unique_ptr<carnet::DB> db;
  auto s = carnet::DB::Open(opts, dir, &db);
  if (!s.ok()) {
    std::fprintf(stderr, "%s\n", s.ToString().c_str());
    return 1;
  }
  std::string value(vsize, 'v');
  std::mt19937_64 rng(1);

  std::printf("carnet bench: %llu keys, %zu byte values\n\n", static_cast<unsigned long long>(n), vsize);
  Run("fillseq", n, [&](uint64_t i) { db->Put(Key(2 * i), value); });  // even keys only
  Run("overwrite (random)", n, [&](uint64_t) { db->Put(Key(2 * (rng() % n)), value); });
  db->CompactUntilStable();

  std::string out;
  uint64_t hits = 0;
  Run("readrandom (present)", n, [&](uint64_t) { hits += db->Get(Key(2 * (rng() % n)), &out).ok(); });
  auto before = db->Stats().bloom_negatives;
  Run("readmissing (bloom)", n, [&](uint64_t) { db->Get(Key(2 * (rng() % n) + 1), &out); });  // odd: absent, inside ranges
  auto st = db->Stats();

  std::printf("\nhits=%llu  bloom-skipped SST probes=%llu  flushes=%llu  compactions=%llu\n",
              static_cast<unsigned long long>(hits),
              static_cast<unsigned long long>(st.bloom_negatives - before),
              static_cast<unsigned long long>(st.flushes), static_cast<unsigned long long>(st.compactions));
  for (size_t l = 0; l < st.levels.size(); l++) {
    std::printf("  L%zu: %3d files  %8.2f MiB\n", l, st.levels[l].first, st.levels[l].second / 1048576.0);
  }
  db.reset();
  std::filesystem::remove_all(dir);
  return 0;
}
