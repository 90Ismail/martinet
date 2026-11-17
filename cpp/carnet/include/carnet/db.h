#pragma once

// carnet::DB — a leveled LSM-tree.
//
//   write  -> WAL (append, optional fsync) -> memtable (skiplist)
//   memtable full -> becomes immutable -> background flush -> L0 SSTable
//   L0 has >= N files            -> compact L0 + overlapping L1 into L1
//   Ln exceeds 10^n * base bytes -> compact one Ln file into Ln+1
//
// Reads check memtable, immutable memtable, L0 (newest first), then one file
// per level L1+ (levels >= 1 have non-overlapping key ranges). Bloom filters
// let most negative lookups skip SSTables entirely.

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "carnet/coding.h"
#include "carnet/memtable.h"
#include "carnet/sstable.h"
#include "carnet/status.h"
#include "carnet/wal.h"

namespace carnet {

struct Options {
  size_t memtable_bytes = 4 << 20;
  size_t block_size = 4096;
  int bloom_bits_per_key = 10;
  int l0_compaction_trigger = 4;
  int l0_stop_writes = 12;  // backpressure: stall writers past this
  uint64_t level1_bytes = 10ull << 20;
  uint64_t target_file_bytes = 2ull << 20;
  int num_levels = 4;
  bool background_work = true;  // false: tests drive Flush/Compact manually
};

class WriteBatch {
 public:
  void Put(std::string_view key, std::string_view value);
  void Delete(std::string_view key);
  uint32_t Count() const { return count_; }
  void Clear() {
    ops_.clear();
    count_ = 0;
  }

  // Serialized form: fixed64 seq | fixed32 count | { type | lp(key) | [lp(value)] }*
  std::string Encode(SequenceNumber seq) const;
  static Status Decode(std::string_view rep, SequenceNumber* seq,
                       const std::function<void(ValueType, std::string_view, std::string_view)>& fn);

 private:
  std::string ops_;
  uint32_t count_ = 0;
};

class Snapshot {
 public:
  SequenceNumber sequence() const { return seq_; }

 private:
  friend class DB;
  explicit Snapshot(SequenceNumber s) : seq_(s) {}
  SequenceNumber seq_;
  std::multiset<SequenceNumber>::iterator pos_;
};

struct DBStats {
  SequenceNumber last_sequence = 0;
  size_t memtable_bytes = 0;
  std::vector<std::pair<int, uint64_t>> levels;  // (files, bytes) per level
  uint64_t compactions = 0;
  uint64_t flushes = 0;
  uint64_t bloom_negatives = 0;
  uint64_t write_stalls = 0;
};

class DB {
 public:
  static Status Open(const Options& options, const std::string& dir, std::unique_ptr<DB>* out);
  ~DB();
  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

  Status Put(std::string_view key, std::string_view value, bool sync = false);
  Status Delete(std::string_view key, bool sync = false);
  Status Write(const WriteBatch& batch, bool sync = false, SequenceNumber* seq_out = nullptr);
  // Atomic check-and-set under the write lock.
  Status PutIfAbsent(std::string_view key, std::string_view value, bool sync, bool* written,
                     std::string* existing);

  Status Get(std::string_view key, std::string* value, const Snapshot* snap = nullptr);

  // Visits live user keys in [start, end) in order. end empty = unbounded.
  // fn returns false to stop early.
  Status Scan(std::string_view start, std::string_view end, size_t limit,
              const std::function<bool(std::string_view, std::string_view)>& fn,
              const Snapshot* snap = nullptr);

  const Snapshot* GetSnapshot();
  void ReleaseSnapshot(const Snapshot* s);

  // Synchronous maintenance, mainly for tests and the benchmark.
  Status Flush();
  Status CompactUntilStable();

  DBStats Stats();

 private:
  struct TableFile {
    FileMeta meta;
    std::shared_ptr<SSTable> table;
  };
  using FilePtr = std::shared_ptr<TableFile>;

  // An immutable snapshot of the file set. Readers grab a shared_ptr and read
  // without holding the DB mutex; compaction installs a new Version.
  struct Version {
    std::vector<std::vector<FilePtr>> levels;
  };

  struct Compaction {
    int level = 0;  // inputs from level and level+1
    std::vector<FilePtr> inputs[2];
  };

  DB(Options o, std::string dir);

  Status Recover();
  Status WriteManifest(const Version& v);
  Status NewLog();
  Status MakeRoomForWrite(std::unique_lock<std::mutex>& lock);
  Status WriteLocked(const WriteBatch& batch, bool sync, SequenceNumber* seq_out);
  Status GetImpl(std::string_view key, SequenceNumber seq, const std::shared_ptr<MemTable>& mem,
                 const std::shared_ptr<MemTable>& imm, const std::shared_ptr<Version>& v,
                 std::string* value);
  uint64_t MinLogNumber() const;  // oldest WAL still needed; requires mu_

  void BackgroundLoop();
  bool NeedsWork() const;  // requires mu_
  Status DoFlushLocked(std::unique_lock<std::mutex>& lock);
  Status DoCompactionLocked(std::unique_lock<std::mutex>& lock);
  bool PickCompaction(Compaction* c);  // requires mu_
  Status RunCompaction(const Compaction& c, SequenceNumber smallest_snapshot,
                       std::shared_ptr<Version> base, std::vector<FileMeta>* outputs);
  Status BuildTable(Iterator* it, std::vector<FileMeta>* outputs, bool split,
                    const std::function<bool(std::string_view, std::string_view)>& keep);
  bool IsBaseLevelForKey(const Version& v, int level, std::string_view user_key) const;

  std::string TablePath(uint64_t n) const;
  std::string LogPath(uint64_t n) const;
  uint64_t LevelBytes(const Version& v, int level) const;
  uint64_t MaxBytesForLevel(int level) const;
  void DeleteObsoleteFiles();  // requires mu_

  const Options options_;
  const std::string dir_;

  std::mutex mu_;
  std::condition_variable work_cv_;  // wakes the background thread
  std::condition_variable done_cv_;  // wakes stalled writers / waiters
  std::shared_ptr<MemTable> mem_;
  std::shared_ptr<MemTable> imm_;
  std::shared_ptr<Version> current_;
  std::unique_ptr<WalWriter> log_;
  uint64_t log_number_ = 0;       // current WAL
  uint64_t imm_log_number_ = 0;   // WAL backing imm_
  std::atomic<uint64_t> next_file_{1};
  SequenceNumber last_seq_ = 0;
  std::multiset<SequenceNumber> snapshots_;
  std::vector<size_t> compact_pointer_;  // round-robin file choice per level
  Status bg_error_;
  bool shutting_down_ = false;
  bool bg_busy_ = false;
  std::thread bg_thread_;

  std::atomic<uint64_t> compactions_{0};
  std::atomic<uint64_t> flushes_{0};
  std::atomic<uint64_t> bloom_negatives_{0};
  std::atomic<uint64_t> write_stalls_{0};
};

}  // namespace carnet
