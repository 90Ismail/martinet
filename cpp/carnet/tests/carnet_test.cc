#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <thread>

#include "carnet/bloom.h"
#include "carnet/crc32c.h"
#include "carnet/db.h"
#include "carnet/memtable.h"
#include "carnet/sstable.h"
#include "carnet/wal.h"

namespace fs = std::filesystem;
using namespace carnet;

namespace {

class TempDir {
 public:
  TempDir() {
    path_ = fs::temp_directory_path() / ("carnet-test-" + std::to_string(::getpid()) + "-" + std::to_string(counter_++));
    fs::remove_all(path_);
    fs::create_directories(path_);
  }
  ~TempDir() { fs::remove_all(path_); }
  std::string str() const { return path_.string(); }

 private:
  static inline int counter_ = 0;
  fs::path path_;
};

std::string Key(int i) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "key%08d", i);
  return buf;
}

Options SmallOptions() {
  Options o;
  o.memtable_bytes = 16 << 10;  // force many flushes
  o.target_file_bytes = 8 << 10;
  o.level1_bytes = 64 << 10;
  o.l0_compaction_trigger = 3;
  o.background_work = false;
  return o;
}

}  // namespace

TEST(Coding, InternalKeyOrderNewestFirst) {
  auto a = MakeInternalKey("k", 5, ValueType::kPut);
  auto b = MakeInternalKey("k", 9, ValueType::kPut);
  auto c = MakeInternalKey("l", 1, ValueType::kPut);
  EXPECT_LT(CompareInternalKey(b, a), 0);  // seq 9 before seq 5
  EXPECT_LT(CompareInternalKey(a, c), 0);
  EXPECT_EQ(ExtractSequence(b), 9u);
  EXPECT_EQ(ExtractUserKey(c), "l");
}

TEST(Crc32c, KnownVector) {
  // Standard check value for CRC-32C over "123456789".
  EXPECT_EQ(crc32c::Value("123456789", 9), 0xe3069283u);
  uint32_t c = crc32c::Value("hello", 5);
  EXPECT_EQ(crc32c::Unmask(crc32c::Mask(c)), c);
}

TEST(MemTable, NewestVisibleVersion) {
  MemTable m;
  m.Add(1, ValueType::kPut, "a", "v1");
  m.Add(2, ValueType::kPut, "a", "v2");
  m.Add(3, ValueType::kDelete, "a", "");
  std::string v;
  bool del;
  ASSERT_TRUE(m.Get("a", 3, &v, &del));
  EXPECT_TRUE(del);
  ASSERT_TRUE(m.Get("a", 2, &v, &del));
  EXPECT_FALSE(del);
  EXPECT_EQ(v, "v2");
  ASSERT_TRUE(m.Get("a", 1, &v, &del));
  EXPECT_EQ(v, "v1");
  EXPECT_FALSE(m.Get("b", 3, &v, &del));
}

TEST(Bloom, NoFalseNegativesAndLowFalsePositives) {
  BloomFilterBuilder b(10);
  for (int i = 0; i < 10000; i++) b.AddKey(Key(i));
  std::string f = b.Finish();
  for (int i = 0; i < 10000; i++) ASSERT_TRUE(BloomMayContain(f, Key(i)));
  int fp = 0;
  for (int i = 10000; i < 20000; i++) fp += BloomMayContain(f, Key(i));
  // 10 bits/key -> ~1% theoretical; allow slack.
  EXPECT_LT(fp, 300) << "false positive rate " << fp / 100.0 << "%";
}

TEST(Wal, ReplaysIntactRecordsAndDropsTornTail) {
  TempDir d;
  std::string path = d.str() + "/000001.log";
  {
    std::unique_ptr<WalWriter> w;
    ASSERT_TRUE(WalWriter::Open(path, &w).ok());
    for (int i = 0; i < 100; i++) ASSERT_TRUE(w->Append("record-" + std::to_string(i), false).ok());
  }
  // Simulate a crash mid-write: append half a record.
  {
    std::ofstream f(path, std::ios::binary | std::ios::app);
    f.write("\x01\x02\x03\x04\xff\x00", 6);
  }
  std::vector<std::string> got;
  WalReplayStats st;
  ASSERT_TRUE(ReplayWal(path, [&](std::string_view r) { got.emplace_back(r); return Status::OK(); }, &st).ok());
  ASSERT_EQ(got.size(), 100u);
  EXPECT_EQ(got[99], "record-99");
  EXPECT_EQ(st.dropped_bytes, 6u);
}

TEST(Wal, StopsAtCorruptRecord) {
  TempDir d;
  std::string path = d.str() + "/000001.log";
  {
    std::unique_ptr<WalWriter> w;
    ASSERT_TRUE(WalWriter::Open(path, &w).ok());
    for (int i = 0; i < 10; i++) ASSERT_TRUE(w->Append("rec" + std::to_string(i), false).ok());
  }
  // Flip a byte inside the 6th record's payload.
  {
    std::fstream f(path, std::ios::binary | std::ios::in | std::ios::out);
    f.seekp(5 * (8 + 4) + 9);
    f.put('X');
  }
  int n = 0;
  ASSERT_TRUE(ReplayWal(path, [&](std::string_view) { n++; return Status::OK(); }, nullptr).ok());
  EXPECT_EQ(n, 5);
}

TEST(SSTable, RoundTripPointAndScan) {
  TempDir d;
  std::string path = d.str() + "/000007.sst";
  SSTableBuilder b(path, 512, 10);
  ASSERT_TRUE(b.Open().ok());
  for (int i = 0; i < 2000; i++) {
    ASSERT_TRUE(b.Add(MakeInternalKey(Key(i * 2), 100 + i, ValueType::kPut), "v" + std::to_string(i)).ok());
  }
  FileMeta m;
  ASSERT_TRUE(b.Finish(&m).ok());
  EXPECT_EQ(m.entries, 2000u);

  std::shared_ptr<SSTable> t;
  ASSERT_TRUE(SSTable::Open(path, 7, &t).ok());
  EXPECT_GT(t->index().size(), 10u);  // multiple blocks -> sparse index exercised

  std::string v;
  bool found, del, skip;
  ASSERT_TRUE(t->Get(Key(1000), kMaxSequence, &v, &found, &del, &skip).ok());
  ASSERT_TRUE(found);
  EXPECT_EQ(v, "v500");
  ASSERT_TRUE(t->Get(Key(1001), kMaxSequence, &v, &found, &del, &skip).ok());
  EXPECT_FALSE(found);

  auto it = t->NewIterator();
  int n = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) n++;
  EXPECT_EQ(n, 2000);
  it->Seek(MakeInternalKey(Key(3), kMaxSequence, ValueType::kPut));
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(ExtractUserKey(it->key()), Key(4));
}

TEST(SSTable, DetectsBlockCorruption) {
  TempDir d;
  std::string path = d.str() + "/000001.sst";
  SSTableBuilder b(path, 4096, 10);
  ASSERT_TRUE(b.Open().ok());
  ASSERT_TRUE(b.Add(MakeInternalKey("hello", 1, ValueType::kPut), "world").ok());
  FileMeta m;
  ASSERT_TRUE(b.Finish(&m).ok());
  {
    std::fstream f(path, std::ios::binary | std::ios::in | std::ios::out);
    f.seekp(3);
    f.put('!');
  }
  std::shared_ptr<SSTable> t;
  ASSERT_TRUE(SSTable::Open(path, 1, &t).ok());
  std::string v;
  bool found, del, skip;
  EXPECT_TRUE(t->Get("hello", kMaxSequence, &v, &found, &del, &skip).IsCorruption());
}

TEST(DB, PutGetDeleteAcrossFlushesAndCompactions) {
  TempDir d;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(SmallOptions(), d.str(), &db).ok());
  std::map<std::string, std::string> model;
  std::mt19937 rng(42);
  for (int i = 0; i < 20000; i++) {
    std::string k = Key(rng() % 3000);
    if (rng() % 5 == 0) {
      ASSERT_TRUE(db->Delete(k).ok());
      model.erase(k);
    } else {
      std::string v = "value-" + std::to_string(i) + std::string(rng() % 40, 'x');
      ASSERT_TRUE(db->Put(k, v).ok());
      model[k] = v;
    }
  }
  ASSERT_TRUE(db->CompactUntilStable().ok());
  auto st = db->Stats();
  EXPECT_GT(st.flushes, 5u);
  EXPECT_GT(st.compactions, 0u);

  for (int i = 0; i < 3000; i++) {
    std::string v;
    Status s = db->Get(Key(i), &v);
    auto it = model.find(Key(i));
    if (it == model.end()) {
      EXPECT_TRUE(s.IsNotFound()) << Key(i);
    } else {
      ASSERT_TRUE(s.ok()) << Key(i) << " " << s.ToString();
      EXPECT_EQ(v, it->second);
    }
  }

  // Scan must equal the model, in order.
  std::vector<std::pair<std::string, std::string>> scanned;
  ASSERT_TRUE(db->Scan("", "", 0, [&](auto k, auto v) {
                  scanned.emplace_back(std::string(k), std::string(v));
                  return true;
                }).ok());
  ASSERT_EQ(scanned.size(), model.size());
  size_t i = 0;
  for (const auto& [k, v] : model) {
    EXPECT_EQ(scanned[i].first, k);
    EXPECT_EQ(scanned[i].second, v);
    i++;
  }
}

TEST(DB, LevelsStayDisjointAfterCompaction) {
  TempDir d;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(SmallOptions(), d.str(), &db).ok());
  for (int i = 0; i < 30000; i++) ASSERT_TRUE(db->Put(Key((i * 7919) % 10000), std::string(30, 'a' + i % 26)).ok());
  ASSERT_TRUE(db->CompactUntilStable().ok());
  auto st = db->Stats();
  EXPECT_LT(st.levels[0].first, SmallOptions().l0_compaction_trigger);
  EXPECT_GT(st.levels[1].first + st.levels[2].first, 1);
}

TEST(DB, SnapshotIsolation) {
  TempDir d;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(SmallOptions(), d.str(), &db).ok());
  ASSERT_TRUE(db->Put("acct", "v1").ok());
  const Snapshot* snap = db->GetSnapshot();
  ASSERT_TRUE(db->Put("acct", "v2").ok());
  ASSERT_TRUE(db->Delete("gone").ok());
  // Churn enough data to push both versions through flush + compaction:
  // the snapshot must keep v1 alive.
  for (int i = 0; i < 20000; i++) ASSERT_TRUE(db->Put(Key(i), std::string(20, 'z')).ok());
  ASSERT_TRUE(db->Flush().ok());
  ASSERT_TRUE(db->CompactUntilStable().ok());

  std::string v;
  ASSERT_TRUE(db->Get("acct", &v, snap).ok());
  EXPECT_EQ(v, "v1");
  ASSERT_TRUE(db->Get("acct", &v).ok());
  EXPECT_EQ(v, "v2");
  int seen = 0;
  ASSERT_TRUE(db->Scan(Key(0), Key(100), 0, [&](auto, auto) { seen++; return true; }, snap).ok());
  EXPECT_EQ(seen, 0);  // those keys did not exist at snapshot time
  db->ReleaseSnapshot(snap);
}

TEST(DB, RecoversFromWalAfterCrash) {
  TempDir d;
  {
    std::unique_ptr<DB> db;
    Options o = SmallOptions();
    o.memtable_bytes = 1 << 30;  // everything stays in the WAL + memtable
    ASSERT_TRUE(DB::Open(o, d.str(), &db).ok());
    for (int i = 0; i < 500; i++) ASSERT_TRUE(db->Put(Key(i), "v" + std::to_string(i)).ok());
    ASSERT_TRUE(db->Delete(Key(7)).ok());
    // Destroy without flushing: only the WAL has the data.
  }
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(SmallOptions(), d.str(), &db).ok());
  std::string v;
  ASSERT_TRUE(db->Get(Key(499), &v).ok());
  EXPECT_EQ(v, "v499");
  EXPECT_TRUE(db->Get(Key(7), &v).IsNotFound());
  EXPECT_EQ(db->Stats().last_sequence, 501u);
}

TEST(DB, ReopenAfterCompactionKeepsData) {
  TempDir d;
  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(SmallOptions(), d.str(), &db).ok());
    for (int i = 0; i < 10000; i++) ASSERT_TRUE(db->Put(Key(i), std::to_string(i)).ok());
    ASSERT_TRUE(db->CompactUntilStable().ok());
  }
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(SmallOptions(), d.str(), &db).ok());
  for (int i = 0; i < 10000; i += 97) {
    std::string v;
    ASSERT_TRUE(db->Get(Key(i), &v).ok()) << i;
    EXPECT_EQ(v, std::to_string(i));
  }
}

TEST(DB, PutIfAbsentIsIdempotent) {
  TempDir d;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(SmallOptions(), d.str(), &db).ok());
  bool written;
  std::string existing;
  ASSERT_TRUE(db->PutIfAbsent("idem/t1/abc", "evt_1", false, &written, &existing).ok());
  EXPECT_TRUE(written);
  ASSERT_TRUE(db->PutIfAbsent("idem/t1/abc", "evt_2", false, &written, &existing).ok());
  EXPECT_FALSE(written);
  EXPECT_EQ(existing, "evt_1");
}

TEST(DB, ConcurrentReadersWithBackgroundCompaction) {
  TempDir d;
  Options o = SmallOptions();
  o.background_work = true;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(o, d.str(), &db).ok());
  std::atomic<bool> stop{false};
  std::atomic<int> errors{0};
  std::thread reader([&] {
    std::mt19937 rng(7);
    while (!stop) {
      int i = rng() % 5000;
      std::string v;
      Status s = db->Get(Key(i), &v);
      if (s.ok() && v != "v" + std::to_string(i)) errors++;
      if (!s.ok() && !s.IsNotFound()) errors++;
    }
  });
  for (int round = 0; round < 4; round++)
    for (int i = 0; i < 5000; i++) ASSERT_TRUE(db->Put(Key(i), "v" + std::to_string(i)).ok());
  stop = true;
  reader.join();
  ASSERT_TRUE(db->CompactUntilStable().ok());
  EXPECT_EQ(errors.load(), 0);
  EXPECT_GT(db->Stats().compactions, 0u);
}
