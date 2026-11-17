#include "carnet/db.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace carnet {
namespace fs = std::filesystem;

namespace {

std::string Hex(std::string_view s) {
  static const char* d = "0123456789abcdef";
  std::string out;
  out.reserve(s.size() * 2);
  for (unsigned char c : s) {
    out.push_back(d[c >> 4]);
    out.push_back(d[c & 15]);
  }
  return out;
}

std::string Unhex(std::string_view s) {
  auto v = [](char c) { return c <= '9' ? c - '0' : c - 'a' + 10; };
  std::string out;
  for (size_t i = 0; i + 1 < s.size(); i += 2) out.push_back(static_cast<char>(v(s[i]) << 4 | v(s[i + 1])));
  return out;
}

// Parses "000123.sst" -> 123.
bool ParseFileName(const std::string& name, const std::string& ext, uint64_t* n) {
  if (name.size() <= ext.size() || name.substr(name.size() - ext.size()) != ext) return false;
  try {
    *n = std::stoull(name.substr(0, name.size() - ext.size()));
  } catch (...) {
    return false;
  }
  return true;
}

bool UserRangeOverlaps(const FileMeta& f, std::string_view lo, std::string_view hi) {
  return !(ExtractUserKey(f.largest) < lo || ExtractUserKey(f.smallest) > hi);
}

}  // namespace

// ---------------------------------------------------------------- WriteBatch

void WriteBatch::Put(std::string_view key, std::string_view value) {
  ops_.push_back(static_cast<char>(ValueType::kPut));
  PutLengthPrefixed(&ops_, key);
  PutLengthPrefixed(&ops_, value);
  count_++;
}

void WriteBatch::Delete(std::string_view key) {
  ops_.push_back(static_cast<char>(ValueType::kDelete));
  PutLengthPrefixed(&ops_, key);
  count_++;
}

std::string WriteBatch::Encode(SequenceNumber seq) const {
  std::string rep;
  PutFixed64(&rep, seq);
  PutFixed32(&rep, count_);
  rep += ops_;
  return rep;
}

Status WriteBatch::Decode(std::string_view rep, SequenceNumber* seq,
                          const std::function<void(ValueType, std::string_view, std::string_view)>& fn) {
  if (rep.size() < 12) return Status::Corruption("batch too small");
  *seq = DecodeFixed64(rep.data());
  uint32_t count = DecodeFixed32(rep.data() + 8);
  rep.remove_prefix(12);
  for (uint32_t i = 0; i < count; i++) {
    if (rep.empty()) return Status::Corruption("batch truncated");
    auto type = static_cast<ValueType>(rep[0]);
    rep.remove_prefix(1);
    std::string_view k, v;
    if (!GetLengthPrefixed(&rep, &k)) return Status::Corruption("batch key");
    if (type == ValueType::kPut && !GetLengthPrefixed(&rep, &v)) return Status::Corruption("batch value");
    fn(type, k, v);
  }
  return Status::OK();
}

// ---------------------------------------------------------------- open / recover

DB::DB(Options o, std::string dir) : options_(o), dir_(std::move(dir)) {
  mem_ = std::make_shared<MemTable>();
  current_ = std::make_shared<Version>();
  current_->levels.resize(options_.num_levels);
  compact_pointer_.assign(options_.num_levels, 0);
}

Status DB::Open(const Options& options, const std::string& dir, std::unique_ptr<DB>* out) {
  std::error_code ec;
  fs::create_directories(dir, ec);
  if (ec) return Status::IOError("mkdir " + dir + ": " + ec.message());
  std::unique_ptr<DB> db(new DB(options, dir));
  CARNET_RETURN_IF_ERROR(db->Recover());
  if (options.background_work) db->bg_thread_ = std::thread([d = db.get()] { d->BackgroundLoop(); });
  *out = std::move(db);
  return Status::OK();
}

DB::~DB() {
  {
    std::lock_guard<std::mutex> l(mu_);
    shutting_down_ = true;
  }
  work_cv_.notify_all();
  if (bg_thread_.joinable()) bg_thread_.join();
}

std::string DB::TablePath(uint64_t n) const {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "/%06llu.sst", static_cast<unsigned long long>(n));
  return dir_ + buf;
}

std::string DB::LogPath(uint64_t n) const {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "/%06llu.log", static_cast<unsigned long long>(n));
  return dir_ + buf;
}

Status DB::Recover() {
  std::unique_lock<std::mutex> lock(mu_);
  uint64_t manifest_log = 0;

  // 1. The MANIFEST names every live SSTable and the oldest WAL still needed.
  std::ifstream mf(dir_ + "/MANIFEST");
  if (mf) {
    std::string line;
    while (std::getline(mf, line)) {
      std::istringstream ss(line);
      std::string tag;
      ss >> tag;
      if (tag == "next_file") {
        uint64_t n;
        ss >> n;
        next_file_ = n;
      } else if (tag == "last_seq") {
        ss >> last_seq_;
      } else if (tag == "log") {
        ss >> manifest_log;
      } else if (tag == "file") {
        int level;
        std::string sm, lg;
        auto f = std::make_shared<TableFile>();
        ss >> level >> f->meta.number >> f->meta.file_size >> f->meta.entries >> sm >> lg;
        f->meta.smallest = Unhex(sm);
        f->meta.largest = Unhex(lg);
        if (level < 0 || level >= options_.num_levels) return Status::Corruption("manifest level");
        CARNET_RETURN_IF_ERROR(SSTable::Open(TablePath(f->meta.number), f->meta.number, &f->table));
        current_->levels[level].push_back(f);
      }
    }
  }

  // 2. Replay every WAL at or after the manifest's log number, oldest first.
  std::vector<uint64_t> logs;
  for (const auto& e : fs::directory_iterator(dir_)) {
    uint64_t n;
    std::string name = e.path().filename().string();
    if (ParseFileName(name, ".log", &n) && n >= manifest_log) logs.push_back(n);
    if (ParseFileName(name, ".sst", &n) || ParseFileName(name, ".log", &n)) {
      next_file_ = std::max<uint64_t>(next_file_, n + 1);
    }
  }
  std::sort(logs.begin(), logs.end());
  for (uint64_t n : logs) {
    WalReplayStats st;
    CARNET_RETURN_IF_ERROR(ReplayWal(
        LogPath(n),
        [&](std::string_view rec) {
          SequenceNumber seq;
          Status s = WriteBatch::Decode(rec, &seq, [&](ValueType t, std::string_view k, std::string_view v) {
            mem_->Add(seq, t, k, v);
            last_seq_ = std::max(last_seq_, seq);
            seq++;
          });
          return s;
        },
        &st));
    if (st.dropped_bytes > 0) {
      std::fprintf(stderr, "carnet: wal %06llu: dropped %llu byte torn tail\n",
                   static_cast<unsigned long long>(n), static_cast<unsigned long long>(st.dropped_bytes));
    }
  }

  // 3. Persist the replayed memtable as an L0 table so old logs can go.
  if (mem_->Entries() > 0) {
    imm_ = mem_;
    mem_ = std::make_shared<MemTable>();
    imm_log_number_ = logs.empty() ? 0 : logs.front();
    CARNET_RETURN_IF_ERROR(NewLog());
    CARNET_RETURN_IF_ERROR(DoFlushLocked(lock));
  } else {
    CARNET_RETURN_IF_ERROR(NewLog());
    CARNET_RETURN_IF_ERROR(WriteManifest(*current_));
  }
  DeleteObsoleteFiles();
  return Status::OK();
}

Status DB::NewLog() {
  uint64_t n = next_file_++;
  std::unique_ptr<WalWriter> w;
  CARNET_RETURN_IF_ERROR(WalWriter::Open(LogPath(n), &w));
  log_ = std::move(w);
  log_number_ = n;
  return Status::OK();
}

uint64_t DB::MinLogNumber() const { return imm_ ? imm_log_number_ : log_number_; }

Status DB::WriteManifest(const Version& v) {
  std::ostringstream ss;
  ss << "next_file " << next_file_.load() << "\n";
  ss << "last_seq " << last_seq_ << "\n";
  ss << "log " << MinLogNumber() << "\n";
  for (int level = 0; level < options_.num_levels; level++) {
    for (const auto& f : v.levels[level]) {
      ss << "file " << level << " " << f->meta.number << " " << f->meta.file_size << " " << f->meta.entries << " "
         << Hex(f->meta.smallest) << " " << Hex(f->meta.largest) << "\n";
    }
  }
  // Write-then-rename makes the manifest switch atomic.
  std::string tmp = dir_ + "/MANIFEST.tmp";
  std::string data = ss.str();
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return Status::IOError("open manifest tmp");
  bool ok = ::write(fd, data.data(), data.size()) == static_cast<ssize_t>(data.size()) && ::fsync(fd) == 0;
  ::close(fd);
  if (!ok) return Status::IOError("write manifest");
  std::error_code ec;
  fs::rename(tmp, dir_ + "/MANIFEST", ec);
  if (ec) return Status::IOError("rename manifest: " + ec.message());
  return Status::OK();
}

void DB::DeleteObsoleteFiles() {
  std::set<uint64_t> live;
  for (const auto& level : current_->levels)
    for (const auto& f : level) live.insert(f->meta.number);
  uint64_t min_log = MinLogNumber();
  for (const auto& e : fs::directory_iterator(dir_)) {
    uint64_t n;
    std::string name = e.path().filename().string();
    bool drop = (ParseFileName(name, ".sst", &n) && !live.count(n)) ||
                (ParseFileName(name, ".log", &n) && n < min_log);
    // Safe even if a reader still has the SSTable open: POSIX keeps the inode
    // alive until the last fd closes.
    if (drop) {
      std::error_code ec;
      fs::remove(e.path(), ec);
    }
  }
}

// ---------------------------------------------------------------- writes

Status DB::Put(std::string_view key, std::string_view value, bool sync) {
  WriteBatch b;
  b.Put(key, value);
  return Write(b, sync);
}

Status DB::Delete(std::string_view key, bool sync) {
  WriteBatch b;
  b.Delete(key);
  return Write(b, sync);
}

Status DB::Write(const WriteBatch& batch, bool sync, SequenceNumber* seq_out) {
  std::unique_lock<std::mutex> lock(mu_);
  CARNET_RETURN_IF_ERROR(MakeRoomForWrite(lock));
  return WriteLocked(batch, sync, seq_out);
}

Status DB::WriteLocked(const WriteBatch& batch, bool sync, SequenceNumber* seq_out) {
  if (batch.Count() == 0) return Status::OK();
  SequenceNumber first = last_seq_ + 1;
  std::string rep = batch.Encode(first);
  CARNET_RETURN_IF_ERROR(log_->Append(rep, sync));  // durable before visible
  SequenceNumber seq = first;
  SequenceNumber ignored;
  CARNET_RETURN_IF_ERROR(WriteBatch::Decode(rep, &ignored, [&](ValueType t, std::string_view k, std::string_view v) {
    mem_->Add(seq++, t, k, v);
  }));
  last_seq_ = seq - 1;
  if (seq_out) *seq_out = last_seq_;
  return Status::OK();
}

Status DB::PutIfAbsent(std::string_view key, std::string_view value, bool sync, bool* written,
                       std::string* existing) {
  std::unique_lock<std::mutex> lock(mu_);
  CARNET_RETURN_IF_ERROR(MakeRoomForWrite(lock));
  Status s = GetImpl(key, last_seq_, mem_, imm_, current_, existing);
  if (s.ok()) {
    *written = false;
    return Status::OK();
  }
  if (!s.IsNotFound()) return s;
  WriteBatch b;
  b.Put(key, value);
  *written = true;
  return WriteLocked(b, sync, nullptr);
}

// Applies backpressure (DDIA ch. 11): if flushing or compaction falls behind,
// writers block instead of letting L0 grow without bound.
Status DB::MakeRoomForWrite(std::unique_lock<std::mutex>& lock) {
  while (true) {
    if (!bg_error_.ok()) return bg_error_;
    int l0 = static_cast<int>(current_->levels[0].size());
    if (l0 >= options_.l0_stop_writes) {
      write_stalls_++;
      if (options_.background_work) {
        work_cv_.notify_one();
        done_cv_.wait(lock);
      } else {
        CARNET_RETURN_IF_ERROR(DoCompactionLocked(lock));
      }
    } else if (mem_->ApproximateBytes() < options_.memtable_bytes) {
      return Status::OK();
    } else if (imm_) {
      if (options_.background_work) {
        write_stalls_++;
        done_cv_.wait(lock);
      } else {
        CARNET_RETURN_IF_ERROR(DoFlushLocked(lock));
      }
    } else {
      imm_ = mem_;
      imm_log_number_ = log_number_;
      mem_ = std::make_shared<MemTable>();
      CARNET_RETURN_IF_ERROR(NewLog());
      if (options_.background_work) {
        work_cv_.notify_one();
      } else {
        CARNET_RETURN_IF_ERROR(DoFlushLocked(lock));
        while (NeedsWork()) CARNET_RETURN_IF_ERROR(DoCompactionLocked(lock));
      }
    }
  }
}

// ---------------------------------------------------------------- reads

Status DB::GetImpl(std::string_view key, SequenceNumber seq, const std::shared_ptr<MemTable>& mem,
                   const std::shared_ptr<MemTable>& imm, const std::shared_ptr<Version>& v,
                   std::string* value) {
  bool deleted = false;
  if (mem->Get(key, seq, value, &deleted)) return deleted ? Status::NotFound() : Status::OK();
  if (imm && imm->Get(key, seq, value, &deleted)) return deleted ? Status::NotFound() : Status::OK();

  auto probe = [&](const FilePtr& f, bool* done) -> Status {
    bool found, skip;
    CARNET_RETURN_IF_ERROR(f->table->Get(key, seq, value, &found, &deleted, &skip));
    if (skip) bloom_negatives_++;
    *done = found;
    return Status::OK();
  };

  // L0 files overlap: check all of them, newest (highest number) first.
  // (L0 is appended in flush order, so iterate it backwards.)
  const auto& l0 = v->levels[0];
  for (auto rit = l0.rbegin(); rit != l0.rend(); ++rit) {
    const FilePtr& f = *rit;
    if (!UserRangeOverlaps(f->meta, key, key)) continue;
    bool done = false;
    CARNET_RETURN_IF_ERROR(probe(f, &done));
    if (done) return deleted ? Status::NotFound() : Status::OK();
  }

  // L1+: files are disjoint and sorted, so binary search picks one per level.
  for (int level = 1; level < options_.num_levels; level++) {
    const auto& files = v->levels[level];
    auto it = std::lower_bound(files.begin(), files.end(), key, [](const FilePtr& f, std::string_view k) {
      return ExtractUserKey(f->meta.largest) < k;
    });
    if (it == files.end() || ExtractUserKey((*it)->meta.smallest) > key) continue;
    bool done = false;
    CARNET_RETURN_IF_ERROR(probe(*it, &done));
    if (done) return deleted ? Status::NotFound() : Status::OK();
  }
  return Status::NotFound();
}

Status DB::Get(std::string_view key, std::string* value, const Snapshot* snap) {
  std::shared_ptr<MemTable> mem, imm;
  std::shared_ptr<Version> v;
  SequenceNumber seq;
  {
    std::lock_guard<std::mutex> l(mu_);
    mem = mem_;
    imm = imm_;
    v = current_;
    seq = snap ? snap->sequence() : last_seq_;
  }
  return GetImpl(key, seq, mem, imm, v, value);
}

Status DB::Scan(std::string_view start, std::string_view end, size_t limit,
                const std::function<bool(std::string_view, std::string_view)>& fn, const Snapshot* snap) {
  std::shared_ptr<MemTable> mem, imm;
  std::shared_ptr<Version> v;
  SequenceNumber seq;
  {
    std::lock_guard<std::mutex> l(mu_);
    mem = mem_;
    imm = imm_;
    v = current_;
    seq = snap ? snap->sequence() : last_seq_;
  }
  std::vector<std::unique_ptr<Iterator>> children;
  children.push_back(mem->NewIterator());
  if (imm) children.push_back(imm->NewIterator());
  for (const auto& level : v->levels)
    for (const auto& f : level)
      if (end.empty() || ExtractUserKey(f->meta.smallest) < end)
        if (ExtractUserKey(f->meta.largest) >= start) children.push_back(f->table->NewIterator());

  auto it = NewMergingIterator(std::move(children));
  size_t emitted = 0;
  std::string last_user;
  bool have_last = false;
  for (it->Seek(MakeInternalKey(start, kMaxSequence, ValueType::kPut)); it->Valid(); it->Next()) {
    std::string_view ik = it->key();
    std::string_view uk = ExtractUserKey(ik);
    if (!end.empty() && uk >= end) break;
    if (ExtractSequence(ik) > seq) continue;          // written after our snapshot
    if (have_last && uk == last_user) continue;       // older version, shadowed
    last_user.assign(uk);
    have_last = true;
    if (ExtractValueType(ik) == ValueType::kDelete) continue;
    if (!fn(uk, it->value())) break;
    if (limit && ++emitted >= limit) break;
  }
  return it->status();
}

const Snapshot* DB::GetSnapshot() {
  std::lock_guard<std::mutex> l(mu_);
  auto* s = new Snapshot(last_seq_);
  s->pos_ = snapshots_.insert(last_seq_);
  return s;
}

void DB::ReleaseSnapshot(const Snapshot* s) {
  {
    std::lock_guard<std::mutex> l(mu_);
    snapshots_.erase(s->pos_);
  }
  delete s;
}

// ---------------------------------------------------------------- background work

uint64_t DB::LevelBytes(const Version& v, int level) const {
  uint64_t sum = 0;
  for (const auto& f : v.levels[level]) sum += f->meta.file_size;
  return sum;
}

uint64_t DB::MaxBytesForLevel(int level) const {
  uint64_t b = options_.level1_bytes;
  for (int i = 1; i < level; i++) b *= 10;  // each level is 10x the previous
  return b;
}

bool DB::NeedsWork() const {
  if (!bg_error_.ok()) return false;
  if (imm_) return true;
  if (static_cast<int>(current_->levels[0].size()) >= options_.l0_compaction_trigger) return true;
  for (int l = 1; l < options_.num_levels - 1; l++)
    if (LevelBytes(*current_, l) > MaxBytesForLevel(l)) return true;
  return false;
}

void DB::BackgroundLoop() {
  std::unique_lock<std::mutex> lock(mu_);
  while (true) {
    work_cv_.wait(lock, [&] { return shutting_down_ || NeedsWork(); });
    if (shutting_down_) break;
    bg_busy_ = true;
    Status s = imm_ ? DoFlushLocked(lock) : DoCompactionLocked(lock);
    bg_busy_ = false;
    if (!s.ok()) {
      std::fprintf(stderr, "carnet: background error: %s\n", s.ToString().c_str());
      bg_error_ = s;
    }
    done_cv_.notify_all();
  }
}

Status DB::BuildTable(Iterator* it, std::vector<FileMeta>* outputs, bool split,
                      const std::function<bool(std::string_view, std::string_view)>& keep) {
  std::unique_ptr<SSTableBuilder> b;
  uint64_t number = 0;
  std::string last_user;

  auto finish = [&]() -> Status {
    if (!b) return Status::OK();
    FileMeta m;
    m.number = number;
    if (b->NumEntries() == 0) {
      b->Abandon();
    } else {
      CARNET_RETURN_IF_ERROR(b->Finish(&m));
      outputs->push_back(m);
    }
    b.reset();
    return Status::OK();
  };

  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string_view ik = it->key();
    if (!keep(ik, it->value())) continue;
    std::string_view uk = ExtractUserKey(ik);
    // Only cut a file between distinct user keys, so that every version of a
    // key lives in exactly one file per level (keeps L1+ disjoint).
    if (b && split && b->FileSize() >= options_.target_file_bytes && uk != last_user) {
      CARNET_RETURN_IF_ERROR(finish());
    }
    if (!b) {
      number = next_file_++;
      b = std::make_unique<SSTableBuilder>(TablePath(number), options_.block_size, options_.bloom_bits_per_key);
      CARNET_RETURN_IF_ERROR(b->Open());
    }
    CARNET_RETURN_IF_ERROR(b->Add(ik, it->value()));
    last_user.assign(uk);
  }
  CARNET_RETURN_IF_ERROR(it->status());
  return finish();
}

Status DB::DoFlushLocked(std::unique_lock<std::mutex>& lock) {
  std::shared_ptr<MemTable> imm = imm_;
  if (!imm) return Status::OK();
  std::vector<FileMeta> outputs;
  lock.unlock();
  auto it = imm->NewIterator();
  Status s = BuildTable(it.get(), &outputs, /*split=*/false, [](auto, auto) { return true; });
  lock.lock();
  CARNET_RETURN_IF_ERROR(s);

  auto v = std::make_shared<Version>(*current_);
  for (auto& m : outputs) {
    auto f = std::make_shared<TableFile>();
    f->meta = m;
    CARNET_RETURN_IF_ERROR(SSTable::Open(TablePath(m.number), m.number, &f->table));
    v->levels[0].push_back(f);
  }
  current_ = v;
  imm_.reset();
  CARNET_RETURN_IF_ERROR(WriteManifest(*current_));
  DeleteObsoleteFiles();
  flushes_++;
  done_cv_.notify_all();
  return Status::OK();
}

bool DB::PickCompaction(Compaction* c) {
  const Version& v = *current_;
  auto fill_overlaps = [&](int level) {
    std::string lo, hi;
    for (const auto& f : c->inputs[0]) {
      std::string s(ExtractUserKey(f->meta.smallest)), l(ExtractUserKey(f->meta.largest));
      if (lo.empty() || s < lo) lo = s;
      if (hi.empty() || l > hi) hi = l;
    }
    for (const auto& f : v.levels[level + 1])
      if (UserRangeOverlaps(f->meta, lo, hi)) c->inputs[1].push_back(f);
  };

  if (static_cast<int>(v.levels[0].size()) >= options_.l0_compaction_trigger) {
    c->level = 0;
    c->inputs[0] = v.levels[0];
    fill_overlaps(0);
    return true;
  }
  for (int level = 1; level < options_.num_levels - 1; level++) {
    if (LevelBytes(v, level) <= MaxBytesForLevel(level)) continue;
    const auto& files = v.levels[level];
    size_t idx = compact_pointer_[level]++ % files.size();  // round-robin over the key space
    c->level = level;
    c->inputs[0] = {files[idx]};
    fill_overlaps(level);
    return true;
  }
  return false;
}

bool DB::IsBaseLevelForKey(const Version& v, int level, std::string_view uk) const {
  for (int l = level + 1; l < options_.num_levels; l++)
    for (const auto& f : v.levels[l])
      if (UserRangeOverlaps(f->meta, uk, uk)) return false;
  return true;
}

Status DB::RunCompaction(const Compaction& c, SequenceNumber smallest_snapshot, std::shared_ptr<Version> base,
                         std::vector<FileMeta>* outputs) {
  std::vector<std::unique_ptr<Iterator>> children;
  for (const auto& in : c.inputs)
    for (const auto& f : in) children.push_back(f->table->NewIterator());
  auto merged = NewMergingIterator(std::move(children));

  int out_level = c.level + 1;
  std::string cur_user;
  bool have_user = false;
  SequenceNumber last_seq_for_key = kMaxSequence;

  // Garbage collection rules (same as LevelDB):
  //  * drop a version if a NEWER version of the same key is already visible
  //    to every live snapshot;
  //  * drop a tombstone once nothing older can exist below it and no
  //    snapshot can still see the deleted value.
  auto keep = [&](std::string_view ik, std::string_view) {
    std::string_view uk = ExtractUserKey(ik);
    if (!have_user || uk != cur_user) {
      cur_user.assign(uk);
      have_user = true;
      last_seq_for_key = kMaxSequence;
    }
    SequenceNumber seq = ExtractSequence(ik);
    bool drop = false;
    if (last_seq_for_key <= smallest_snapshot) {
      drop = true;
    } else if (ExtractValueType(ik) == ValueType::kDelete && seq <= smallest_snapshot &&
               IsBaseLevelForKey(*base, out_level, uk)) {
      drop = true;
    }
    last_seq_for_key = seq;
    return !drop;
  };
  return BuildTable(merged.get(), outputs, /*split=*/true, keep);
}

Status DB::DoCompactionLocked(std::unique_lock<std::mutex>& lock) {
  if (imm_) return DoFlushLocked(lock);
  Compaction c;
  if (!PickCompaction(&c)) return Status::OK();
  std::shared_ptr<Version> base = current_;
  SequenceNumber smallest_snapshot = snapshots_.empty() ? last_seq_ : *snapshots_.begin();
  int out_level = c.level + 1;

  std::vector<FileMeta> outputs;
  bool trivial_move = c.level > 0 && c.inputs[0].size() == 1 && c.inputs[1].empty();
  if (!trivial_move) {
    lock.unlock();
    Status s = RunCompaction(c, smallest_snapshot, base, &outputs);
    lock.lock();
    CARNET_RETURN_IF_ERROR(s);
  }

  // Install: only this thread mutates the file set, so current_ == base
  // except for L0 appends, which never touch the input files.
  std::set<uint64_t> removed;
  for (const auto& in : c.inputs)
    for (const auto& f : in) removed.insert(f->meta.number);
  auto v = std::make_shared<Version>(*current_);
  for (int l : {c.level, out_level}) {
    auto& files = v->levels[l];
    files.erase(std::remove_if(files.begin(), files.end(),
                               [&](const FilePtr& f) { return removed.count(f->meta.number) > 0; }),
                files.end());
  }
  if (trivial_move) {
    v->levels[out_level].push_back(c.inputs[0][0]);
  } else {
    for (auto& m : outputs) {
      auto f = std::make_shared<TableFile>();
      f->meta = m;
      CARNET_RETURN_IF_ERROR(SSTable::Open(TablePath(m.number), m.number, &f->table));
      v->levels[out_level].push_back(f);
    }
  }
  std::sort(v->levels[out_level].begin(), v->levels[out_level].end(), [](const FilePtr& a, const FilePtr& b) {
    return CompareInternalKey(a->meta.smallest, b->meta.smallest) < 0;
  });
  current_ = v;
  CARNET_RETURN_IF_ERROR(WriteManifest(*current_));
  DeleteObsoleteFiles();
  compactions_++;
  done_cv_.notify_all();
  return Status::OK();
}

// ---------------------------------------------------------------- maintenance

Status DB::Flush() {
  std::unique_lock<std::mutex> lock(mu_);
  if (options_.background_work) done_cv_.wait(lock, [&] { return !imm_ || !bg_error_.ok(); });
  CARNET_RETURN_IF_ERROR(bg_error_);
  if (mem_->Entries() == 0) return Status::OK();
  imm_ = mem_;
  imm_log_number_ = log_number_;
  mem_ = std::make_shared<MemTable>();
  CARNET_RETURN_IF_ERROR(NewLog());
  if (!options_.background_work) return DoFlushLocked(lock);
  work_cv_.notify_one();
  done_cv_.wait(lock, [&] { return !imm_ || !bg_error_.ok(); });
  return bg_error_;
}

Status DB::CompactUntilStable() {
  std::unique_lock<std::mutex> lock(mu_);
  if (options_.background_work) {
    work_cv_.notify_one();
    done_cv_.wait(lock, [&] { return (!NeedsWork() && !bg_busy_) || !bg_error_.ok(); });
    return bg_error_;
  }
  while (NeedsWork()) CARNET_RETURN_IF_ERROR(DoCompactionLocked(lock));
  return Status::OK();
}

DBStats DB::Stats() {
  std::lock_guard<std::mutex> l(mu_);
  DBStats s;
  s.last_sequence = last_seq_;
  s.memtable_bytes = mem_->ApproximateBytes();
  for (int level = 0; level < options_.num_levels; level++)
    s.levels.emplace_back(static_cast<int>(current_->levels[level].size()), LevelBytes(*current_, level));
  s.compactions = compactions_;
  s.flushes = flushes_;
  s.bloom_negatives = bloom_negatives_;
  s.write_stalls = write_stalls_;
  return s;
}

}  // namespace carnet
