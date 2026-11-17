#include "carnet/sstable.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "carnet/crc32c.h"

namespace carnet {
namespace {

Status Errno(const std::string& ctx) { return Status::IOError(ctx + ": " + std::strerror(errno)); }

Status PRead(int fd, uint64_t off, size_t n, std::string* out) {
  out->resize(n);
  size_t done = 0;
  while (done < n) {
    ssize_t r = ::pread(fd, out->data() + done, n - done, static_cast<off_t>(off + done));
    if (r < 0) {
      if (errno == EINTR) continue;
      return Errno("pread");
    }
    if (r == 0) return Status::Corruption("short read");
    done += static_cast<size_t>(r);
  }
  return Status::OK();
}

}  // namespace

bool ParseBlock(std::string_view block, std::vector<BlockEntry>* out) {
  out->clear();
  while (!block.empty()) {
    uint32_t kl, vl;
    if (!GetVarint32(&block, &kl) || !GetVarint32(&block, &vl)) return false;
    if (block.size() < static_cast<size_t>(kl) + vl) return false;
    out->push_back({block.substr(0, kl), block.substr(kl, vl)});
    block.remove_prefix(kl + vl);
  }
  return true;
}

// ---------------------------------------------------------------- builder

SSTableBuilder::SSTableBuilder(std::string path, size_t block_size, int bloom_bits_per_key)
    : path_(std::move(path)), block_size_(block_size), bloom_(bloom_bits_per_key) {}

SSTableBuilder::~SSTableBuilder() {
  if (!finished_) Abandon();
}

Status SSTableBuilder::Open() {
  fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd_ < 0) return Errno("create " + path_);
  return Status::OK();
}

// Buffered: compaction writes many small blocks; one syscall per 256 KiB
// instead of two per 4 KiB block.
Status SSTableBuilder::Write(std::string_view data) {
  out_buf_.append(data);
  offset_ += data.size();
  if (out_buf_.size() >= (256u << 10)) return FlushBuffer();
  return Status::OK();
}

Status SSTableBuilder::FlushBuffer() {
  const char* p = out_buf_.data();
  size_t n = out_buf_.size();
  while (n > 0) {
    ssize_t w = ::write(fd_, p, n);
    if (w < 0) {
      if (errno == EINTR) continue;
      return Errno("write " + path_);
    }
    p += w;
    n -= static_cast<size_t>(w);
  }
  out_buf_.clear();
  return Status::OK();
}

Status SSTableBuilder::Add(std::string_view ikey, std::string_view value) {
  if (!last_key_.empty() && CompareInternalKey(ikey, last_key_) <= 0) {
    return Status::InvalidArgument("keys out of order");
  }
  if (entries_ == 0) first_key_.assign(ikey);
  PutVarint32(&block_, static_cast<uint32_t>(ikey.size()));
  PutVarint32(&block_, static_cast<uint32_t>(value.size()));
  block_.append(ikey);
  block_.append(value);
  last_key_.assign(ikey);
  // Bloom on the user key: Get() queries by user key, not by sequence.
  bloom_.AddKey(ExtractUserKey(ikey));
  entries_++;
  if (block_.size() >= block_size_) return FlushBlock();
  return Status::OK();
}

Status SSTableBuilder::FlushBlock() {
  if (block_.empty()) return Status::OK();
  uint64_t off = offset_;
  CARNET_RETURN_IF_ERROR(Write(block_));
  std::string crc;
  PutFixed32(&crc, crc32c::Mask(crc32c::Value(block_.data(), block_.size())));
  CARNET_RETURN_IF_ERROR(Write(crc));
  PutLengthPrefixed(&index_, last_key_);
  PutFixed64(&index_, off);
  PutFixed64(&index_, block_.size());
  block_.clear();
  return Status::OK();
}

Status SSTableBuilder::Finish(FileMeta* meta) {
  CARNET_RETURN_IF_ERROR(FlushBlock());
  std::string bloom = bloom_.Finish();
  uint64_t bloom_off = offset_;
  CARNET_RETURN_IF_ERROR(Write(bloom));
  uint64_t index_off = offset_;
  CARNET_RETURN_IF_ERROR(Write(index_));

  std::string footer;
  PutFixed64(&footer, index_off);
  PutFixed64(&footer, index_.size());
  PutFixed64(&footer, bloom_off);
  PutFixed64(&footer, bloom.size());
  PutFixed64(&footer, entries_);
  PutFixed64(&footer, kSSTableMagic);
  CARNET_RETURN_IF_ERROR(Write(footer));
  CARNET_RETURN_IF_ERROR(FlushBuffer());

  if (::fsync(fd_) != 0) return Errno("fsync " + path_);
  ::close(fd_);
  fd_ = -1;
  finished_ = true;

  meta->file_size = offset_;
  meta->entries = entries_;
  meta->smallest = first_key_;
  meta->largest = last_key_;
  return Status::OK();
}

void SSTableBuilder::Abandon() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  ::unlink(path_.c_str());
  finished_ = true;
}

// ---------------------------------------------------------------- reader

Status SSTable::Open(const std::string& path, uint64_t number, std::shared_ptr<SSTable>* out) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Errno("open " + path);
  std::shared_ptr<SSTable> t(new SSTable());
  t->fd_ = fd;
  t->number_ = number;
  t->path_ = path;

  struct stat st;
  if (::fstat(fd, &st) != 0) return Errno("stat " + path);
  if (static_cast<size_t>(st.st_size) < kFooterSize) return Status::Corruption("file too small: " + path);

  std::string footer;
  CARNET_RETURN_IF_ERROR(PRead(fd, st.st_size - kFooterSize, kFooterSize, &footer));
  const char* f = footer.data();
  if (DecodeFixed64(f + 40) != kSSTableMagic) return Status::Corruption("bad magic: " + path);
  uint64_t index_off = DecodeFixed64(f), index_size = DecodeFixed64(f + 8);
  uint64_t bloom_off = DecodeFixed64(f + 16), bloom_size = DecodeFixed64(f + 24);
  t->entries_ = DecodeFixed64(f + 32);

  CARNET_RETURN_IF_ERROR(PRead(fd, bloom_off, bloom_size, &t->bloom_));
  std::string index;
  CARNET_RETURN_IF_ERROR(PRead(fd, index_off, index_size, &index));
  std::string_view in(index);
  while (!in.empty()) {
    std::string_view k;
    if (!GetLengthPrefixed(&in, &k) || in.size() < 16) return Status::Corruption("bad index: " + path);
    IndexEntry e{std::string(k), DecodeFixed64(in.data()), DecodeFixed64(in.data() + 8)};
    in.remove_prefix(16);
    t->index_.push_back(std::move(e));
  }
  *out = std::move(t);
  return Status::OK();
}

SSTable::~SSTable() {
  if (fd_ >= 0) ::close(fd_);
}

Status SSTable::ReadBlock(const IndexEntry& e, std::string* out) const {
  std::string raw;
  CARNET_RETURN_IF_ERROR(PRead(fd_, e.offset, e.size + 4, &raw));
  uint32_t want = crc32c::Unmask(DecodeFixed32(raw.data() + e.size));
  if (crc32c::Value(raw.data(), e.size) != want) {
    return Status::Corruption("block checksum mismatch in " + path_);
  }
  raw.resize(e.size);
  *out = std::move(raw);
  return Status::OK();
}

size_t SSTable::FindBlock(std::string_view ikey) const {
  auto it = std::lower_bound(index_.begin(), index_.end(), ikey,
                             [](const IndexEntry& e, std::string_view k) {
                               return CompareInternalKey(e.last_key, k) < 0;
                             });
  return static_cast<size_t>(it - index_.begin());
}

Status SSTable::Get(std::string_view user_key, SequenceNumber seq, std::string* value, bool* found,
                    bool* deleted, bool* bloom_skip) const {
  *found = false;
  *bloom_skip = false;
  if (!BloomMayContain(bloom_, user_key)) {
    *bloom_skip = true;
    return Status::OK();
  }
  std::string target = LookupKey(user_key, seq);
  size_t b = FindBlock(target);
  if (b >= index_.size()) return Status::OK();
  std::string block;
  CARNET_RETURN_IF_ERROR(ReadBlock(index_[b], &block));
  std::vector<BlockEntry> entries;
  if (!ParseBlock(block, &entries)) return Status::Corruption("bad block in " + path_);
  for (const auto& e : entries) {
    if (CompareInternalKey(e.key, target) < 0) continue;
    if (ExtractUserKey(e.key) != user_key) return Status::OK();
    *found = true;
    *deleted = ExtractValueType(e.key) == ValueType::kDelete;
    if (!*deleted) value->assign(e.value);
    return Status::OK();
  }
  return Status::OK();
}

// Two-level iterator: walks the sparse index, loading one block at a time.
class SSTableIterator final : public Iterator {
 public:
  explicit SSTableIterator(std::shared_ptr<const SSTable> t) : t_(std::move(t)) {}

  bool Valid() const override { return status_.ok() && block_idx_ < t_->index().size() && pos_ < entries_.size(); }
  void SeekToFirst() override {
    LoadBlock(0);
    pos_ = 0;
    SkipEmpty();
  }
  void Seek(std::string_view ikey) override {
    auto& idx = t_->index();
    auto it = std::lower_bound(idx.begin(), idx.end(), ikey, [](const SSTable::IndexEntry& e, std::string_view k) {
      return CompareInternalKey(e.last_key, k) < 0;
    });
    LoadBlock(static_cast<size_t>(it - idx.begin()));
    pos_ = 0;
    while (pos_ < entries_.size() && CompareInternalKey(entries_[pos_].key, ikey) < 0) pos_++;
    SkipEmpty();
  }
  void Next() override {
    pos_++;
    SkipEmpty();
  }
  std::string_view key() const override { return entries_[pos_].key; }
  std::string_view value() const override { return entries_[pos_].value; }
  Status status() const override { return status_; }

 private:
  void LoadBlock(size_t i) {
    block_idx_ = i;
    entries_.clear();
    if (i >= t_->index().size()) return;
    status_ = t_->ReadBlock(t_->index()[i], &block_);
    if (status_.ok() && !ParseBlock(block_, &entries_)) status_ = Status::Corruption("bad block");
  }
  void SkipEmpty() {
    while (status_.ok() && block_idx_ < t_->index().size() && pos_ >= entries_.size()) {
      LoadBlock(block_idx_ + 1);
      pos_ = 0;
    }
  }

  std::shared_ptr<const SSTable> t_;
  size_t block_idx_ = 0;
  size_t pos_ = 0;
  std::string block_;
  std::vector<BlockEntry> entries_;
  Status status_;
};

std::unique_ptr<Iterator> SSTable::NewIterator() const {
  return std::make_unique<SSTableIterator>(shared_from_this());
}

}  // namespace carnet
