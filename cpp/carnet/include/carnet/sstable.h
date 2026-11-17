#pragma once

// Sorted String Table: an immutable, sorted file of internal keys.
//
// File layout
//   [data block 0][crc][data block 1][crc] ... [bloom filter][index block][footer]
//
//   data block  : repeated { varint klen | varint vlen | ikey | value }
//   crc         : masked crc32c of the preceding block (4 bytes)
//   index block : repeated { lp(last ikey in block) | fixed64 offset | fixed64 size }
//                 -- a *sparse* index: one entry per ~4 KiB block, not per key
//   footer      : fixed64 index_off | index_size | bloom_off | bloom_size | entries | magic

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "carnet/bloom.h"
#include "carnet/coding.h"
#include "carnet/iterator.h"
#include "carnet/status.h"

namespace carnet {

constexpr uint64_t kSSTableMagic = 0x6361726e65747631ull;  // "carnetv1"
constexpr size_t kFooterSize = 48;

struct FileMeta {
  uint64_t number = 0;
  uint64_t file_size = 0;
  uint64_t entries = 0;
  std::string smallest;  // internal keys
  std::string largest;
};

class SSTableBuilder {
 public:
  SSTableBuilder(std::string path, size_t block_size, int bloom_bits_per_key);
  ~SSTableBuilder();

  Status Open();
  // REQUIRES: keys added in strictly increasing internal-key order.
  Status Add(std::string_view ikey, std::string_view value);
  Status Finish(FileMeta* meta);
  void Abandon();  // delete the partial file
  uint64_t FileSize() const { return offset_ + block_.size(); }
  uint64_t NumEntries() const { return entries_; }

 private:
  Status FlushBlock();
  Status Write(std::string_view data);
  Status FlushBuffer();

  std::string path_;
  size_t block_size_;
  int fd_ = -1;
  uint64_t offset_ = 0;
  uint64_t entries_ = 0;
  std::string block_;
  std::string out_buf_;
  std::string last_key_;
  std::string first_key_;
  std::string index_;
  BloomFilterBuilder bloom_;
  bool finished_ = false;
};

class SSTable : public std::enable_shared_from_this<SSTable> {
 public:
  static Status Open(const std::string& path, uint64_t number, std::shared_ptr<SSTable>* out);
  ~SSTable();

  // Point lookup of the newest version of user_key visible at seq.
  // *found = an entry (put or tombstone) exists; *bloom_skip = the bloom
  // filter ruled the file out without reading a data block.
  Status Get(std::string_view user_key, SequenceNumber seq, std::string* value, bool* found,
             bool* deleted, bool* bloom_skip) const;

  std::unique_ptr<Iterator> NewIterator() const;

  uint64_t number() const { return number_; }
  uint64_t entries() const { return entries_; }

  struct IndexEntry {
    std::string last_key;
    uint64_t offset;
    uint64_t size;
  };

  Status ReadBlock(const IndexEntry& e, std::string* out) const;
  const std::vector<IndexEntry>& index() const { return index_; }

 private:
  SSTable() = default;
  size_t FindBlock(std::string_view ikey) const;  // first block whose last_key >= ikey

  int fd_ = -1;
  uint64_t number_ = 0;
  uint64_t entries_ = 0;
  std::string path_;
  std::vector<IndexEntry> index_;
  std::string bloom_;
};

// Decode the entries of one data block.
struct BlockEntry {
  std::string_view key;
  std::string_view value;
};
bool ParseBlock(std::string_view block, std::vector<BlockEntry>* out);

}  // namespace carnet
