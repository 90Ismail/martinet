#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <string_view>

#include "carnet/coding.h"
#include "carnet/iterator.h"
#include "carnet/skiplist.h"

namespace carnet {

// In-memory write buffer. Sorted by internal key so it can be flushed to an
// SSTable with a single sequential pass.
class MemTable {
 public:
  MemTable();
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  // REQUIRES: external synchronization (one writer at a time).
  void Add(SequenceNumber seq, ValueType type, std::string_view key, std::string_view value);

  // Looks for the newest entry for `key` with sequence <= seq.
  // Returns true if an entry was found; *deleted says whether it is a
  // tombstone. Safe to call concurrently with Add().
  bool Get(std::string_view key, SequenceNumber seq, std::string* value, bool* deleted) const;

  size_t ApproximateBytes() const { return bytes_.load(std::memory_order_relaxed); }
  size_t Entries() const { return entries_.load(std::memory_order_relaxed); }

  std::unique_ptr<Iterator> NewIterator() const;

 private:
  struct Entry {
    std::string ikey;
    std::string value;
  };
  struct EntryCompare {
    int operator()(const Entry& a, const Entry& b) const { return CompareInternalKey(a.ikey, b.ikey); }
  };
  using Table = SkipList<Entry, EntryCompare>;

  friend class MemTableIterator;
  Table table_;
  std::atomic<size_t> bytes_{0};
  std::atomic<size_t> entries_{0};
};

}  // namespace carnet
