#include "carnet/memtable.h"

namespace carnet {

MemTable::MemTable() : table_(EntryCompare{}) {}

void MemTable::Add(SequenceNumber seq, ValueType type, std::string_view key, std::string_view value) {
  Entry e{MakeInternalKey(key, seq, type), std::string(value)};
  size_t sz = e.ikey.size() + e.value.size() + 48;  // + rough node overhead
  table_.Insert(std::move(e));
  bytes_.fetch_add(sz, std::memory_order_relaxed);
  entries_.fetch_add(1, std::memory_order_relaxed);
}

bool MemTable::Get(std::string_view key, SequenceNumber seq, std::string* value, bool* deleted) const {
  Table::Iterator it(&table_);
  it.Seek(Entry{LookupKey(key, seq), {}});
  if (!it.Valid()) return false;
  const Entry& e = it.key();
  if (ExtractUserKey(e.ikey) != key) return false;
  if (ExtractValueType(e.ikey) == ValueType::kDelete) {
    *deleted = true;
  } else {
    *deleted = false;
    value->assign(e.value);
  }
  return true;
}

class MemTableIterator final : public Iterator {
 public:
  explicit MemTableIterator(const MemTable::Table* t) : it_(t) {}
  bool Valid() const override { return it_.Valid(); }
  void SeekToFirst() override { it_.SeekToFirst(); }
  void Seek(std::string_view ikey) override { it_.Seek(MemTable::Entry{std::string(ikey), {}}); }
  void Next() override { it_.Next(); }
  std::string_view key() const override { return it_.key().ikey; }
  std::string_view value() const override { return it_.key().value; }

 private:
  MemTable::Table::Iterator it_;
};

std::unique_ptr<Iterator> MemTable::NewIterator() const {
  return std::make_unique<MemTableIterator>(&table_);
}

}  // namespace carnet
