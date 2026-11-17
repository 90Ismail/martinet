#include <queue>

#include "carnet/coding.h"
#include "carnet/iterator.h"

namespace carnet {
namespace {

// Min-heap merge. With L levels plus two memtables the fan-in is small, but a
// heap keeps Next() at O(log n) regardless.
class MergingIterator final : public Iterator {
 public:
  explicit MergingIterator(std::vector<std::unique_ptr<Iterator>> children)
      : children_(std::move(children)) {}

  bool Valid() const override { return current_ != nullptr; }

  void SeekToFirst() override {
    for (auto& c : children_) c->SeekToFirst();
    Rebuild();
  }

  void Seek(std::string_view target) override {
    for (auto& c : children_) c->Seek(target);
    Rebuild();
  }

  void Next() override {
    current_->Next();
    if (current_->Valid()) heap_.push(current_);
    Pop();
  }

  std::string_view key() const override { return current_->key(); }
  std::string_view value() const override { return current_->value(); }

  Status status() const override {
    for (const auto& c : children_) {
      Status s = c->status();
      if (!s.ok()) return s;
    }
    return Status::OK();
  }

 private:
  struct Greater {
    bool operator()(Iterator* a, Iterator* b) const {
      return CompareInternalKey(a->key(), b->key()) > 0;
    }
  };

  void Rebuild() {
    heap_ = {};
    for (auto& c : children_) {
      if (c->Valid()) heap_.push(c.get());
    }
    Pop();
  }

  void Pop() {
    if (heap_.empty()) {
      current_ = nullptr;
      return;
    }
    current_ = heap_.top();
    heap_.pop();
  }

  std::vector<std::unique_ptr<Iterator>> children_;
  std::priority_queue<Iterator*, std::vector<Iterator*>, Greater> heap_;
  Iterator* current_ = nullptr;
};

}  // namespace

std::unique_ptr<Iterator> NewMergingIterator(std::vector<std::unique_ptr<Iterator>> children) {
  return std::make_unique<MergingIterator>(std::move(children));
}

}  // namespace carnet
