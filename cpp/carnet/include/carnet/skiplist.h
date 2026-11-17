#pragma once

// A concurrent skiplist: one writer at a time (the DB serializes writes), any
// number of lock-free readers. Next-pointers are published with release
// stores and read with acquire loads, so a reader either sees a fully linked
// node or does not see it at all. Nodes are never removed; the whole list is
// freed when the memtable is dropped after a flush.

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <new>
#include <random>
#include <vector>

namespace carnet {

template <typename Key, class Comparator>
class SkipList {
 private:
  struct Node;

 public:
  static constexpr int kMaxHeight = 12;

  explicit SkipList(Comparator cmp) : cmp_(cmp), head_(NewNode(Key{}, kMaxHeight)), rng_(0xdecafbad) {
    for (int i = 0; i < kMaxHeight; i++) head_->SetNext(i, nullptr);
  }

  ~SkipList() {
    for (Node* n : allocated_) {
      n->key.~Key();
      std::free(n);
    }
  }

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // REQUIRES: no entry comparing equal to key is in the list; external sync.
  void Insert(Key key) {
    Node* prev[kMaxHeight];
    Node* x = FindGreaterOrEqual(key, prev);
    assert(x == nullptr || cmp_(key, x->key) != 0);
    (void)x;

    int height = RandomHeight();
    int cur = max_height_.load(std::memory_order_relaxed);
    if (height > cur) {
      for (int i = cur; i < height; i++) prev[i] = head_;
      max_height_.store(height, std::memory_order_relaxed);
    }
    Node* n = NewNode(std::move(key), height);
    for (int i = 0; i < height; i++) {
      n->NoBarrierSetNext(i, prev[i]->NoBarrierNext(i));
      prev[i]->SetNext(i, n);  // publish
    }
  }

  bool Contains(const Key& key) const {
    Node* x = FindGreaterOrEqual(key, nullptr);
    return x != nullptr && cmp_(key, x->key) == 0;
  }

  class Iterator {
   public:
    explicit Iterator(const SkipList* list) : list_(list) {}
    bool Valid() const { return node_ != nullptr; }
    const Key& key() const { return node_->key; }
    void Next() { node_ = node_->Next(0); }
    void Seek(const Key& target) { node_ = list_->FindGreaterOrEqual(target, nullptr); }
    void SeekToFirst() { node_ = list_->head_->Next(0); }

   private:
    const SkipList* list_;
    typename SkipList::Node* node_ = nullptr;
  };

 private:
  struct Node {
    explicit Node(Key k) : key(std::move(k)) {}
    Key key;

    Node* Next(int n) { return next_[n].load(std::memory_order_acquire); }
    void SetNext(int n, Node* x) { next_[n].store(x, std::memory_order_release); }
    Node* NoBarrierNext(int n) { return next_[n].load(std::memory_order_relaxed); }
    void NoBarrierSetNext(int n, Node* x) { next_[n].store(x, std::memory_order_relaxed); }

    // Variable-length: allocated with room for `height` pointers.
    std::atomic<Node*> next_[1];
  };

  Node* NewNode(Key key, int height) {
    size_t bytes = sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1);
    void* mem = std::malloc(bytes);
    Node* n = new (mem) Node(std::move(key));
    for (int i = 1; i < height; i++) new (&n->next_[i]) std::atomic<Node*>(nullptr);
    allocated_.push_back(n);
    return n;
  }

  int RandomHeight() {
    // Each level is 1/4 as populated as the one below (p = 0.25).
    int h = 1;
    while (h < kMaxHeight && (rng_() & 3) == 0) h++;
    return h;
  }

  Node* FindGreaterOrEqual(const Key& key, Node** prev) const {
    Node* x = head_;
    int level = max_height_.load(std::memory_order_relaxed) - 1;
    while (true) {
      Node* next = x->Next(level);
      if (next != nullptr && cmp_(next->key, key) < 0) {
        x = next;
      } else {
        if (prev != nullptr) prev[level] = x;
        if (level == 0) return next;
        level--;
      }
    }
  }

  Comparator cmp_;
  std::vector<Node*> allocated_;
  Node* head_;
  std::atomic<int> max_height_{1};
  std::minstd_rand rng_;
};

}  // namespace carnet
