#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "carnet/status.h"

namespace carnet {

// Iterator over (internal key, value) pairs in internal-key order.
class Iterator {
 public:
  virtual ~Iterator() = default;
  virtual bool Valid() const = 0;
  virtual void SeekToFirst() = 0;
  virtual void Seek(std::string_view internal_key) = 0;
  virtual void Next() = 0;
  virtual std::string_view key() const = 0;  // internal key
  virtual std::string_view value() const = 0;
  virtual Status status() const { return Status::OK(); }
};

// K-way merge of sorted children. Children must not contain equal internal
// keys (sequence numbers are unique, so this holds by construction).
std::unique_ptr<Iterator> NewMergingIterator(std::vector<std::unique_ptr<Iterator>> children);

}  // namespace carnet
