#pragma once

#include <string>
#include <utility>

namespace carnet {

// Status is a cheap error type in the LevelDB style: no exceptions cross the
// storage engine boundary.
class Status {
 public:
  enum class Code { kOk, kNotFound, kCorruption, kIOError, kInvalidArgument, kBusy };

  Status() = default;
  static Status OK() { return Status(); }
  static Status NotFound(std::string msg = "") { return Status(Code::kNotFound, std::move(msg)); }
  static Status Corruption(std::string msg) { return Status(Code::kCorruption, std::move(msg)); }
  static Status IOError(std::string msg) { return Status(Code::kIOError, std::move(msg)); }
  static Status InvalidArgument(std::string msg) {
    return Status(Code::kInvalidArgument, std::move(msg));
  }
  static Status Busy(std::string msg) { return Status(Code::kBusy, std::move(msg)); }

  bool ok() const { return code_ == Code::kOk; }
  bool IsNotFound() const { return code_ == Code::kNotFound; }
  bool IsCorruption() const { return code_ == Code::kCorruption; }
  Code code() const { return code_; }
  const std::string& message() const { return msg_; }

  std::string ToString() const {
    switch (code_) {
      case Code::kOk: return "OK";
      case Code::kNotFound: return "NotFound: " + msg_;
      case Code::kCorruption: return "Corruption: " + msg_;
      case Code::kIOError: return "IOError: " + msg_;
      case Code::kInvalidArgument: return "InvalidArgument: " + msg_;
      case Code::kBusy: return "Busy: " + msg_;
    }
    return "Unknown";
  }

 private:
  Status(Code c, std::string m) : code_(c), msg_(std::move(m)) {}
  Code code_ = Code::kOk;
  std::string msg_;
};

#define CARNET_RETURN_IF_ERROR(expr)      \
  do {                                    \
    ::carnet::Status _s = (expr);         \
    if (!_s.ok()) return _s;              \
  } while (0)

}  // namespace carnet
