#pragma once

// Write-ahead log. Every write batch is appended here (and optionally
// fsync'd) before it touches the memtable, so a crash never loses an
// acknowledged write.
//
// Record framing:   [masked crc32c : 4][length : 4][payload : length]
// The CRC covers length + payload. On recovery, the first record with a bad
// CRC or a short read marks the torn tail of the log: everything before it is
// replayed, everything after it is discarded.

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "carnet/status.h"

namespace carnet {

class WalWriter {
 public:
  static Status Open(const std::string& path, std::unique_ptr<WalWriter>* out);
  ~WalWriter();

  Status Append(std::string_view payload, bool sync);
  Status Sync();
  uint64_t size() const { return size_; }

 private:
  WalWriter(int fd, std::string path) : fd_(fd), path_(std::move(path)) {}
  int fd_;
  std::string path_;
  uint64_t size_ = 0;
};

struct WalReplayStats {
  uint64_t records = 0;
  uint64_t dropped_bytes = 0;  // torn tail
};

// Calls fn for every intact record in order.
Status ReplayWal(const std::string& path, const std::function<Status(std::string_view)>& fn,
                 WalReplayStats* stats);

}  // namespace carnet
