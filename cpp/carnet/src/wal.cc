#include "carnet/wal.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <iterator>
#include <vector>

#include "carnet/coding.h"
#include "carnet/crc32c.h"

namespace carnet {
namespace {

Status Errno(const std::string& ctx) { return Status::IOError(ctx + ": " + std::strerror(errno)); }

Status WriteAll(int fd, const char* data, size_t n) {
  while (n > 0) {
    ssize_t w = ::write(fd, data, n);
    if (w < 0) {
      if (errno == EINTR) continue;
      return Errno("wal write");
    }
    data += w;
    n -= static_cast<size_t>(w);
  }
  return Status::OK();
}

}  // namespace

Status WalWriter::Open(const std::string& path, std::unique_ptr<WalWriter>* out) {
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd < 0) return Errno("open " + path);
  out->reset(new WalWriter(fd, path));
  off_t end = ::lseek(fd, 0, SEEK_END);
  (*out)->size_ = end > 0 ? static_cast<uint64_t>(end) : 0;
  return Status::OK();
}

WalWriter::~WalWriter() {
  if (fd_ >= 0) ::close(fd_);
}

Status WalWriter::Append(std::string_view payload, bool sync) {
  std::string rec;
  rec.reserve(8 + payload.size());
  std::string len;
  PutFixed32(&len, static_cast<uint32_t>(payload.size()));
  uint32_t crc = crc32c::Extend(crc32c::Value(len.data(), len.size()), payload.data(), payload.size());
  PutFixed32(&rec, crc32c::Mask(crc));
  rec += len;
  rec.append(payload.data(), payload.size());
  CARNET_RETURN_IF_ERROR(WriteAll(fd_, rec.data(), rec.size()));
  size_ += rec.size();
  return sync ? Sync() : Status::OK();
}

Status WalWriter::Sync() {
#ifdef __APPLE__
  // fsync on macOS does not flush the drive cache; F_FULLFSYNC does.
  if (::fcntl(fd_, F_FULLFSYNC) == 0) return Status::OK();
#endif
  if (::fsync(fd_) != 0) return Errno("fsync " + path_);
  return Status::OK();
}

Status ReplayWal(const std::string& path, const std::function<Status(std::string_view)>& fn,
                 WalReplayStats* stats) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return Status::NotFound(path);
  std::vector<char> buf((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());

  size_t pos = 0;
  while (pos + 8 <= buf.size()) {
    uint32_t masked = DecodeFixed32(&buf[pos]);
    uint32_t len = DecodeFixed32(&buf[pos + 4]);
    if (pos + 8 + len > buf.size()) break;  // short read: torn tail
    uint32_t crc = crc32c::Extend(crc32c::Value(&buf[pos + 4], 4), &buf[pos + 8], len);
    if (crc32c::Unmask(masked) != crc) break;  // corrupt: stop here
    CARNET_RETURN_IF_ERROR(fn(std::string_view(&buf[pos + 8], len)));
    if (stats) stats->records++;
    pos += 8 + len;
  }
  if (stats) stats->dropped_bytes = buf.size() - pos;
  return Status::OK();
}

}  // namespace carnet
