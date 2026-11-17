#pragma once

#include <cstddef>
#include <cstdint>

namespace carnet::crc32c {

// CRC-32C (Castagnoli). Used to detect torn writes in the WAL and bit rot in
// SSTable blocks.
uint32_t Extend(uint32_t crc, const char* data, size_t n);
inline uint32_t Value(const char* data, size_t n) { return Extend(0, data, n); }

// Masking (as in LevelDB) avoids problems when computing the CRC of a string
// that itself contains embedded CRCs.
constexpr uint32_t kMaskDelta = 0xa282ead8ul;
inline uint32_t Mask(uint32_t crc) { return ((crc >> 15) | (crc << 17)) + kMaskDelta; }
inline uint32_t Unmask(uint32_t masked) {
  uint32_t rot = masked - kMaskDelta;
  return (rot >> 17) | (rot << 15);
}

}  // namespace carnet::crc32c
