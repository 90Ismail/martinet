#include "carnet/crc32c.h"

#include <array>
#include <cstring>

#if defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#elif defined(__SSE4_2__)
#include <nmmintrin.h>
#endif

namespace carnet::crc32c {
namespace {

constexpr uint32_t kPoly = 0x82f63b78u;  // reversed Castagnoli polynomial

constexpr std::array<uint32_t, 256> MakeTable() {
  std::array<uint32_t, 256> t{};
  for (uint32_t i = 0; i < 256; i++) {
    uint32_t c = i;
    for (int k = 0; k < 8; k++) c = (c & 1) ? (kPoly ^ (c >> 1)) : (c >> 1);
    t[i] = c;
  }
  return t;
}

constexpr auto kTable = MakeTable();

}  // namespace

uint32_t Extend(uint32_t crc, const char* data, size_t n) {
  uint32_t c = crc ^ 0xffffffffu;
  const auto* p = reinterpret_cast<const uint8_t*>(data);
#if defined(__ARM_FEATURE_CRC32) || defined(__SSE4_2__)
  // Hardware CRC32C: 8 bytes per instruction.
  for (; n >= 8; n -= 8, p += 8) {
    uint64_t w;
    std::memcpy(&w, p, 8);
#if defined(__ARM_FEATURE_CRC32)
    c = __crc32cd(c, w);
#else
    c = static_cast<uint32_t>(_mm_crc32_u64(c, w));
#endif
  }
#endif
  for (size_t i = 0; i < n; i++) c = kTable[(c ^ p[i]) & 0xff] ^ (c >> 8);
  return c ^ 0xffffffffu;
}

}  // namespace carnet::crc32c
