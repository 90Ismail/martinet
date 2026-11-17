#include "carnet/bloom.h"

#include <algorithm>
#include <cstdint>

namespace carnet {

// Murmur-inspired 32-bit hash.
uint32_t BloomHash(std::string_view key) {
  constexpr uint32_t m = 0xc6a4a793;
  uint32_t h = 0xbc9f1d34 ^ static_cast<uint32_t>(key.size() * m);
  size_t i = 0;
  for (; i + 4 <= key.size(); i += 4) {
    uint32_t w = static_cast<uint8_t>(key[i]) | (static_cast<uint8_t>(key[i + 1]) << 8) |
                 (static_cast<uint8_t>(key[i + 2]) << 16) | (static_cast<uint32_t>(static_cast<uint8_t>(key[i + 3])) << 24);
    h += w;
    h *= m;
    h ^= (h >> 16);
  }
  switch (key.size() - i) {
    case 3: h += static_cast<uint8_t>(key[i + 2]) << 16; [[fallthrough]];
    case 2: h += static_cast<uint8_t>(key[i + 1]) << 8; [[fallthrough]];
    case 1:
      h += static_cast<uint8_t>(key[i]);
      h *= m;
      h ^= (h >> 24);
  }
  return h;
}

BloomFilterBuilder::BloomFilterBuilder(int bits_per_key) : bits_per_key_(bits_per_key) {
  // Optimal k = ln(2) * bits_per_key.
  k_ = std::clamp(static_cast<int>(bits_per_key * 0.69), 1, 30);
}

void BloomFilterBuilder::AddKey(std::string_view key) { hashes_.push_back(BloomHash(key)); }

std::string BloomFilterBuilder::Finish() const {
  size_t bits = std::max<size_t>(64, hashes_.size() * bits_per_key_);
  size_t bytes = (bits + 7) / 8;
  bits = bytes * 8;
  std::string out(bytes, '\0');
  for (uint32_t h : hashes_) {
    // Double hashing: g_i(x) = h1(x) + i * h2(x)  (Kirsch & Mitzenmacher).
    uint32_t delta = (h >> 17) | (h << 15);
    for (int j = 0; j < k_; j++) {
      uint32_t bit = h % bits;
      out[bit / 8] |= static_cast<char>(1 << (bit % 8));
      h += delta;
    }
  }
  out.push_back(static_cast<char>(k_));
  return out;
}

bool BloomMayContain(std::string_view filter, std::string_view key) {
  if (filter.size() < 2) return true;  // no filter: must check
  size_t bytes = filter.size() - 1;
  size_t bits = bytes * 8;
  int k = static_cast<uint8_t>(filter.back());
  if (k > 30) return true;  // reserved for future encodings
  uint32_t h = BloomHash(key);
  uint32_t delta = (h >> 17) | (h << 15);
  for (int j = 0; j < k; j++) {
    uint32_t bit = h % bits;
    if ((filter[bit / 8] & (1 << (bit % 8))) == 0) return false;
    h += delta;
  }
  return true;
}

}  // namespace carnet
