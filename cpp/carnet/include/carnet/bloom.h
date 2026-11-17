#pragma once

// Bloom filter over the user keys of one SSTable. A negative answer lets a
// point lookup skip the file without any disk I/O (DDIA ch. 3).

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace carnet {

class BloomFilterBuilder {
 public:
  explicit BloomFilterBuilder(int bits_per_key = 10);
  void AddKey(std::string_view key);
  // Serialized filter: bit array followed by one byte holding k.
  std::string Finish() const;
  size_t NumKeys() const { return hashes_.size(); }

 private:
  int bits_per_key_;
  int k_;
  std::vector<uint32_t> hashes_;
};

bool BloomMayContain(std::string_view filter, std::string_view key);

uint32_t BloomHash(std::string_view key);

}  // namespace carnet
