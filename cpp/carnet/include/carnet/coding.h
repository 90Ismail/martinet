#pragma once

// Byte-level encodings shared by the WAL, memtable and SSTables.
//
// An *internal key* is the user key followed by an 8-byte little-endian tag:
//     tag = (sequence << 8) | value_type
// Internal keys sort by user key ascending, then by sequence DESCENDING, so a
// forward scan always sees the newest version of a key first. This is what
// makes MVCC snapshots and tombstones work without any extra index.

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace carnet {

enum class ValueType : uint8_t { kDelete = 0, kPut = 1 };
using SequenceNumber = uint64_t;
constexpr SequenceNumber kMaxSequence = (1ull << 56) - 1;

inline void PutFixed32(std::string* dst, uint32_t v) {
  char buf[4];
  for (int i = 0; i < 4; i++) buf[i] = static_cast<char>(v >> (8 * i));
  dst->append(buf, 4);
}

inline void PutFixed64(std::string* dst, uint64_t v) {
  char buf[8];
  for (int i = 0; i < 8; i++) buf[i] = static_cast<char>(v >> (8 * i));
  dst->append(buf, 8);
}

inline uint32_t DecodeFixed32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; i++) v |= static_cast<uint32_t>(static_cast<uint8_t>(p[i])) << (8 * i);
  return v;
}

inline uint64_t DecodeFixed64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; i++) v |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i);
  return v;
}

inline void PutVarint32(std::string* dst, uint32_t v) {
  while (v >= 0x80) {
    dst->push_back(static_cast<char>(v | 0x80));
    v >>= 7;
  }
  dst->push_back(static_cast<char>(v));
}

inline bool GetVarint32(std::string_view* in, uint32_t* out) {
  uint32_t result = 0;
  for (int shift = 0; shift <= 28 && !in->empty(); shift += 7) {
    uint8_t byte = static_cast<uint8_t>((*in)[0]);
    in->remove_prefix(1);
    result |= static_cast<uint32_t>(byte & 0x7f) << shift;
    if ((byte & 0x80) == 0) {
      *out = result;
      return true;
    }
  }
  return false;
}

inline void PutLengthPrefixed(std::string* dst, std::string_view s) {
  PutVarint32(dst, static_cast<uint32_t>(s.size()));
  dst->append(s.data(), s.size());
}

inline bool GetLengthPrefixed(std::string_view* in, std::string_view* out) {
  uint32_t len;
  if (!GetVarint32(in, &len) || in->size() < len) return false;
  *out = in->substr(0, len);
  in->remove_prefix(len);
  return true;
}

inline std::string MakeInternalKey(std::string_view user_key, SequenceNumber seq, ValueType t) {
  std::string k;
  k.reserve(user_key.size() + 8);
  k.append(user_key.data(), user_key.size());
  PutFixed64(&k, (seq << 8) | static_cast<uint8_t>(t));
  return k;
}

inline std::string_view ExtractUserKey(std::string_view ikey) {
  return ikey.substr(0, ikey.size() - 8);
}
inline uint64_t ExtractTag(std::string_view ikey) { return DecodeFixed64(ikey.data() + ikey.size() - 8); }
inline SequenceNumber ExtractSequence(std::string_view ikey) { return ExtractTag(ikey) >> 8; }
inline ValueType ExtractValueType(std::string_view ikey) {
  return static_cast<ValueType>(ExtractTag(ikey) & 0xff);
}

inline int CompareInternalKey(std::string_view a, std::string_view b) {
  int r = ExtractUserKey(a).compare(ExtractUserKey(b));
  if (r != 0) return r < 0 ? -1 : 1;
  uint64_t ta = ExtractTag(a), tb = ExtractTag(b);
  if (ta > tb) return -1;  // newer first
  if (ta < tb) return 1;
  return 0;
}

struct InternalKeyLess {
  bool operator()(std::string_view a, std::string_view b) const {
    return CompareInternalKey(a, b) < 0;
  }
};

// Key a Get() seeks to: the first entry for user_key with sequence <= seq.
inline std::string LookupKey(std::string_view user_key, SequenceNumber seq) {
  return MakeInternalKey(user_key, seq, ValueType::kPut);
}

}  // namespace carnet
