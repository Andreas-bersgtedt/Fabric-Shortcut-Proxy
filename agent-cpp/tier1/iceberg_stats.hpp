// Iceberg single-value binary encoding (min/max bounds), ported from iceberg/stats.py
// `encode_bound` / `_encode_decimal`. The Parquet-stats reader (collect_split_stats)
// stays in Python (Arrow). Pure, byte-exact. Assumes a little-endian host. C++17.
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace fsp {

inline std::string le_bytes(uint64_t v, int n) {
    std::string s;
    for (int i = 0; i < n; ++i) s += static_cast<char>((v >> (8 * i)) & 0xff);
    return s;
}

inline std::string encode_bool(bool v) { return v ? std::string("\x01", 1) : std::string("\x00", 1); }
inline std::string encode_int32(int32_t v) { return le_bytes(static_cast<uint32_t>(v), 4); }
inline std::string encode_long(int64_t v) { return le_bytes(static_cast<uint64_t>(v), 8); }
inline std::string encode_float(float f) {
    uint32_t b;
    std::memcpy(&b, &f, 4);
    return le_bytes(b, 4);
}
inline std::string encode_double(double d) {
    uint64_t b;
    std::memcpy(&b, &d, 8);
    return le_bytes(b, 8);
}
inline std::string encode_date_days(int32_t days) { return encode_int32(days); }
inline std::string encode_micros(int64_t micros) { return encode_long(micros); }
inline std::string encode_string(const std::string& utf8) { return utf8; }
inline std::string encode_binary(const std::string& bytes) { return bytes; }
inline std::string encode_uuid16(const std::string& raw16) { return raw16; }

// Big-endian minimal magnitude bytes of an unscaled decimal digit string (empty for zero).
inline std::vector<uint8_t> dec_magnitude_bytes(std::string digits) {
    size_t nz = digits.find_first_not_of('0');
    if (nz == std::string::npos) return {};  // zero
    digits = digits.substr(nz);
    std::vector<uint8_t> le;
    while (digits != "0") {
        std::string q;
        int rem = 0;
        for (char c : digits) {
            int cur = rem * 10 + (c - '0');
            q += static_cast<char>('0' + cur / 256);
            rem = cur % 256;
        }
        size_t z = q.find_first_not_of('0');
        q = (z == std::string::npos) ? "0" : q.substr(z);
        le.push_back(static_cast<uint8_t>(rem));
        digits = q;
    }
    std::reverse(le.begin(), le.end());
    return le;
}

// Two's-complement big-endian, minimal signed length (Iceberg decimal bound).
inline std::string encode_decimal(const std::string& value) {
    bool neg = !value.empty() && value[0] == '-';
    std::string body = neg ? value.substr(1) : value;
    std::string unscaled;
    for (char c : body)
        if (c != '.') unscaled += c;  // intpart + fracpart, trailing zeros kept

    std::vector<uint8_t> mag = dec_magnitude_bytes(unscaled);
    if (mag.empty()) neg = false;  // -0 == 0

    int bit_length = 0;
    if (!mag.empty()) {
        int top = mag.front();
        int bits = 0;
        while (top) { ++bits; top >>= 1; }
        bit_length = static_cast<int>(mag.size() - 1) * 8 + bits;
    }
    int length = (bit_length + 8) / 8;
    if (length < 1) length = 1;

    std::vector<uint8_t> out(length, 0);
    for (size_t i = 0; i < mag.size(); ++i) out[length - 1 - i] = mag[mag.size() - 1 - i];

    if (neg) {  // two's complement over `length` bytes
        int carry = 1;
        for (int i = length - 1; i >= 0; --i) {
            int v = (~out[i] & 0xff) + carry;
            out[i] = static_cast<uint8_t>(v & 0xff);
            carry = v >> 8;
        }
    }
    return std::string(out.begin(), out.end());
}

inline std::string to_hex(const std::string& bytes) {
    static const char* h = "0123456789abcdef";
    std::string out;
    for (unsigned char c : bytes) {
        out += h[c >> 4];
        out += h[c & 0xf];
    }
    return out;
}

}  // namespace fsp
