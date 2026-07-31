// Compact MD5 (RFC 1321), used only for the deterministic Delta table UUID.
// Public-domain style reference implementation. C++17.
#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <string>

namespace fsp {

class Md5 {
public:
    Md5() { reset(); }

    void update(const uint8_t* data, size_t len) {
        size_t index = static_cast<size_t>((count_[0] >> 3) & 0x3f);
        count_[0] += static_cast<uint32_t>(len << 3);
        if (count_[0] < (len << 3)) count_[1]++;
        count_[1] += static_cast<uint32_t>(len >> 29);
        size_t part = 64 - index;
        size_t i = 0;
        if (len >= part) {
            std::memcpy(&buffer_[index], data, part);
            transform(buffer_.data());
            for (i = part; i + 63 < len; i += 64) transform(&data[i]);
            index = 0;
        }
        std::memcpy(&buffer_[index], &data[i], len - i);
    }

    std::string hex() {
        static const uint8_t kPadding[64] = {0x80};
        uint8_t bits[8];
        encode(bits, count_, 8);
        size_t index = static_cast<size_t>((count_[0] >> 3) & 0x3f);
        size_t padLen = (index < 56) ? (56 - index) : (120 - index);
        update(kPadding, padLen);
        update(bits, 8);
        uint8_t digest[16];
        encode(digest, state_, 16);
        static const char* h = "0123456789abcdef";
        std::string out;
        out.reserve(32);
        for (int i = 0; i < 16; ++i) {
            out += h[digest[i] >> 4];
            out += h[digest[i] & 0xf];
        }
        return out;
    }

private:
    void reset() {
        count_[0] = count_[1] = 0;
        state_[0] = 0x67452301;
        state_[1] = 0xefcdab89;
        state_[2] = 0x98badcfe;
        state_[3] = 0x10325476;
    }

    static uint32_t rol(uint32_t x, int n) { return (x << n) | (x >> (32 - n)); }
    static uint32_t F(uint32_t x, uint32_t y, uint32_t z) { return (x & y) | (~x & z); }
    static uint32_t G(uint32_t x, uint32_t y, uint32_t z) { return (x & z) | (y & ~z); }
    static uint32_t H(uint32_t x, uint32_t y, uint32_t z) { return x ^ y ^ z; }
    static uint32_t I(uint32_t x, uint32_t y, uint32_t z) { return y ^ (x | ~z); }

    static void encode(uint8_t* out, const uint32_t* in, size_t len) {
        for (size_t i = 0, j = 0; j < len; i++, j += 4) {
            out[j] = static_cast<uint8_t>(in[i] & 0xff);
            out[j + 1] = static_cast<uint8_t>((in[i] >> 8) & 0xff);
            out[j + 2] = static_cast<uint8_t>((in[i] >> 16) & 0xff);
            out[j + 3] = static_cast<uint8_t>((in[i] >> 24) & 0xff);
        }
    }

    static void decode(uint32_t* out, const uint8_t* in, size_t len) {
        for (size_t i = 0, j = 0; j < len; i++, j += 4)
            out[i] = static_cast<uint32_t>(in[j]) | (static_cast<uint32_t>(in[j + 1]) << 8) |
                     (static_cast<uint32_t>(in[j + 2]) << 16) | (static_cast<uint32_t>(in[j + 3]) << 24);
    }

    void transform(const uint8_t block[64]) {
        static const uint32_t K[64] = {
            0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
            0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
            0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
            0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
            0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
            0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
            0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
            0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391};
        static const int S[64] = {7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
                                  5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20,
                                  4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
                                  6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21};
        uint32_t a = state_[0], b = state_[1], c = state_[2], d = state_[3];
        uint32_t x[16];
        decode(x, block, 64);
        for (int i = 0; i < 64; ++i) {
            uint32_t f;
            int g;
            if (i < 16) { f = F(b, c, d); g = i; }
            else if (i < 32) { f = G(b, c, d); g = (5 * i + 1) & 15; }
            else if (i < 48) { f = H(b, c, d); g = (3 * i + 5) & 15; }
            else { f = I(b, c, d); g = (7 * i) & 15; }
            uint32_t tmp = d;
            d = c;
            c = b;
            b = b + rol(a + f + K[i] + x[g], S[i]);
            a = tmp;
        }
        state_[0] += a;
        state_[1] += b;
        state_[2] += c;
        state_[3] += d;
    }

    uint32_t state_[4];
    uint32_t count_[2];
    std::array<uint8_t, 64> buffer_{};
};

inline std::string md5_hex(const std::string& s) {
    Md5 m;
    m.update(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    return m.hex();
}

}  // namespace fsp
