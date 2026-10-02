#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "tier1/sha256.hpp"

namespace fsp::sigv4 {

struct VerificationError {
    std::string code;
    std::string message;
};

struct VerificationResult {
    bool ok;
    std::string access_key_id;
    VerificationError error;
};

namespace detail {

inline VerificationResult failure(const char* code, const char* message) {
    return {false, {}, {code, message}};
}

inline std::string lowercase(std::string text) {
    for (char& c : text) {
        if (c >= 'A' && c <= 'Z') c = static_cast<char>(c + ('a' - 'A'));
    }
    return text;
}

inline bool whitespace(unsigned char c) {
    return c == ' ' || (c >= '\t' && c <= '\r') || (c >= 0x1c && c <= 0x1f);
}

inline std::string trim(const std::string& text) {
    std::size_t begin = 0, end = text.size();
    while (begin < end && whitespace(static_cast<unsigned char>(text[begin]))) ++begin;
    while (end > begin && whitespace(static_cast<unsigned char>(text[end - 1]))) --end;
    return text.substr(begin, end - begin);
}

inline std::string canonical_header(const std::string& text) {
    std::string result;
    bool pending_space = false;
    for (unsigned char c : text) {
        if (whitespace(c)) {
            pending_space = !result.empty();
        } else {
            if (pending_space) result += ' ';
            result += static_cast<char>(c);
            pending_space = false;
        }
    }
    return result;
}

inline std::vector<std::string> split(const std::string& text, char separator) {
    std::vector<std::string> parts;
    std::size_t begin = 0;
    for (;;) {
        const auto end = text.find(separator, begin);
        parts.push_back(text.substr(begin, end == std::string::npos
            ? std::string::npos : end - begin));
        if (end == std::string::npos) return parts;
        begin = end + 1;
    }
}

inline int hex_value(unsigned char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

inline std::string uri_encode(const std::string& text, bool preserve_slashes = false) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string result;
    for (unsigned char c : text) {
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_' ||
            c == '.' || c == '~' || (preserve_slashes && c == '/')) {
            result += static_cast<char>(c);
        } else {
            result += '%';
            result += hex[c >> 4];
            result += hex[c & 15];
        }
    }
    return result;
}

// Keep valid client escapes byte-for-byte, matching the Python verifier.
// Unit cases: /b/a%20b, /b/a%2Fb, /b/%252F, /b/a%2fb, and /b//./../
// remain unchanged; malformed percent signs are escaped as literal '%' bytes.
// An empty path canonicalizes to "/".
inline std::string canonical_uri(const std::string& encoded_path) {
    if (encoded_path.empty()) return "/";
    std::string result;
    result.reserve(encoded_path.size());
    for (std::size_t i = 0; i < encoded_path.size(); ++i) {
        if (encoded_path[i] != '%') {
            result += encoded_path[i];
            continue;
        }
        if (encoded_path.size() - i >= 3 &&
            hex_value(static_cast<unsigned char>(encoded_path[i + 1])) >= 0 &&
            hex_value(static_cast<unsigned char>(encoded_path[i + 2])) >= 0) {
            result.append(encoded_path, i, 3);
            i += 2;
        } else {
            result += "%25";
        }
    }
    return result;
}

// urllib.parse.unquote uses UTF-8 with replacement for invalid sequences.
inline std::string replace_invalid_utf8(const std::string& bytes) {
    std::string result;
    std::size_t i = 0;
    while (i < bytes.size()) {
        const auto first = static_cast<unsigned char>(bytes[i]);
        if (first < 0x80) {
            result += bytes[i++];
            continue;
        }
        const int length = first >= 0xc2 && first <= 0xdf ? 2 :
            first >= 0xe0 && first <= 0xef ? 3 :
            first >= 0xf0 && first <= 0xf4 ? 4 : 0;
        std::size_t consumed = 1;
        if (length != 0) {
            while (consumed < static_cast<std::size_t>(length) &&
                   i + consumed < bytes.size()) {
                const auto next = static_cast<unsigned char>(bytes[i + consumed]);
                if (next < 0x80 || next > 0xbf) break;
                if (consumed == 1 &&
                    ((first == 0xe0 && next < 0xa0) ||
                     (first == 0xed && next > 0x9f) ||
                     (first == 0xf0 && next < 0x90) ||
                     (first == 0xf4 && next > 0x8f))) break;
                ++consumed;
            }
        }
        if (length != 0 && consumed == static_cast<std::size_t>(length)) {
            result.append(bytes, i, consumed);
        } else {
            result += "\xEF\xBF\xBD";
        }
        i += consumed;
    }
    return result;
}

inline std::string uri_decode(const std::string& text) {
    std::string bytes;
    for (std::size_t i = 0; i < text.size(); ++i) {
        if (text[i] == '%' && text.size() - i >= 3) {
            const int high = hex_value(static_cast<unsigned char>(text[i + 1]));
            const int low = hex_value(static_cast<unsigned char>(text[i + 2]));
            if (high >= 0 && low >= 0) {
                bytes += static_cast<char>((high << 4) | low);
                i += 2;
                continue;
            }
        }
        // Unlike form encoding, '+' remains a literal plus.
        bytes += text[i];
    }
    return replace_invalid_utf8(bytes);
}

inline std::string canonical_query(const std::string& query) {
    std::vector<std::pair<std::string, std::string>> pairs;
    for (const auto& part : split(query, '&')) {
        if (part.empty()) continue;
        const auto equal = part.find('=');
        pairs.emplace_back(uri_encode(uri_decode(part.substr(0, equal))),
            uri_encode(uri_decode(equal == std::string::npos
                ? std::string{} : part.substr(equal + 1))));
    }
    std::sort(pairs.begin(), pairs.end());
    std::string result;
    for (const auto& pair : pairs) {
        if (!result.empty()) result += '&';
        result += pair.first + "=" + pair.second;
    }
    return result;
}

inline std::string digest_bytes(const std::string& text) {
    const auto hex = fsp::sha256_hex(text);
    std::string result;
    result.reserve(32);
    for (std::size_t i = 0; i < hex.size(); i += 2) {
        result += static_cast<char>((hex_value(static_cast<unsigned char>(hex[i])) << 4) |
            hex_value(static_cast<unsigned char>(hex[i + 1])));
    }
    return result;
}

inline std::string hmac_sha256(std::string key, const std::string& message) {
    if (key.size() > 64) key = digest_bytes(key);
    key.resize(64, '\0');
    std::string inner(64, '\0'), outer(64, '\0');
    for (std::size_t i = 0; i < 64; ++i) {
        const auto byte = static_cast<unsigned char>(key[i]);
        inner[i] = static_cast<char>(byte ^ 0x36);
        outer[i] = static_cast<char>(byte ^ 0x5c);
    }
    return digest_bytes(outer + digest_bytes(inner + message));
}

inline std::string signing_key(const std::string& secret, const std::string& date,
                               const std::string& region, const std::string& service) {
    return hmac_sha256(hmac_sha256(hmac_sha256(hmac_sha256("AWS4" + secret, date),
        region), service), "aws4_request");
}

inline std::string hex_encode(const std::string& bytes) {
    static constexpr char hex[] = "0123456789abcdef";
    std::string result;
    result.reserve(bytes.size() * 2);
    for (unsigned char c : bytes) {
        result += hex[c >> 4];
        result += hex[c & 15];
    }
    return result;
}

inline bool signature_equal(const std::string& expected, const std::string& provided) {
    if (expected.size() != provided.size()) return false;
    volatile unsigned int difference = 0;
    for (std::size_t i = 0; i < expected.size(); ++i) {
        difference = difference |
            (static_cast<unsigned char>(expected[i]) ^ static_cast<unsigned char>(provided[i]));
    }
    return difference == 0;
}

inline bool leap_year(int year) {
    return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

// Gregorian UTC arithmetic avoids non-portable timegm and local timezone/DST.
inline bool parse_date(const std::string& date, std::int64_t& seconds) {
    if (date.size() != 16 || date[8] != 'T' || date[15] != 'Z') return false;
    for (std::size_t i = 0; i < date.size(); ++i) {
        if (i != 8 && i != 15 && (date[i] < '0' || date[i] > '9')) return false;
    }
    const auto number = [&date](std::size_t begin, std::size_t count) {
        int result = 0;
        for (std::size_t i = begin; i < begin + count; ++i) result = result * 10 + date[i] - '0';
        return result;
    };
    const int year = number(0, 4), month = number(4, 2), day = number(6, 2);
    const int hour = number(9, 2), minute = number(11, 2), second = number(13, 2);
    static constexpr int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year == 0 || month < 1 || month > 12 || day < 1 ||
        day > month_days[month - 1] + (month == 2 && leap_year(year) ? 1 : 0) ||
        hour > 23 || minute > 59 || second > 59) return false;
    const std::int64_t prior_year = year - 1;
    std::int64_t days = 365 * prior_year + prior_year / 4 - prior_year / 100 +
        prior_year / 400 - 719162;
    for (int m = 1; m < month; ++m) days += month_days[m - 1];
    if (month > 2 && leap_year(year)) ++days;
    days += day - 1;
    seconds = days * 86400 + hour * 3600 + minute * 60 + second;
    return true;
}

} // namespace detail

// encoded_path must be the raw HTTP path component, already client-encoded,
// without query/fragment, decoding, or normalization. Existing valid percent
// escapes are preserved; malformed ones are rejected by the HTTP parser.
// No body is accepted: the payload hash header is signed as supplied, as in
// Python's no-body mode.
inline VerificationResult verify(
    const std::string& method,
    const std::string& encoded_path,
    const std::string& raw_query,
    const std::map<std::string, std::string>& headers,
    const std::string& expected_access_key,
    const std::string& secret,
    std::time_t now = std::time(nullptr)) {
    std::map<std::string, std::string> lower;
    for (const auto& header : headers) lower[detail::lowercase(header.first)] = header.second;

    const auto auth = lower.find("authorization");
    const std::string algorithm = "AWS4-HMAC-SHA256";
    if (auth == lower.end() || auth->second.compare(0, algorithm.size(), algorithm) != 0) {
        return detail::failure("AccessDenied", "Missing or unsupported Authorization header.");
    }
    std::map<std::string, std::string> parts;
    for (const auto& item : detail::split(detail::trim(auth->second.substr(algorithm.size())), ',')) {
        const auto equal = item.find('=');
        if (equal != std::string::npos) {
            parts[detail::trim(item.substr(0, equal))] = detail::trim(item.substr(equal + 1));
        }
    }
    if (parts.find("Credential") == parts.end() ||
        parts.find("SignedHeaders") == parts.end() || parts.find("Signature") == parts.end()) {
        return detail::failure("AccessDenied", "Malformed Authorization header: missing required field.");
    }
    const auto credential = detail::split(parts.at("Credential"), '/');
    if (credential.size() != 5) {
        return detail::failure("AccessDenied", "Malformed credential scope.");
    }
    if (credential[3] != "s3" || credential[4] != "aws4_request") {
        return detail::failure("AccessDenied", "Unexpected credential scope.");
    }
    if (credential[0] != expected_access_key) {
        return detail::failure("InvalidAccessKeyId", "The access key id does not match.");
    }
    const auto date = lower.find("x-amz-date");
    if (date == lower.end() || date->second.empty()) {
        return detail::failure("AccessDenied", "Missing x-amz-date header.");
    }
    std::int64_t request_seconds = 0;
    if (!detail::parse_date(date->second, request_seconds)) {
        return detail::failure("AccessDenied", "Malformed x-amz-date header.");
    }
    if (credential[1] != date->second.substr(0, 8)) {
        return detail::failure("SignatureDoesNotMatch", "Credential scope date does not match x-amz-date.");
    }
    const long double skew = static_cast<long double>(now) - request_seconds;
    if (skew < -900 || skew > 900) {
        return detail::failure("RequestTimeTooSkewed",
            "The difference between the request time and the current time is too large.");
    }

    auto signed_headers = detail::split(parts.at("SignedHeaders"), ';');
    for (auto& name : signed_headers) name = detail::lowercase(name);
    std::sort(signed_headers.begin(), signed_headers.end());
    std::string canonical_headers, signed_names;
    for (std::size_t i = 0; i < signed_headers.size(); ++i) {
        const auto& name = signed_headers[i];
        const auto header = lower.find(name);
        if (header == lower.end()) {
            return detail::failure("SignatureDoesNotMatch", "Signed header not present.");
        }
        canonical_headers += name + ":" + detail::canonical_header(header->second) + "\n";
        if (i != 0) signed_names += ';';
        signed_names += name;
    }
    const auto payload = lower.find("x-amz-content-sha256");
    const std::string payload_hash = payload == lower.end()
        ? fsp::sha256_hex("") : payload->second;
    const std::string canonical_request = method + "\n" +
        detail::canonical_uri(encoded_path) + "\n" +
        detail::canonical_query(raw_query) + "\n" +
        canonical_headers + "\n" + signed_names + "\n" + payload_hash;
    const std::string scope = credential[1] + "/" + credential[2] + "/s3/aws4_request";
    const std::string string_to_sign = algorithm + "\n" + date->second + "\n" + scope +
        "\n" + fsp::sha256_hex(canonical_request);
    const std::string expected_signature = detail::hex_encode(detail::hmac_sha256(
        detail::signing_key(secret, credential[1], credential[2], credential[3]), string_to_sign));
    if (!detail::signature_equal(expected_signature, parts.at("Signature"))) {
        return detail::failure("SignatureDoesNotMatch", "The request signature does not match.");
    }
    return {true, credential[0], {{}, {}}};
}

} // namespace fsp::sigv4
