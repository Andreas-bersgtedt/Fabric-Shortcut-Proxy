// Minimal JSON string escaping matched to Python's json (ensure_ascii). C++17.
#pragma once

#include <string>

namespace fsp {

// Append the JSON-escaped form of s (no surrounding quotes). Identifiers are
// assumed ASCII (SQL names), matching how these appear in the Python source.
inline void json_escape_into(std::string& out, const std::string& s) {
    for (char ch : s) {
        unsigned char c = static_cast<unsigned char>(ch);
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            default:
                if (c < 0x20) {
                    static const char* h = "0123456789abcdef";
                    out += "\\u00";
                    out += h[(c >> 4) & 0xf];
                    out += h[c & 0xf];
                } else {
                    out += ch;
                }
        }
    }
}

inline std::string json_quote(const std::string& s) {
    std::string out = "\"";
    json_escape_into(out, s);
    out += "\"";
    return out;
}

}  // namespace fsp
