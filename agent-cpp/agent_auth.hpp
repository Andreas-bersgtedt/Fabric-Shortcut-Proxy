#pragma once

#include <string>

namespace fsp {

inline bool agent_header_value_safe(const std::string& value) {
    if (value.empty()) return false;
    for (unsigned char c : value) {
        if (c < 0x21 || c > 0x7e) return false;
    }
    return true;
}

inline std::string agent_auth_headers(const std::string& token, const std::string& agent_id) {
    std::string headers;
    if (agent_header_value_safe(token)) {
        headers += "X-FSP-Agent-Token: " + token + "\r\n";
    }
    if (agent_header_value_safe(agent_id)) {
        headers += "X-FSP-Agent-ID: " + agent_id + "\r\n";
    }
    return headers;
}

}  // namespace fsp
