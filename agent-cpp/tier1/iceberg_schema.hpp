// Iceberg schema dict (for metadata.json), ported from iceberg/schema.py.
// The Arrow half (pyarrow_schema) stays in Python; this is the pure JSON side. C++17.
#pragma once

#include <cctype>
#include <string>
#include <vector>

#include "json.hpp"

namespace fsp {

struct IcebergColumn {
    int field_id = 0;
    std::string name;
    std::string iceberg_type;
    bool nullable = true;
};

// Iceberg JSON type: every primitive is a plain string; decimal is normalized
// to "decimal(P, S)" (Iceberg spec Appendix C). Non-decimal types pass through
// verbatim, matching iceberg/schema.py `_iceberg_type_json`.
inline std::string iceberg_type_json(const std::string& iceberg_type) {
    std::string t;
    for (char c : iceberg_type) t += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    size_t a = t.find_first_not_of(" ");
    size_t b = t.find_last_not_of(" ");
    t = (a == std::string::npos) ? "" : t.substr(a, b - a + 1);

    if (t.rfind("decimal(", 0) == 0 && !t.empty() && t.back() == ')') {
        std::string inner = t.substr(8, t.size() - 9);
        size_t comma = inner.find(',');
        int prec = std::stoi(inner.substr(0, comma));
        int scale = std::stoi(inner.substr(comma + 1));
        return "decimal(" + std::to_string(prec) + ", " + std::to_string(scale) + ")";
    }
    return iceberg_type;  // primitives: original string
}

// Compact Iceberg v2 schema dict JSON (as embedded, pretty-printed, in metadata.json).
inline std::string iceberg_schema_json(int schema_id, const std::vector<IcebergColumn>& cols) {
    std::string s = "{\"schema-id\":" + std::to_string(schema_id) + ",\"type\":\"struct\",\"fields\":[";
    for (size_t i = 0; i < cols.size(); ++i) {
        if (i) s += ",";
        s += "{\"id\":" + std::to_string(cols[i].field_id) +
             ",\"name\":" + json_quote(cols[i].name) +
             ",\"required\":" + (cols[i].nullable ? "false" : "true") +
             ",\"type\":" + json_quote(iceberg_type_json(cols[i].iceberg_type)) + "}";
    }
    s += "]}";
    return s;
}

}  // namespace fsp
