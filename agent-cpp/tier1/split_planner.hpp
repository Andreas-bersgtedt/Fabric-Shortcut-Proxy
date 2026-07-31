// Split-planning pure logic, ported from planner/split_planner.py.
// The async DB-driven planning (row-count estimation, bound fetching) stays in Python;
// this is the deterministic math and column selection. C++17, no dependencies.
#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace fsp {

struct Column {
    std::string name;
    std::string iceberg_type;
    bool nullable = true;
};

inline bool is_integer_type(const std::string& t) { return t == "int" || t == "long"; }
inline bool is_temporal_type(const std::string& t) {
    return t == "date" || t == "timestamp" || t == "timestamptz";
}

// Split key column: explicit key_column, else first non-nullable int/long, else first column.
inline std::string pk_column(const std::string& key_column, const std::vector<Column>& schema) {
    if (!key_column.empty()) return key_column;
    for (const auto& c : schema)
        if (!c.nullable && is_integer_type(c.iceberg_type)) return c.name;
    return schema.empty() ? std::string() : schema.front().name;
}

// Partition inclusive [lo, hi] into n contiguous half-open [start, end) ranges with no overlap.
inline std::vector<std::pair<int64_t, int64_t>> compute_key_ranges(int64_t lo, int64_t hi, int n) {
    if (n < 1) n = 1;
    std::vector<std::pair<int64_t, int64_t>> out;
    if (hi < lo) {
        out.emplace_back(lo, lo + 1);
        for (int i = 1; i < n; ++i) out.emplace_back(lo + 1, lo + 1);
        return out;
    }
    const int64_t span = hi - lo + 1;
    std::vector<int64_t> bounds(n + 1);
    for (int i = 0; i <= n; ++i) bounds[i] = lo + (span * i) / n;  // non-negative floor division
    bounds[0] = lo;
    bounds[n] = hi + 1;
    for (int i = 0; i < n; ++i) out.emplace_back(bounds[i], bounds[i + 1]);
    return out;
}

// Split count from row-target planning with guardrails.
inline int compute_split_count(std::optional<int64_t> estimated_rows, int64_t target_rows,
                               int min_splits, int max_splits, int default_splits) {
    if (target_rows <= 0 || !estimated_rows || *estimated_rows <= 0)
        return std::max(min_splits, std::min(max_splits, default_splits));
    int64_t proposed = (*estimated_rows + target_rows - 1) / target_rows;  // ceil, both positive
    int p = static_cast<int>(std::min<int64_t>(proposed, max_splits));
    return std::max(min_splits, std::min(max_splits, p));
}

// -- civil date helpers (Howard Hinnant), matched to Python ordinal / epoch semantics ------------

// Days since 1970-01-01 for a proleptic Gregorian date.
inline int64_t days_from_civil(int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int64_t>(doe) - 719468;
}

inline void civil_from_days(int64_t z, int64_t& y, unsigned& m, unsigned& d) {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    y = static_cast<int64_t>(yoe) + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp + (mp < 10 ? 3 : -9);
    y += (m <= 2);
}

constexpr int64_t kOrdinalOfEpoch = 719163;  // date(1970,1,1).toordinal()

inline int64_t ordinal_from_date_str(const std::string& s) {
    int y = 0, m = 0, d = 0;
    std::sscanf(s.c_str(), "%d-%d-%d", &y, &m, &d);
    return days_from_civil(y, static_cast<unsigned>(m), static_cast<unsigned>(d)) + kOrdinalOfEpoch;
}

inline std::string date_str_from_ordinal(int64_t ord) {
    int64_t y;
    unsigned m, d;
    civil_from_days(ord - kOrdinalOfEpoch, y, m, d);
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04lld-%02u-%02u", static_cast<long long>(y), m, d);
    return buf;
}

// Microseconds since the Unix epoch (UTC) for an ISO-8601 string (offset optional, 'Z' allowed).
inline int64_t micros_from_ts_str(const std::string& in) {
    std::string s = in;
    size_t zp = s.find('Z');
    if (zp != std::string::npos) s = s.substr(0, zp) + "+00:00";
    int y = 0, mo = 0, d = 0, hh = 0, mm = 0, ss = 0;
    std::sscanf(s.c_str(), "%d-%d-%dT%d:%d:%d", &y, &mo, &d, &hh, &mm, &ss);

    int64_t frac = 0;
    size_t dot = s.find('.');
    if (dot != std::string::npos) {
        size_t i = dot + 1;
        std::string digits;
        while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) digits += s[i++];
        while (digits.size() < 6) digits += '0';
        frac = std::stoll(digits.substr(0, 6));
    }

    int64_t offset_sec = 0;
    size_t tzpos = s.find_first_of("+-", 10);  // skip the date's dashes
    if (tzpos != std::string::npos) {
        int oh = 0, om = 0;
        std::sscanf(s.c_str() + tzpos + 1, "%d:%d", &oh, &om);
        offset_sec = (oh * 3600 + om * 60) * (s[tzpos] == '-' ? -1 : 1);
    }

    const int64_t days = days_from_civil(y, static_cast<unsigned>(mo), static_cast<unsigned>(d));
    const int64_t sec = days * 86400 + hh * 3600 + mm * 60 + ss - offset_sec;
    return sec * 1000000 + frac;
}

inline std::string ts_str_from_micros(int64_t micros) {
    int64_t sec = micros / 1000000;
    int64_t frac = micros % 1000000;
    if (frac < 0) { frac += 1000000; sec -= 1; }
    int64_t days = sec / 86400;
    int64_t rem = sec % 86400;
    if (rem < 0) { rem += 86400; days -= 1; }
    int64_t y;
    unsigned m, d;
    civil_from_days(days, y, m, d);
    int hh = static_cast<int>(rem / 3600), mm = static_cast<int>((rem % 3600) / 60), ss = static_cast<int>(rem % 60);
    char buf[64];
    if (frac)
        std::snprintf(buf, sizeof(buf), "%04lld-%02u-%02uT%02d:%02d:%02d.%06lld+00:00",
                      static_cast<long long>(y), m, d, hh, mm, ss, static_cast<long long>(frac));
    else
        std::snprintf(buf, sizeof(buf), "%04lld-%02u-%02uT%02d:%02d:%02d+00:00",
                      static_cast<long long>(y), m, d, hh, mm, ss);
    return buf;
}

inline std::vector<std::pair<std::string, std::string>>
compute_temporal_ranges_date(const std::string& lo, const std::string& hi, int n) {
    auto ranges = compute_key_ranges(ordinal_from_date_str(lo), ordinal_from_date_str(hi), n);
    std::vector<std::pair<std::string, std::string>> out;
    for (auto& r : ranges) out.emplace_back(date_str_from_ordinal(r.first), date_str_from_ordinal(r.second));
    return out;
}

inline std::vector<std::pair<std::string, std::string>>
compute_temporal_ranges_ts(const std::string& lo, const std::string& hi, int n) {
    auto ranges = compute_key_ranges(micros_from_ts_str(lo), micros_from_ts_str(hi), n);
    std::vector<std::pair<std::string, std::string>> out;
    for (auto& r : ranges) out.emplace_back(ts_str_from_micros(r.first), ts_str_from_micros(r.second));
    return out;
}

}  // namespace fsp
