// Size-weighted split assignment (LPT), ported from planner/shard_weight.py.
// Pure, deterministic, coordinator-free. C++17.
#pragma once

#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace fsp {

struct ShardLoad {
    int shard = 0;
    int splits = 0;
    int64_t bytes = 0;
};

inline std::string stable_key(const std::string& table_name, int split_index) {
    return table_name + "#" + std::to_string(split_index);
}

inline double default_weight(const std::vector<double>& known) {
    double sum = 0.0;
    int cnt = 0;
    for (double v : known)
        if (v > 0.0) { sum += v; ++cnt; }
    return cnt ? sum / cnt : 1.0;
}

inline double weight_or(const std::map<std::string, double>& weights, const std::string& k, double dflt) {
    auto it = weights.find(k);
    return it == weights.end() ? dflt : it->second;
}

// Greedy LPT assignment: keys sorted by descending weight (tie-break key ascending),
// each placed on the currently least-loaded shard (tie-break lowest index).
inline std::map<std::string, int> assign_owners(const std::vector<std::string>& keys, int shard_count,
                                                const std::map<std::string, double>& weights) {
    int n = std::max(1, shard_count);
    std::map<std::string, int> assignment;
    if (n == 1) {
        for (const auto& k : keys) assignment[k] = 0;
        return assignment;
    }
    std::vector<std::string> ordered = keys;
    std::sort(ordered.begin(), ordered.end(), [&](const std::string& a, const std::string& b) {
        double wa = weight_or(weights, a, 1.0), wb = weight_or(weights, b, 1.0);
        if (wa != wb) return wa > wb;
        return a < b;
    });
    std::vector<double> loads(n, 0.0);
    for (const auto& k : ordered) {
        int shard = 0;
        for (int i = 1; i < n; ++i)
            if (loads[i] < loads[shard]) shard = i;
        assignment[k] = shard;
        loads[shard] += weight_or(weights, k, 1.0);
    }
    return assignment;
}

inline std::vector<ShardLoad> shard_loads(const std::map<std::string, int>& assignment, int shard_count,
                                          const std::map<std::string, double>& weights) {
    int n = std::max(1, shard_count);
    std::vector<ShardLoad> out(n);
    std::vector<double> acc(n, 0.0);
    for (int i = 0; i < n; ++i) out[i].shard = i;
    for (const auto& kv : assignment) {
        int shard = kv.second;
        if (shard >= 0 && shard < n) {
            out[shard].splits += 1;
            acc[shard] += weight_or(weights, kv.first, 0.0);
        }
    }
    for (int i = 0; i < n; ++i) out[i].bytes = static_cast<int64_t>(acc[i]);
    return out;
}

}  // namespace fsp
