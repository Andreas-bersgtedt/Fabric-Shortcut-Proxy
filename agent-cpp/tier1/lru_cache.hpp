// TTL + size-bounded LRU byte cache with pinning, ported from cache/lru_cache.py.
// In-memory core only; the disk / artifact-store tiers stay in Python. C++17.
#pragma once

#include <cstddef>
#include <functional>
#include <list>
#include <string>
#include <unordered_map>

namespace fsp {

inline double steady_seconds();

// Thread-unsafe in itself (mirror the Python single-lock model at a higher layer).
class BytesLruCache {
public:
    using Clock = std::function<double()>;

    BytesLruCache(size_t max_bytes, double ttl_seconds, Clock clock = steady_seconds)
        : max_bytes_(max_bytes), ttl_(ttl_seconds), clock_(std::move(clock)) {}

    // Returns a pointer to the cached bytes (nullptr on miss/expiry). Touches LRU recency.
    const std::string* get(const std::string& key) {
        auto it = index_.find(key);
        if (it == index_.end()) return nullptr;
        if (clock_() > it->second->expiry) {  // expired
            evict(key);
            return nullptr;
        }
        order_.splice(order_.end(), order_, it->second);  // move to newest
        return &it->second->data;
    }

    void put(const std::string& key, std::string data) {
        if (data.size() > max_bytes_) return;  // too large to cache
        auto existing = index_.find(key);
        if (existing != index_.end()) evict(key);
        while (current_bytes_ + data.size() > max_bytes_ && !order_.empty())
            evict(order_.front().key);
        double expiry = clock_() + ttl_;
        order_.push_back(Node{key, std::move(data), expiry});
        auto last = std::prev(order_.end());
        current_bytes_ += last->data.size();
        index_[key] = last;
    }

    void evict(const std::string& key) {
        auto it = index_.find(key);
        if (it == index_.end()) return;
        current_bytes_ -= it->second->data.size();
        order_.erase(it->second);
        index_.erase(it);
    }

    size_t entries() const { return index_.size(); }
    size_t current_bytes() const { return current_bytes_; }
    size_t max_bytes() const { return max_bytes_; }

private:
    struct Node {
        std::string key;
        std::string data;
        double expiry;
    };
    std::list<Node> order_;  // front = oldest, back = newest
    std::unordered_map<std::string, std::list<Node>::iterator> index_;
    size_t max_bytes_;
    size_t current_bytes_ = 0;
    double ttl_;
    Clock clock_;
};

// Pinned authoritative splits: served verbatim, never expired or evicted.
class PinnedStore {
public:
    void pin(const std::string& key, std::string data) { pinned_[key] = std::move(data); }
    const std::string* peek(const std::string& key) const {
        auto it = pinned_.find(key);
        return it == pinned_.end() ? nullptr : &it->second;
    }
    void unpin(const std::string& key) { pinned_.erase(key); }
    void clear() { pinned_.clear(); }
    size_t entries() const { return pinned_.size(); }
    size_t bytes() const {
        size_t total = 0;
        for (const auto& kv : pinned_) total += kv.second.size();
        return total;
    }

private:
    std::unordered_map<std::string, std::string> pinned_;
};

}  // namespace fsp

#include <chrono>
namespace fsp {
inline double steady_seconds() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
}  // namespace fsp
