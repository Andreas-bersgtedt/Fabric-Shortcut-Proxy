// Snapshot identity + object-key derivation, ported from iceberg/state_store.py.
// Content-addressed (SHA-256), deterministic across restarts. The DB-URL
// connection resolution stays in Python; server/database come in pre-resolved. C++17.
#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include "sha256.hpp"

namespace fsp {

// Keep path segments filesystem/S3-friendly and deterministic (state_store._safe_segment).
inline std::string safe_segment(const std::string& s, const std::string& fallback) {
    size_t a = s.find_first_not_of(" \t\r\n\f\v");
    size_t b = s.find_last_not_of(" \t\r\n\f\v");
    std::string v = (a == std::string::npos) ? "" : s.substr(a, b - a + 1);
    if (v.empty()) v = fallback;
    std::string out;
    bool prev_bad = false;
    for (char c : v) {
        bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                  c == '.' || c == '_' || c == '-';
        if (ok) { out += c; prev_bad = false; }
        else if (!prev_bad) { out += '_'; prev_bad = true; }
    }
    return out.empty() ? fallback : out;
}

// (schema, object) from a possibly dotted source table (rpartition on '.').
inline std::pair<std::string, std::string> split_source_table(const std::string& source_table) {
    size_t dot = source_table.rfind('.');
    if (dot != std::string::npos) {
        std::string schema = source_table.substr(0, dot);
        std::string name = source_table.substr(dot + 1);
        return {schema.empty() ? "default" : schema, name.empty() ? "table" : name};
    }
    return {"default", source_table.empty() ? "table" : source_table};
}

inline std::string legacy_table_path(const std::string& table_name, const std::string& warehouse_prefix) {
    return warehouse_prefix + "/" + safe_segment(table_name, "table");
}

// server/database are already the resolved connection identity (pre-safed in Python).
inline std::string canonical_table_path(const std::string& server, const std::string& database,
                                        const std::string& source_table,
                                        const std::string& warehouse_prefix) {
    auto so = split_source_table(source_table);
    return warehouse_prefix + "/" + safe_segment(server, "local") + "/" +
           safe_segment(database, "default") + "/" + safe_segment(so.first, "default") + "/" +
           safe_segment(so.second, "table");
}

struct SnapshotIdentity {
    uint64_t snapshot_id = 0;
    std::string uuid;
    int64_t watermark_ms = 0;
    int sequence_number = 1;
    std::string manifest_list_key;
    std::string manifest_file_key;
    std::string metadata_key;
    std::string version_hint_key;
};

inline uint64_t hex_to_u64(const std::string& h) { return std::stoull(h, nullptr, 16); }

// Version 1 identity (build_table_snapshot): seed = "{bucket}/{table_path}".
inline SnapshotIdentity build_snapshot_identity(const std::string& bucket, const std::string& table_path) {
    std::string digest = sha256_hex(bucket + "/" + table_path);
    SnapshotIdentity id;
    id.snapshot_id = hex_to_u64(digest.substr(0, 15));
    id.uuid = digest.substr(0, 32);
    id.watermark_ms = 1700000000000LL + static_cast<int64_t>(hex_to_u64(digest.substr(15, 10)) % 86400000ULL);
    id.sequence_number = 1;
    std::string sid = std::to_string(id.snapshot_id);
    id.manifest_list_key = table_path + "/metadata/snap-" + sid + "-1-" + id.uuid + ".avro";
    id.manifest_file_key = table_path + "/metadata/" + id.uuid + "-m0.avro";
    id.metadata_key = table_path + "/metadata/v1.metadata.json";
    id.version_hint_key = table_path + "/metadata/version-hint.text";
    return id;
}

// Version N>=2 identity (advance_table_snapshot): seed = "{bucket}/{table_path}/v{version}".
inline SnapshotIdentity advance_snapshot_identity(const std::string& bucket, const std::string& table_path,
                                                  int version, int64_t prev_watermark_ms, int prev_sequence) {
    std::string digest = sha256_hex(bucket + "/" + table_path + "/v" + std::to_string(version));
    SnapshotIdentity id;
    id.snapshot_id = hex_to_u64(digest.substr(0, 15));
    id.uuid = digest.substr(0, 32);
    id.sequence_number = prev_sequence + 1;
    id.watermark_ms = prev_watermark_ms + 1000LL * version;
    std::string sid = std::to_string(id.snapshot_id);
    std::string seq = std::to_string(id.sequence_number);
    id.manifest_list_key = table_path + "/metadata/snap-" + sid + "-" + seq + "-" + id.uuid + ".avro";
    id.manifest_file_key = table_path + "/metadata/" + id.uuid + "-m" + std::to_string(version) + ".avro";
    id.metadata_key = table_path + "/metadata/v" + std::to_string(version) + ".metadata.json";
    id.version_hint_key = table_path + "/metadata/version-hint.text";
    return id;
}

inline std::string split_object_key(const std::string& table_path, int split_index, uint64_t snapshot_id) {
    return table_path + "/data/split-" + std::to_string(split_index) + "-" +
           std::to_string(snapshot_id) + ".parquet";
}

}  // namespace fsp
