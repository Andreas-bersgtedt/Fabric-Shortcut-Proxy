// Iceberg metadata.json builder (single-snapshot path), ported from iceberg/metadata.py.
// Emits the same indent=2 JSON as Python json.dumps. History (F2) stays in Python. C++17.
#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "iceberg_schema.hpp"
#include "iceberg_state.hpp"
#include "md5.hpp"

namespace fsp {

// Deterministic UUID-like string from a seed (md5, not cryptographic).
inline std::string stable_uuid(const std::string& seed) {
    std::string h = md5_hex(seed);
    return h.substr(0, 8) + "-" + h.substr(8, 4) + "-" + h.substr(12, 4) + "-" +
           h.substr(16, 4) + "-" + h.substr(20, 12);
}

// metadata.json bytes for a single snapshot (ICEBERG_SNAPSHOT_HISTORY off path).
inline std::string build_metadata_json(const std::string& bucket, const std::string& table_path,
                                       const std::vector<IcebergColumn>& cols, const SnapshotIdentity& id,
                                       int64_t total_records, int num_splits) {
    const std::string location = "s3://" + bucket + "/" + table_path;
    int last_col = 0;
    for (const auto& c : cols) last_col = std::max(last_col, c.field_id);
    const std::string sid = std::to_string(id.snapshot_id);
    const std::string wm = std::to_string(id.watermark_ms);
    const std::string seq = std::to_string(id.sequence_number);
    const std::string recs = std::to_string(total_records);
    const std::string files = std::to_string(num_splits);

    std::string s = "{\n";
    s += "  \"format-version\": 2,\n";
    s += "  \"table-uuid\": \"" + stable_uuid(location) + "\",\n";
    s += "  \"location\": \"" + location + "\",\n";
    s += "  \"last-sequence-number\": " + seq + ",\n";
    s += "  \"last-updated-ms\": " + wm + ",\n";
    s += "  \"last-column-id\": " + std::to_string(last_col) + ",\n";
    s += "  \"current-schema-id\": 0,\n";
    s += "  \"schemas\": [\n    {\n      \"schema-id\": 0,\n      \"type\": \"struct\",\n      \"fields\": [\n";
    for (size_t i = 0; i < cols.size(); ++i) {
        s += "        {\n";
        s += "          \"id\": " + std::to_string(cols[i].field_id) + ",\n";
        s += "          \"name\": " + json_quote(cols[i].name) + ",\n";
        s += "          \"required\": " + std::string(cols[i].nullable ? "false" : "true") + ",\n";
        s += "          \"type\": " + json_quote(iceberg_type_json(cols[i].iceberg_type)) + "\n";
        s += (i + 1 < cols.size()) ? "        },\n" : "        }\n";
    }
    s += "      ]\n    }\n  ],\n";
    s += "  \"partition-specs\": [\n    {\n      \"spec-id\": 0,\n      \"fields\": []\n    }\n  ],\n";
    s += "  \"default-spec-id\": 0,\n";
    s += "  \"last-partition-id\": 0,\n";
    s += "  \"sort-orders\": [\n    {\n      \"order-id\": 0,\n      \"fields\": []\n    }\n  ],\n";
    s += "  \"default-sort-order-id\": 0,\n";
    s += "  \"snapshots\": [\n    {\n";
    s += "      \"snapshot-id\": " + sid + ",\n";
    s += "      \"sequence-number\": " + seq + ",\n";
    s += "      \"timestamp-ms\": " + wm + ",\n";
    s += "      \"summary\": {\n";
    s += "        \"operation\": \"append\",\n";
    s += "        \"added-data-files\": \"" + files + "\",\n";
    s += "        \"added-records\": \"" + recs + "\",\n";
    s += "        \"total-records\": \"" + recs + "\",\n";
    s += "        \"total-data-files\": \"" + files + "\",\n";
    s += "        \"total-delete-files\": \"0\"\n";
    s += "      },\n";
    s += "      \"manifest-list\": \"s3://" + bucket + "/" + id.manifest_list_key + "\",\n";
    s += "      \"schema-id\": 0\n";
    s += "    }\n  ],\n";
    s += "  \"current-snapshot-id\": " + sid + ",\n";
    s += "  \"snapshot-log\": [\n    {\n      \"timestamp-ms\": " + wm +
         ",\n      \"snapshot-id\": " + sid + "\n    }\n  ],\n";
    s += "  \"metadata-log\": [],\n";
    s += "  \"refs\": {\n    \"main\": {\n      \"type\": \"branch\",\n      \"snapshot-id\": " + sid +
         "\n    }\n  },\n";
    s += "  \"statistics\": [],\n";
    s += "  \"partition-statistics\": []\n";
    s += "}";
    return s;
}

}  // namespace fsp
