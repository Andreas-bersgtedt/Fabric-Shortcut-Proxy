// Iceberg manifest list (snap-*.avro) and manifest file (*-m0.avro) writers,
// ported from iceberg/manifest.py. Uses avro-cpp GenericDatum with the same
// embedded schemas (field-id custom attributes preserved, verified via the avro
// probe). Not byte-identical to fastavro output, but validated by round-tripping
// through fastavro/pyiceberg. C++17 + avro-cpp.
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <fmt/format.h>  // avro/Exception.hh uses fmt::format but only pulls fmt/base.h

#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Stream.hh>
#include <avro/ValidSchema.hh>

#include "split_stats.hpp"  // fsp::native::ColumnStats

namespace fsp {
namespace native {

// Placeholders match iceberg/manifest.py when a split's counts are unknown.
constexpr int64_t kPlaceholderRecordCount = 100000;
constexpr int64_t kPlaceholderFileSize = 10 * 1024 * 1024;

struct ManifestSplit {
    std::string object_key;
    std::optional<int64_t> record_count;
    std::optional<int64_t> file_size_in_bytes;
    std::map<int, ColumnStats> stats;  // empty => no per-column stats
};

struct ManifestSnapshot {
    int64_t snapshot_id = 0;
    int64_t sequence_number = 0;
    std::string bucket_name;
    std::string manifest_file_key;
    std::vector<ManifestSplit> splits;
    bool with_stats = false;  // config.ICEBERG_MANIFEST_STATS
};

// --- Embedded Avro schemas (mirror iceberg/manifest.py) ---------------------

inline const char* manifest_list_schema_json() {
    return R"({
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path",        "type": "string", "field-id": 500},
    {"name": "manifest_length",      "type": "long",   "field-id": 501},
    {"name": "partition_spec_id",    "type": "int",    "field-id": 502},
    {"name": "content",              "type": "int",    "field-id": 517, "default": 0},
    {"name": "sequence_number",      "type": "long",   "field-id": 515, "default": 0},
    {"name": "min_sequence_number",  "type": "long",   "field-id": 516, "default": 0},
    {"name": "added_snapshot_id",    "type": "long",   "field-id": 503},
    {"name": "added_files_count",    "type": "int",    "field-id": 504, "default": 0},
    {"name": "existing_files_count", "type": "int",    "field-id": 505, "default": 0},
    {"name": "deleted_files_count",  "type": "int",    "field-id": 506, "default": 0},
    {"name": "added_rows_count",     "type": "long",   "field-id": 512, "default": 0},
    {"name": "existing_rows_count",  "type": "long",   "field-id": 513, "default": 0},
    {"name": "deleted_rows_count",   "type": "long",   "field-id": 514, "default": 0},
    {"name": "partitions",
     "type": {"type": "array",
              "items": {"type": "record", "name": "r508",
                        "fields": [
                          {"name": "contains_null", "type": "boolean", "field-id": 509},
                          {"name": "contains_nan",  "type": ["null", "boolean"], "field-id": 518, "default": null},
                          {"name": "lower_bound",   "type": ["null", "bytes"],   "field-id": 510, "default": null},
                          {"name": "upper_bound",   "type": ["null", "bytes"],   "field-id": 511, "default": null}
                        ]},
              "element-id": 508},
     "field-id": 507, "default": []}
  ]
})";
}

// A map<int, value_type> encoded as an Avro array of key/value records, matching
// iceberg/manifest.py `_map_field`.
inline std::string map_field_json(const std::string& name, int field_id, int key_id,
                                  int value_id, const std::string& value_type) {
    return "{\"name\": \"" + name + "\", \"type\": [\"null\", {\"type\": \"array\", \"items\": "
           "{\"type\": \"record\", \"name\": \"k" + std::to_string(key_id) + "_v" +
           std::to_string(value_id) + "\", \"fields\": ["
           "{\"name\": \"key\", \"type\": \"int\", \"field-id\": " + std::to_string(key_id) + "}, "
           "{\"name\": \"value\", \"type\": \"" + value_type + "\", \"field-id\": " +
           std::to_string(value_id) + "}]}, \"logicalType\": \"map\"}], "
           "\"field-id\": " + std::to_string(field_id) + ", \"default\": null}";
}

inline std::string manifest_entry_schema_json(bool with_stats) {
    std::string data_file_fields =
        "{\"name\": \"content\",            \"type\": \"int\",    \"field-id\": 134, \"default\": 0}, "
        "{\"name\": \"file_path\",          \"type\": \"string\", \"field-id\": 100}, "
        "{\"name\": \"file_format\",        \"type\": \"string\", \"field-id\": 101}, "
        "{\"name\": \"partition\", \"type\": {\"type\": \"record\", \"name\": \"r102\", \"fields\": []}, \"field-id\": 102}, "
        "{\"name\": \"record_count\",       \"type\": \"long\",   \"field-id\": 103}, "
        "{\"name\": \"file_size_in_bytes\", \"type\": \"long\",   \"field-id\": 104}";
    if (with_stats) {
        data_file_fields += ", " + map_field_json("column_sizes", 108, 117, 118, "long");
        data_file_fields += ", " + map_field_json("value_counts", 109, 119, 120, "long");
        data_file_fields += ", " + map_field_json("null_value_counts", 110, 121, 122, "long");
        data_file_fields += ", " + map_field_json("nan_value_counts", 137, 138, 139, "long");
        data_file_fields += ", " + map_field_json("lower_bounds", 125, 126, 127, "bytes");
        data_file_fields += ", " + map_field_json("upper_bounds", 128, 129, 130, "bytes");
    }
    data_file_fields +=
        ", {\"name\": \"key_metadata\",  \"type\": [\"null\", \"bytes\"], \"field-id\": 131, \"default\": null}, "
        "{\"name\": \"split_offsets\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"long\", \"element-id\": 133}], \"field-id\": 132, \"default\": null}, "
        "{\"name\": \"equality_ids\",  \"type\": [\"null\", {\"type\": \"array\", \"items\": \"int\",  \"element-id\": 136}], \"field-id\": 135, \"default\": null}, "
        "{\"name\": \"sort_order_id\", \"type\": [\"null\", \"int\"], \"field-id\": 140, \"default\": null}";

    return std::string("{\"type\": \"record\", \"name\": \"manifest_entry\", \"fields\": [") +
        "{\"name\": \"status\",               \"type\": \"int\",           \"field-id\": 0}, "
        "{\"name\": \"snapshot_id\",          \"type\": [\"null\", \"long\"], \"field-id\": 1, \"default\": null}, "
        "{\"name\": \"sequence_number\",      \"type\": [\"null\", \"long\"], \"field-id\": 3, \"default\": null}, "
        "{\"name\": \"file_sequence_number\", \"type\": [\"null\", \"long\"], \"field-id\": 4, \"default\": null}, "
        "{\"name\": \"data_file\", \"type\": {\"type\": \"record\", \"name\": \"r2\", \"fields\": [" +
        data_file_fields + "]}, \"field-id\": 2}]}";
}

// --- Helpers ----------------------------------------------------------------

inline std::string s3_url(const std::string& bucket, const std::string& object_key) {
    return "s3://" + bucket + "/" + object_key;
}

inline std::vector<uint8_t> to_bytes(const std::string& s) {
    return std::vector<uint8_t>(s.begin(), s.end());
}

// An avro OutputStream that appends into an externally-owned std::string. The
// buffer lives outside the stream, so the bytes survive DataFileWriter::close()
// releasing (and destroying) its stream. avro's own memoryOutputStream would be
// destroyed by close(), leaving no way to read the result back.
class StringOutputStream : public avro::OutputStream {
public:
    explicit StringOutputStream(std::string& sink, size_t chunkSize = 4096)
        : sink_(sink), chunk_(chunkSize), pos_(0) {}
    ~StringOutputStream() override { commit(); }

    bool next(uint8_t** data, size_t* len) override {
        if (pos_ == chunk_.size()) commit();
        *data = chunk_.data() + pos_;
        *len = chunk_.size() - pos_;
        pos_ = chunk_.size();
        return true;
    }
    void backup(size_t len) override { pos_ -= len; }
    uint64_t byteCount() const override { return sink_.size() + pos_; }
    void flush() override { commit(); }

private:
    void commit() {
        if (pos_) {
            sink_.append(reinterpret_cast<const char*>(chunk_.data()), pos_);
            pos_ = 0;
        }
    }
    std::string& sink_;
    std::vector<uint8_t> chunk_;
    size_t pos_;
};

// Serialize records into an in-memory Avro Object Container File.
inline std::string avro_bytes(const avro::ValidSchema& schema,
                              const std::vector<avro::GenericDatum>& records) {
    std::string sink;
    {
        std::unique_ptr<avro::OutputStream> out(new StringOutputStream(sink));
        avro::DataFileWriter<avro::GenericDatum> writer(std::move(out), schema);
        for (const auto& r : records) writer.write(r);
        writer.flush();
        writer.close();
    }  // writer + StringOutputStream torn down; the destructor commits any remainder.
    return sink;
}

// Set a [null, T] union field to its null (branch 0) or value (branch 1).
inline avro::GenericDatum& union_value(avro::GenericRecord& rec, const std::string& field) {
    avro::GenericDatum& f = rec.field(field);
    f.selectBranch(1);
    return f;
}
inline void union_null(avro::GenericRecord& rec, const std::string& field) {
    rec.field(field).selectBranch(0);
}

// Append the six Iceberg stat maps to a data_file record (F3), mirroring
// iceberg/manifest.py `_stat_maps`.
inline void set_stat_map_long(avro::GenericRecord& df, const std::string& name,
                              const std::map<int, ColumnStats>& stats,
                              int64_t (*pick)(const ColumnStats&)) {
    avro::GenericDatum& f = union_value(df, name);
    avro::GenericArray& arr = f.value<avro::GenericArray>();
    avro::NodePtr item = arr.schema()->leafAt(0);
    for (const auto& kv : stats) {
        avro::GenericDatum d(item);
        avro::GenericRecord& r = d.value<avro::GenericRecord>();
        r.field("key").value<int32_t>() = kv.first;
        r.field("value").value<int64_t>() = pick(kv.second);
        arr.value().push_back(d);
    }
}

inline void set_stat_map_bytes(avro::GenericRecord& df, const std::string& name,
                               const std::map<int, ColumnStats>& stats, bool upper) {
    avro::GenericDatum& f = union_value(df, name);
    avro::GenericArray& arr = f.value<avro::GenericArray>();
    avro::NodePtr item = arr.schema()->leafAt(0);
    for (const auto& kv : stats) {
        const std::optional<std::string>& b = upper ? kv.second.upper : kv.second.lower;
        if (!b.has_value()) continue;  // omit absent bounds (matches Python)
        avro::GenericDatum d(item);
        avro::GenericRecord& r = d.value<avro::GenericRecord>();
        r.field("key").value<int32_t>() = kv.first;
        r.field("value").value<std::vector<uint8_t>>() = to_bytes(*b);
        arr.value().push_back(d);
    }
}

// --- Public builders --------------------------------------------------------

inline std::string build_manifest_file(const ManifestSnapshot& snap) {
    avro::ValidSchema schema =
        avro::compileJsonSchemaFromString(manifest_entry_schema_json(snap.with_stats));

    std::vector<avro::GenericDatum> entries;
    entries.reserve(snap.splits.size());
    for (const auto& split : snap.splits) {
        const int64_t record_count = split.record_count.value_or(kPlaceholderRecordCount);
        const int64_t file_size = split.file_size_in_bytes.value_or(kPlaceholderFileSize);

        avro::GenericDatum entry(schema);
        avro::GenericRecord& rec = entry.value<avro::GenericRecord>();
        rec.field("status").value<int32_t>() = 1;  // ADDED
        union_value(rec, "snapshot_id").value<int64_t>() = snap.snapshot_id;
        union_value(rec, "sequence_number").value<int64_t>() = snap.sequence_number;
        union_value(rec, "file_sequence_number").value<int64_t>() = snap.sequence_number;

        avro::GenericRecord& df = rec.field("data_file").value<avro::GenericRecord>();
        df.field("content").value<int32_t>() = 0;  // DATA
        df.field("file_path").value<std::string>() = s3_url(snap.bucket_name, split.object_key);
        df.field("file_format").value<std::string>() = "PARQUET";
        // partition (r102) has no fields.
        df.field("record_count").value<int64_t>() = record_count;
        df.field("file_size_in_bytes").value<int64_t>() = file_size;

        if (snap.with_stats) {
            set_stat_map_long(df, "column_sizes", split.stats,
                              [](const ColumnStats& s) { return s.column_size; });
            set_stat_map_long(df, "value_counts", split.stats,
                              [](const ColumnStats& s) { return s.value_count; });
            set_stat_map_long(df, "null_value_counts", split.stats,
                              [](const ColumnStats& s) { return s.null_count; });
            union_value(df, "nan_value_counts");  // empty array (present, no entries)
            set_stat_map_bytes(df, "lower_bounds", split.stats, /*upper=*/false);
            set_stat_map_bytes(df, "upper_bounds", split.stats, /*upper=*/true);
        }

        union_null(df, "key_metadata");
        union_null(df, "split_offsets");
        union_null(df, "equality_ids");
        union_null(df, "sort_order_id");

        entries.push_back(entry);
    }
    return avro_bytes(schema, entries);
}

inline std::string build_manifest_list(const ManifestSnapshot& snap, int64_t manifest_length) {
    avro::ValidSchema schema = avro::compileJsonSchemaFromString(manifest_list_schema_json());

    int64_t total_rows = 0;
    for (const auto& s : snap.splits)
        total_rows += s.record_count.value_or(kPlaceholderRecordCount);

    avro::GenericDatum datum(schema);
    avro::GenericRecord& rec = datum.value<avro::GenericRecord>();
    rec.field("manifest_path").value<std::string>() = s3_url(snap.bucket_name, snap.manifest_file_key);
    rec.field("manifest_length").value<int64_t>() = manifest_length;
    rec.field("partition_spec_id").value<int32_t>() = 0;
    rec.field("content").value<int32_t>() = 0;  // DATA
    rec.field("sequence_number").value<int64_t>() = snap.sequence_number;
    rec.field("min_sequence_number").value<int64_t>() = snap.sequence_number;
    rec.field("added_snapshot_id").value<int64_t>() = snap.snapshot_id;
    rec.field("added_files_count").value<int32_t>() = static_cast<int32_t>(snap.splits.size());
    rec.field("existing_files_count").value<int32_t>() = 0;
    rec.field("deleted_files_count").value<int32_t>() = 0;
    rec.field("added_rows_count").value<int64_t>() = total_rows;
    rec.field("existing_rows_count").value<int64_t>() = 0;
    rec.field("deleted_rows_count").value<int64_t>() = 0;
    // partitions: empty array (default []).

    return avro_bytes(schema, {datum});
}

}  // namespace native
}  // namespace fsp
