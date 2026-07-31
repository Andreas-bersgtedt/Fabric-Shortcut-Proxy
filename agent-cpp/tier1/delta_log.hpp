// Native Delta _delta_log commit emitter, ported from delta/log.py.
// Pure logic (JSON + md5); the state-store/cache glue stays in Python. C++17.
#pragma once

#include <cctype>
#include <cstdint>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include "md5.hpp"

namespace fsp {

struct DeltaColumn {
    std::string name;
    std::string iceberg_type;
    bool nullable = true;
};

struct DeltaSplit {
    std::string object_key;
    int64_t size = 0;
    int64_t records = 0;
};

struct DeltaVersion {
    int version = 0;
    int64_t watermark_ms = 0;
    std::vector<DeltaSplit> splits;
};

// Append the JSON-escaped form of s (no surrounding quotes), matching Python json (ensure_ascii).
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
                    out += ch;  // identifiers assumed ASCII (SQL names)
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

// Iceberg type string -> Delta type string.
inline std::string delta_type(const std::string& iceberg_type) {
    std::string t;
    for (char c : iceberg_type) t += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    // trim surrounding whitespace (Python str.strip())
    size_t a = t.find_first_not_of(" ");
    size_t b = t.find_last_not_of(" ");
    t = (a == std::string::npos) ? "" : t.substr(a, b - a + 1);

    if (t == "boolean") return "boolean";
    if (t == "int") return "integer";
    if (t == "long") return "long";
    if (t == "float") return "float";
    if (t == "double") return "double";
    if (t == "date") return "date";
    if (t == "string") return "string";
    if (t == "binary") return "binary";
    if (t == "uuid") return "string";
    if (t == "time") return "string";
    if (t == "timestamp") return "timestamp_ntz";
    if (t == "timestamptz") return "timestamp";
    if (t.rfind("decimal(", 0) == 0) {  // remove spaces
        std::string r;
        for (char c : t) if (c != ' ') r += c;
        return r;
    }
    if (t.rfind("fixed(", 0) == 0) return "binary";
    return "string";
}

inline std::string delta_schema_string(const std::vector<DeltaColumn>& cols) {
    std::string s = "{\"type\":\"struct\",\"fields\":[";
    for (size_t i = 0; i < cols.size(); ++i) {
        if (i) s += ",";
        s += "{\"name\":" + json_quote(cols[i].name) +
             ",\"type\":" + json_quote(delta_type(cols[i].iceberg_type)) +
             ",\"nullable\":" + (cols[i].nullable ? "true" : "false") + ",\"metadata\":{}}";
    }
    s += "]}";
    return s;
}

inline std::string delta_table_uuid(const std::string& name) {
    std::string h = md5_hex("delta:" + name);
    return h.substr(0, 8) + "-" + h.substr(8, 4) + "-" + h.substr(12, 4) + "-" +
           h.substr(16, 4) + "-" + h.substr(20, 12);
}

inline std::string delta_rel_path(const std::string& object_key, const std::string& table_path) {
    std::string prefix = table_path + "/";
    if (object_key.rfind(prefix, 0) == 0) return object_key.substr(prefix.size());
    return object_key;
}

// Ordered incremental Delta log: commit 0 is protocol + metaData + adds; later commits diff.
class DeltaLog {
public:
    DeltaLog(std::string table_name, std::string table_path, std::vector<DeltaColumn> schema)
        : name_(std::move(table_name)), table_path_(std::move(table_path)), schema_(std::move(schema)) {}

    void register_version(const DeltaVersion& snap) {
        if (committed_version_ >= snap.version) return;
        std::vector<std::tuple<std::string, int64_t, int64_t>> files;
        for (const auto& s : snap.splits)
            files.emplace_back(delta_rel_path(s.object_key, table_path_), s.size, s.records);
        int64_t ts = snap.watermark_ms;

        std::string actions;
        if (commits_.empty()) {
            actions += "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n";
            actions += metadata_action(ts) + "\n";
            for (const auto& f : files)
                actions += add_action(std::get<0>(f), std::get<1>(f), std::get<2>(f), ts) + "\n";
        } else {
            std::set<std::string> cur_paths, prev_paths;
            for (const auto& f : files) cur_paths.insert(std::get<0>(f));
            for (const auto& p : prev_files_) prev_paths.insert(std::get<0>(p));
            std::string body;
            for (const auto& f : files)
                if (!prev_paths.count(std::get<0>(f)))
                    body += add_action(std::get<0>(f), std::get<1>(f), std::get<2>(f), ts) + "\n";
            for (const auto& p : prev_files_)
                if (!cur_paths.count(std::get<0>(p)))
                    body += remove_action(std::get<0>(p), std::get<1>(p), ts) + "\n";
            if (body.empty()) {  // no-op: identical content-addressed file set
                committed_version_ = snap.version;
                return;
            }
            actions = body;
        }
        commits_.push_back(actions);
        prev_files_ = files;
        committed_version_ = snap.version;
    }

    const std::vector<std::string>& commits() const { return commits_; }

private:
    std::string add_action(const std::string& path, int64_t size, int64_t records, int64_t ts) const {
        std::string stats = "{\"numRecords\": " + std::to_string(records) + "}";  // default json spacing
        std::string a = "{\"add\":{\"path\":" + json_quote(path) +
                        ",\"partitionValues\":{},\"size\":" + std::to_string(size) +
                        ",\"modificationTime\":" + std::to_string(ts) +
                        ",\"dataChange\":true,\"stats\":" + json_quote(stats) + "}}";
        return a;
    }

    std::string remove_action(const std::string& path, int64_t size, int64_t ts) const {
        return "{\"remove\":{\"path\":" + json_quote(path) +
               ",\"deletionTimestamp\":" + std::to_string(ts) +
               ",\"dataChange\":true,\"extendedFileMetadata\":true,\"partitionValues\":{},\"size\":" +
               std::to_string(size) + "}}";
    }

    std::string metadata_action(int64_t created_time) const {
        return "{\"metaData\":{\"id\":" + json_quote(delta_table_uuid(name_)) +
               ",\"name\":" + json_quote(name_) +
               ",\"format\":{\"provider\":\"parquet\",\"options\":{}}" +
               ",\"schemaString\":" + json_quote(delta_schema_string(schema_)) +
               ",\"partitionColumns\":[],\"configuration\":{},\"createdTime\":" +
               std::to_string(created_time) + "}}";
    }

    std::string name_;
    std::string table_path_;
    std::vector<DeltaColumn> schema_;
    std::vector<std::string> commits_;
    std::vector<std::tuple<std::string, int64_t, int64_t>> prev_files_;
    int committed_version_ = 0;
};

}  // namespace fsp
