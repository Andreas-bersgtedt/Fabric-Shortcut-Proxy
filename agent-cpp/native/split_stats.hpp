// Read a generated Parquet file's row-group statistics and aggregate per-column
// Iceberg stats, mirroring iceberg/stats.py `collect_split_stats` + `encode_bound`.
// The single-value bound encoding reuses the tier1 encoders (byte-exact with the
// Python `struct`/two's-complement forms). C++17 + Arrow/Parquet.
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/io/memory.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/statistics.h>

#include "iceberg_arrow.hpp"   // fsp::native::normalize_type
#include "iceberg_schema.hpp"  // fsp::IcebergColumn
#include "iceberg_stats.hpp"   // fsp::encode_* (agent-cpp/tier1)

namespace fsp {
namespace native {

struct ColumnStats {
    int field_id = 0;
    int64_t column_size = 0;
    int64_t value_count = 0;
    int64_t null_count = 0;
    std::optional<std::string> lower;
    std::optional<std::string> upper;
};

// Compare the min (want_max=false) or max (want_max=true) of two typed stat
// objects, using the Iceberg type's natural ordering (as Python does on the
// decoded values). Returns <0, 0, >0. Types we do not bound return 0.
inline int stat_cmp(const std::string& itype, const std::shared_ptr<parquet::Statistics>& a,
                    const std::shared_ptr<parquet::Statistics>& b, bool want_max) {
    const std::string t = normalize_type(itype);
    auto cmp = [](auto x, auto y) -> int { return (x < y) ? -1 : ((x > y) ? 1 : 0); };
    if (t == "boolean") {
        auto A = std::static_pointer_cast<parquet::BoolStatistics>(a);
        auto B = std::static_pointer_cast<parquet::BoolStatistics>(b);
        return cmp(want_max ? A->max() : A->min(), want_max ? B->max() : B->min());
    }
    if (t == "int" || t == "date") {
        auto A = std::static_pointer_cast<parquet::Int32Statistics>(a);
        auto B = std::static_pointer_cast<parquet::Int32Statistics>(b);
        return cmp(want_max ? A->max() : A->min(), want_max ? B->max() : B->min());
    }
    if (t == "long" || t == "time" || t == "timestamp" || t == "timestamptz") {
        auto A = std::static_pointer_cast<parquet::Int64Statistics>(a);
        auto B = std::static_pointer_cast<parquet::Int64Statistics>(b);
        return cmp(want_max ? A->max() : A->min(), want_max ? B->max() : B->min());
    }
    if (t == "float") {
        auto A = std::static_pointer_cast<parquet::FloatStatistics>(a);
        auto B = std::static_pointer_cast<parquet::FloatStatistics>(b);
        return cmp(want_max ? A->max() : A->min(), want_max ? B->max() : B->min());
    }
    if (t == "double") {
        auto A = std::static_pointer_cast<parquet::DoubleStatistics>(a);
        auto B = std::static_pointer_cast<parquet::DoubleStatistics>(b);
        return cmp(want_max ? A->max() : A->min(), want_max ? B->max() : B->min());
    }
    if (t == "string" || t == "binary") {
        auto A = std::static_pointer_cast<parquet::ByteArrayStatistics>(a);
        auto B = std::static_pointer_cast<parquet::ByteArrayStatistics>(b);
        parquet::ByteArray av = want_max ? A->max() : A->min();
        parquet::ByteArray bv = want_max ? B->max() : B->min();
        std::string as(reinterpret_cast<const char*>(av.ptr), av.len);
        std::string bs(reinterpret_cast<const char*>(bv.ptr), bv.len);
        return cmp(as, bs);
    }
    if (t == "uuid" || t.rfind("fixed(", 0) == 0) {
        auto A = std::static_pointer_cast<parquet::FLBAStatistics>(a);
        auto B = std::static_pointer_cast<parquet::FLBAStatistics>(b);
        int len = a->descr()->type_length();
        parquet::FLBA av = want_max ? A->max() : A->min();
        parquet::FLBA bv = want_max ? B->max() : B->min();
        std::string as(reinterpret_cast<const char*>(av.ptr), static_cast<size_t>(len));
        std::string bs(reinterpret_cast<const char*>(bv.ptr), static_cast<size_t>(len));
        return cmp(as, bs);
    }
    return 0;  // decimal and anything else: no bound
}

// Encode a typed Parquet stat (min or max) to an Iceberg bound, dispatching on
// the Iceberg type exactly as iceberg/stats.py `encode_bound` does. Returns
// nullopt for types we do not bound (e.g. decimal), matching the "bounds are
// optional" fail-safe.
inline std::optional<std::string> encode_stat(
    const std::string& itype, const std::shared_ptr<parquet::Statistics>& st, bool want_max) {
    const std::string t = normalize_type(itype);
    if (t == "boolean") {
        auto s = std::static_pointer_cast<parquet::BoolStatistics>(st);
        return encode_bool(want_max ? s->max() : s->min());
    }
    if (t == "int") {
        auto s = std::static_pointer_cast<parquet::Int32Statistics>(st);
        return encode_int32(want_max ? s->max() : s->min());
    }
    if (t == "long") {
        auto s = std::static_pointer_cast<parquet::Int64Statistics>(st);
        return encode_long(want_max ? s->max() : s->min());
    }
    if (t == "float") {
        auto s = std::static_pointer_cast<parquet::FloatStatistics>(st);
        return encode_float(want_max ? s->max() : s->min());
    }
    if (t == "double") {
        auto s = std::static_pointer_cast<parquet::DoubleStatistics>(st);
        return encode_double(want_max ? s->max() : s->min());
    }
    if (t == "date") {
        auto s = std::static_pointer_cast<parquet::Int32Statistics>(st);
        return encode_date_days(want_max ? s->max() : s->min());
    }
    if (t == "time" || t == "timestamp" || t == "timestamptz") {
        auto s = std::static_pointer_cast<parquet::Int64Statistics>(st);
        return encode_micros(want_max ? s->max() : s->min());
    }
    if (t == "string") {
        auto s = std::static_pointer_cast<parquet::ByteArrayStatistics>(st);
        parquet::ByteArray v = want_max ? s->max() : s->min();
        return encode_string(std::string(reinterpret_cast<const char*>(v.ptr), v.len));
    }
    if (t == "binary") {
        auto s = std::static_pointer_cast<parquet::ByteArrayStatistics>(st);
        parquet::ByteArray v = want_max ? s->max() : s->min();
        return encode_binary(std::string(reinterpret_cast<const char*>(v.ptr), v.len));
    }
    if (t == "uuid" || t.rfind("fixed(", 0) == 0) {
        auto s = std::static_pointer_cast<parquet::FLBAStatistics>(st);
        parquet::FLBA v = want_max ? s->max() : s->min();
        int len = st->descr()->type_length();
        return std::string(reinterpret_cast<const char*>(v.ptr), static_cast<size_t>(len));
    }
    return std::nullopt;  // decimal and anything else: no bound
}

// Aggregate per-column stats from a Parquet file's metadata (all row groups).
inline std::map<int, ColumnStats> collect_split_stats(
    const std::string& parquet_bytes, const std::vector<IcebergColumn>& columns) {
    auto buffer = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t*>(parquet_bytes.data()),
        static_cast<int64_t>(parquet_bytes.size()));
    auto input = std::make_shared<arrow::io::BufferReader>(buffer);
    std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::Open(input);
    std::shared_ptr<parquet::FileMetaData> md = reader->metadata();

    std::map<std::string, const IcebergColumn*> name_to_col;
    for (const auto& c : columns) name_to_col[c.name] = &c;

    std::map<int, ColumnStats> result;
    const int num_columns = md->num_columns();
    const int num_row_groups = md->num_row_groups();
    const int64_t num_rows = md->num_rows();

    for (int c = 0; c < num_columns; ++c) {
        const std::string name = md->schema()->Column(c)->path()->ToDotString();
        auto it = name_to_col.find(name);
        if (it == name_to_col.end()) continue;
        const IcebergColumn& coldef = *it->second;

        ColumnStats out;
        out.field_id = coldef.field_id;
        out.value_count = num_rows;

        std::shared_ptr<parquet::Statistics> best_min;  // holds winning stat for min
        std::shared_ptr<parquet::Statistics> best_max;  // holds winning stat for max

        for (int rg = 0; rg < num_row_groups; ++rg) {
            auto cc = md->RowGroup(rg)->ColumnChunk(c);
            out.column_size += cc->total_compressed_size();
            if (!cc->is_stats_set()) continue;
            std::shared_ptr<parquet::Statistics> st = cc->statistics();
            out.null_count += st->null_count();
            if (!st->HasMinMax()) continue;
            if (!best_min || stat_cmp(coldef.iceberg_type, st, best_min, /*want_max=*/false) < 0)
                best_min = st;
            if (!best_max || stat_cmp(coldef.iceberg_type, st, best_max, /*want_max=*/true) > 0)
                best_max = st;
        }
        if (best_min) out.lower = encode_stat(coldef.iceberg_type, best_min, /*want_max=*/false);
        if (best_max) out.upper = encode_stat(coldef.iceberg_type, best_max, /*want_max=*/true);
        result[coldef.field_id] = out;
    }
    return result;
}

}  // namespace native
}  // namespace fsp
