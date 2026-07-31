// Iceberg type -> Arrow type and Arrow schema with PARQUET:field_id metadata.
// Ports iceberg/schema.py `_iceberg_to_pa` and `pyarrow_schema`. The field_id
// metadata is what Iceberg readers (and OneLake Iceberg->Delta) require to map
// Parquet columns to Iceberg fields; without it conversion fails. C++17 + Arrow.
#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "iceberg_schema.hpp"  // fsp::IcebergColumn (agent-cpp/tier1)

namespace fsp {
namespace native {

// Lowercase + trim, matching the Python `.strip().lower()`.
inline std::string normalize_type(const std::string& iceberg_type) {
    std::string t;
    for (char c : iceberg_type) t += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    size_t a = t.find_first_not_of(' ');
    size_t b = t.find_last_not_of(' ');
    return (a == std::string::npos) ? std::string() : t.substr(a, b - a + 1);
}

// Iceberg type string -> Arrow data type, mirroring iceberg/schema.py `_iceberg_to_pa`.
inline std::shared_ptr<arrow::DataType> arrow_type_for(const std::string& iceberg_type) {
    const std::string t = normalize_type(iceberg_type);
    if (t == "boolean") return arrow::boolean();
    if (t == "int") return arrow::int32();
    if (t == "long") return arrow::int64();
    if (t == "float") return arrow::float32();
    if (t == "double") return arrow::float64();
    if (t == "date") return arrow::date32();
    if (t == "time") return arrow::time64(arrow::TimeUnit::MICRO);
    if (t == "timestamp") return arrow::timestamp(arrow::TimeUnit::MICRO);
    if (t == "timestamptz") return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    if (t == "string") return arrow::utf8();
    if (t == "binary") return arrow::binary();
    if (t == "uuid") return arrow::fixed_size_binary(16);
    if (t.rfind("decimal(", 0) == 0 && !t.empty() && t.back() == ')') {
        std::string inner = t.substr(8, t.size() - 9);
        size_t comma = inner.find(',');
        int prec = std::stoi(inner.substr(0, comma));
        int scale = std::stoi(inner.substr(comma + 1));
        return arrow::decimal128(prec, scale);
    }
    if (t.rfind("fixed(", 0) == 0 && !t.empty() && t.back() == ')') {
        int length = std::stoi(t.substr(6, t.size() - 7));
        return arrow::fixed_size_binary(length);
    }
    throw std::invalid_argument("Unsupported Iceberg type: " + iceberg_type);
}

// Arrow schema with a PARQUET:field_id metadata entry per field, mirroring
// iceberg/schema.py `pyarrow_schema`.
inline std::shared_ptr<arrow::Schema> build_arrow_schema(const std::vector<IcebergColumn>& cols) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(cols.size());
    for (const auto& col : cols) {
        auto meta = arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(col.field_id)});
        fields.push_back(arrow::field(col.name, arrow_type_for(col.iceberg_type), col.nullable, meta));
    }
    return arrow::schema(fields);
}

}  // namespace native
}  // namespace fsp
