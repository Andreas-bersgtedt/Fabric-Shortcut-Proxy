// SQLite row source: read a table (by key ranges) into Arrow tables typed per the
// Iceberg schema, mirroring the Python db/executor.py + planner range strategy.
// Reuses the tier1 dialect query builder and split planner. Also seeds a demo
// table so the native SQL path is self-contained. C++17 + Arrow + sqlite3.
#pragma once

#include <cstdint>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <sqlite3.h>

#include <arrow/api.h>

#include "dialects.hpp"        // fsp::Dialect, SQLiteDialect
#include "iceberg_arrow.hpp"   // normalize_type, arrow_type_for
#include "iceberg_schema.hpp"  // fsp::IcebergColumn
#include "split_planner.hpp"   // days_from_civil, civil_from_days, micros_from_ts_str

namespace fsp {
namespace native {

inline sqlite3* sql_open(const std::string& path) {
    sqlite3* db = nullptr;
    int rc = sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
    if (rc != SQLITE_OK) {
        std::string e = db ? sqlite3_errmsg(db) : "open failed";
        sqlite3_close(db);
        throw std::runtime_error("sqlite open '" + path + "': " + e);
    }
    return db;
}

inline void sql_exec(sqlite3* db, const std::string& sql) {
    char* err = nullptr;
    if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
        std::string e = err ? err : "exec failed";
        sqlite3_free(err);
        throw std::runtime_error("sqlite exec: " + e);
    }
}

inline int64_t sql_scalar_i64(sqlite3* db, const std::string& sql, int64_t def) {
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK)
        throw std::runtime_error(std::string("sqlite prepare: ") + sqlite3_errmsg(db));
    int64_t v = def;
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) != SQLITE_NULL)
        v = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    return v;
}

// Create + populate a demo "sales" table (mirrors demo/seed_db.py). Idempotent.
inline int64_t seed_demo_table(sqlite3* db, const std::string& table, int64_t rows) {
    static const char* products[] = {"Widget A", "Widget B", "Gadget X",
                                     "Gadget Y", "Service Pack", "Support Plan"};
    static const char* regions[] = {"North", "South", "East", "West", "Central"};

    sql_exec(db, "CREATE TABLE IF NOT EXISTS \"" + table + "\" ("
                 "id INTEGER PRIMARY KEY, order_date TEXT, customer_id INTEGER, product TEXT, "
                 "quantity INTEGER, unit_price REAL, total REAL, region TEXT)");
    if (sql_scalar_i64(db, "SELECT COUNT(*) FROM \"" + table + "\"", 0) > 0)
        return sql_scalar_i64(db, "SELECT COUNT(*) FROM \"" + table + "\"", 0);

    sql_exec(db, "BEGIN");
    sqlite3_stmt* st = nullptr;
    const std::string ins = "INSERT INTO \"" + table + "\" (id,order_date,customer_id,product,"
                            "quantity,unit_price,total,region) VALUES (?,?,?,?,?,?,?,?)";
    if (sqlite3_prepare_v2(db, ins.c_str(), -1, &st, nullptr) != SQLITE_OK)
        throw std::runtime_error(std::string("sqlite prepare insert: ") + sqlite3_errmsg(db));

    const int64_t base_days = days_from_civil(2023, 1, 1);
    for (int64_t i = 1; i <= rows; ++i) {
        int64_t y;
        unsigned m, d;
        civil_from_days(base_days + (i % 365), y, m, d);
        char ds[16];
        std::snprintf(ds, sizeof(ds), "%04lld-%02u-%02u", static_cast<long long>(y), m, d);
        const int q = static_cast<int>(1 + (i % 100));
        const double up = static_cast<double>((i * 97) % 99000 + 999) / 100.0;
        sqlite3_bind_int64(st, 1, i);
        sqlite3_bind_text(st, 2, ds, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 3, 1 + (i % 5000));
        sqlite3_bind_text(st, 4, products[i % 6], -1, SQLITE_STATIC);
        sqlite3_bind_int(st, 5, q);
        sqlite3_bind_double(st, 6, up);
        sqlite3_bind_double(st, 7, q * up);
        sqlite3_bind_text(st, 8, regions[i % 5], -1, SQLITE_STATIC);
        if (sqlite3_step(st) != SQLITE_DONE)
            throw std::runtime_error(std::string("sqlite insert: ") + sqlite3_errmsg(db));
        sqlite3_reset(st);
    }
    sqlite3_finalize(st);
    sql_exec(db, "COMMIT");
    return rows;
}

// Inclusive [min, max] of the key column (max < min signals an empty table).
inline std::pair<int64_t, int64_t> sql_key_bounds(sqlite3* db, const Dialect& dialect,
                                                  const std::string& table, const std::string& pk) {
    const std::string src = dialect.quote_qualified(table);
    const std::string col = dialect.quote(pk);
    if (sql_scalar_i64(db, "SELECT COUNT(*) FROM " + src, 0) == 0) return {0, -1};
    int64_t lo = sql_scalar_i64(db, "SELECT MIN(" + col + ") FROM " + src, 0);
    int64_t hi = sql_scalar_i64(db, "SELECT MAX(" + col + ") FROM " + src, -1);
    return {lo, hi};
}

inline void append_cell(arrow::ArrayBuilder* b, const std::string& itype, sqlite3_stmt* st, int col) {
    auto ok = [](const arrow::Status& s) {
        if (!s.ok()) throw std::runtime_error("arrow append: " + s.ToString());
    };
    if (sqlite3_column_type(st, col) == SQLITE_NULL) {
        ok(b->AppendNull());
        return;
    }
    const std::string t = normalize_type(itype);
    if (t == "long") {
        ok(static_cast<arrow::Int64Builder*>(b)->Append(sqlite3_column_int64(st, col)));
    } else if (t == "int") {
        ok(static_cast<arrow::Int32Builder*>(b)->Append(static_cast<int32_t>(sqlite3_column_int(st, col))));
    } else if (t == "double") {
        ok(static_cast<arrow::DoubleBuilder*>(b)->Append(sqlite3_column_double(st, col)));
    } else if (t == "float") {
        ok(static_cast<arrow::FloatBuilder*>(b)->Append(static_cast<float>(sqlite3_column_double(st, col))));
    } else if (t == "boolean") {
        ok(static_cast<arrow::BooleanBuilder*>(b)->Append(sqlite3_column_int(st, col) != 0));
    } else if (t == "date") {
        const unsigned char* s = sqlite3_column_text(st, col);
        int y = 0, m = 0, d = 0;
        if (s) std::sscanf(reinterpret_cast<const char*>(s), "%d-%d-%d", &y, &m, &d);
        auto days = static_cast<int32_t>(days_from_civil(y, static_cast<unsigned>(m), static_cast<unsigned>(d)));
        ok(static_cast<arrow::Date32Builder*>(b)->Append(days));
    } else if (t == "timestamp" || t == "timestamptz") {
        const unsigned char* s = sqlite3_column_text(st, col);
        int64_t micros = s ? micros_from_ts_str(reinterpret_cast<const char*>(s)) : 0;
        ok(static_cast<arrow::TimestampBuilder*>(b)->Append(micros));
    } else if (t == "string") {
        const unsigned char* s = sqlite3_column_text(st, col);
        int n = sqlite3_column_bytes(st, col);
        ok(static_cast<arrow::StringBuilder*>(b)->Append(reinterpret_cast<const char*>(s), n));
    } else if (t == "binary") {
        const void* p = sqlite3_column_blob(st, col);
        int n = sqlite3_column_bytes(st, col);
        ok(static_cast<arrow::BinaryBuilder*>(b)->Append(reinterpret_cast<const uint8_t*>(p), n));
    } else {
        throw std::runtime_error("SQL source: unsupported Iceberg type '" + itype + "'");
    }
}

// Read rows with pk in [lo, hi) into an Arrow table matching `schema`.
inline std::shared_ptr<arrow::Table> sql_read_range(
    sqlite3* db, const Dialect& dialect, const std::vector<IcebergColumn>& cols,
    const std::shared_ptr<arrow::Schema>& schema, const std::string& table, const std::string& pk,
    int64_t lo, int64_t hi, int64_t max_rows) {
    std::string projected;
    for (size_t i = 0; i < cols.size(); ++i) {
        if (i) projected += ",";
        projected += dialect.quote(cols[i].name);
    }
    const std::string sql = dialect.build_select_range(
        projected, dialect.quote_qualified(table), dialect.quote(pk), "key_lo", "key_hi", "max_rows");

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK)
        throw std::runtime_error(std::string("sqlite prepare range: ") + sqlite3_errmsg(db));
    sqlite3_bind_int64(st, sqlite3_bind_parameter_index(st, ":key_lo"), lo);
    sqlite3_bind_int64(st, sqlite3_bind_parameter_index(st, ":key_hi"), hi);
    sqlite3_bind_int64(st, sqlite3_bind_parameter_index(st, ":max_rows"), max_rows);

    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    for (const auto& c : cols) {
        std::unique_ptr<arrow::ArrayBuilder> b;
        auto s = arrow::MakeBuilder(arrow::default_memory_pool(), arrow_type_for(c.iceberg_type), &b);
        if (!s.ok()) throw std::runtime_error("MakeBuilder: " + s.ToString());
        builders.push_back(std::move(b));
    }

    while (sqlite3_step(st) == SQLITE_ROW)
        for (size_t c = 0; c < cols.size(); ++c)
            append_cell(builders[c].get(), cols[c].iceberg_type, st, static_cast<int>(c));
    sqlite3_finalize(st);

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto& b : builders) {
        std::shared_ptr<arrow::Array> a;
        auto s = b->Finish(&a);
        if (!s.ok()) throw std::runtime_error("builder finish: " + s.ToString());
        arrays.push_back(a);
    }
    return arrow::Table::Make(schema, arrays);
}

}  // namespace native
}  // namespace fsp
