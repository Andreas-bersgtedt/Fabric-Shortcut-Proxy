// PostgreSQL row source: read a table (by key ranges) into Arrow tables typed
// per the Iceberg schema, mirroring sql_source.hpp for SQLite. Uses libpq with
// $N positional params and text results. Reuses the tier1 PostgresDialect for
// identifier quoting and the split planner's date parsing. C++17 + Arrow + libpq.
#pragma once

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <libpq-fe.h>

#include <arrow/api.h>

#include "dialects.hpp"        // fsp::Dialect, PostgresDialect
#include "iceberg_arrow.hpp"   // normalize_type, arrow_type_for
#include "iceberg_schema.hpp"  // fsp::IcebergColumn
#include "split_planner.hpp"   // days_from_civil, civil_from_days, micros_from_ts_str

namespace fsp {
namespace native {

inline PGconn* pg_open(const std::string& conninfo) {
    PGconn* conn = PQconnectdb(conninfo.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        std::string e = PQerrorMessage(conn);
        PQfinish(conn);
        throw std::runtime_error("postgres connect: " + e);
    }
    return conn;
}

inline void pg_exec(PGconn* conn, const std::string& sql) {
    PGresult* r = PQexec(conn, sql.c_str());
    const ExecStatusType st = PQresultStatus(r);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        std::string e = PQerrorMessage(conn);
        PQclear(r);
        throw std::runtime_error("postgres exec: " + e);
    }
    PQclear(r);
}

inline int64_t pg_scalar_i64(PGconn* conn, const std::string& sql, int64_t def) {
    PGresult* r = PQexec(conn, sql.c_str());
    if (PQresultStatus(r) != PGRES_TUPLES_OK) {
        std::string e = PQerrorMessage(conn);
        PQclear(r);
        throw std::runtime_error("postgres query: " + e);
    }
    int64_t v = def;
    if (PQntuples(r) > 0 && !PQgetisnull(r, 0, 0)) v = std::strtoll(PQgetvalue(r, 0, 0), nullptr, 10);
    PQclear(r);
    return v;
}

// Create + populate a demo "sales" table (mirrors demo/seed_db.py). Idempotent.
inline int64_t seed_demo_table_pg(PGconn* conn, const std::string& table, int64_t rows) {
    static const char* products[] = {"Widget A", "Widget B", "Gadget X",
                                     "Gadget Y", "Service Pack", "Support Plan"};
    static const char* regions[] = {"North", "South", "East", "West", "Central"};

    pg_exec(conn, "CREATE TABLE IF NOT EXISTS \"" + table + "\" ("
                  "id BIGINT PRIMARY KEY, order_date DATE, customer_id BIGINT, product TEXT, "
                  "quantity INTEGER, unit_price DOUBLE PRECISION, total DOUBLE PRECISION, region TEXT)");
    const std::string count_sql = "SELECT COUNT(*) FROM \"" + table + "\"";
    if (pg_scalar_i64(conn, count_sql, 0) > 0) return pg_scalar_i64(conn, count_sql, 0);

    pg_exec(conn, "BEGIN");
    const std::string ins = "INSERT INTO \"" + table + "\" (id,order_date,customer_id,product,"
                            "quantity,unit_price,total,region) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)";
    PGresult* pr = PQprepare(conn, "ins", ins.c_str(), 8, nullptr);
    if (PQresultStatus(pr) != PGRES_COMMAND_OK) {
        std::string e = PQerrorMessage(conn);
        PQclear(pr);
        throw std::runtime_error("postgres prepare insert: " + e);
    }
    PQclear(pr);

    const int64_t base_days = days_from_civil(2023, 1, 1);
    for (int64_t i = 1; i <= rows; ++i) {
        int64_t y;
        unsigned m, d;
        civil_from_days(base_days + (i % 365), y, m, d);
        char ds[16];
        std::snprintf(ds, sizeof(ds), "%04lld-%02u-%02u", static_cast<long long>(y), m, d);
        const int q = static_cast<int>(1 + (i % 100));
        const double up = static_cast<double>((i * 97) % 99000 + 999) / 100.0;
        char sup[32], stot[32];
        std::snprintf(sup, sizeof(sup), "%.17g", up);
        std::snprintf(stot, sizeof(stot), "%.17g", q * up);
        const std::string sid = std::to_string(i);
        const std::string scust = std::to_string(1 + (i % 5000));
        const std::string sq = std::to_string(q);
        const char* vals[8] = {sid.c_str(), ds, scust.c_str(), products[i % 6],
                              sq.c_str(), sup, stot, regions[i % 5]};
        PGresult* r = PQexecPrepared(conn, "ins", 8, vals, nullptr, nullptr, 0);
        if (PQresultStatus(r) != PGRES_COMMAND_OK) {
            std::string e = PQerrorMessage(conn);
            PQclear(r);
            throw std::runtime_error("postgres insert: " + e);
        }
        PQclear(r);
    }
    pg_exec(conn, "COMMIT");
    return rows;
}

// Inclusive [min, max] of the key column (max < min signals an empty table).
inline std::pair<int64_t, int64_t> pg_key_bounds(PGconn* conn, const Dialect& dialect,
                                                 const std::string& table, const std::string& pk) {
    const std::string src = dialect.quote_qualified(table);
    const std::string col = dialect.quote(pk);
    if (pg_scalar_i64(conn, "SELECT COUNT(*) FROM " + src, 0) == 0) return {0, -1};
    int64_t lo = pg_scalar_i64(conn, "SELECT MIN(" + col + ") FROM " + src, 0);
    int64_t hi = pg_scalar_i64(conn, "SELECT MAX(" + col + ") FROM " + src, -1);
    return {lo, hi};
}

inline void pg_append_cell(arrow::ArrayBuilder* b, const std::string& itype,
                           const char* text, bool is_null) {
    auto ok = [](const arrow::Status& s) {
        if (!s.ok()) throw std::runtime_error("arrow append: " + s.ToString());
    };
    if (is_null) {
        ok(b->AppendNull());
        return;
    }
    const std::string t = normalize_type(itype);
    if (t == "long") {
        ok(static_cast<arrow::Int64Builder*>(b)->Append(std::strtoll(text, nullptr, 10)));
    } else if (t == "int") {
        ok(static_cast<arrow::Int32Builder*>(b)->Append(static_cast<int32_t>(std::strtol(text, nullptr, 10))));
    } else if (t == "double") {
        ok(static_cast<arrow::DoubleBuilder*>(b)->Append(std::strtod(text, nullptr)));
    } else if (t == "float") {
        ok(static_cast<arrow::FloatBuilder*>(b)->Append(static_cast<float>(std::strtod(text, nullptr))));
    } else if (t == "boolean") {
        ok(static_cast<arrow::BooleanBuilder*>(b)->Append(text[0] == 't' || text[0] == 'T' || text[0] == '1'));
    } else if (t == "date") {
        int y = 0, m = 0, d = 0;
        std::sscanf(text, "%d-%d-%d", &y, &m, &d);
        ok(static_cast<arrow::Date32Builder*>(b)->Append(
            static_cast<int32_t>(days_from_civil(y, static_cast<unsigned>(m), static_cast<unsigned>(d)))));
    } else if (t == "timestamp" || t == "timestamptz") {
        std::string s = text;  // Postgres uses a space; the parser wants ISO 'T'
        for (char& ch : s)
            if (ch == ' ') ch = 'T';
        ok(static_cast<arrow::TimestampBuilder*>(b)->Append(micros_from_ts_str(s)));
    } else if (t == "string") {
        ok(static_cast<arrow::StringBuilder*>(b)->Append(std::string(text)));
    } else {
        throw std::runtime_error("PG source: unsupported Iceberg type '" + itype + "'");
    }
}

// Read rows with pk in [lo, hi) into an Arrow table matching `schema`.
inline std::shared_ptr<arrow::Table> pg_read_range(
    PGconn* conn, const Dialect& dialect, const std::vector<IcebergColumn>& cols,
    const std::shared_ptr<arrow::Schema>& schema, const std::string& table, const std::string& pk,
    int64_t lo, int64_t hi, int64_t max_rows) {
    std::string projected;
    for (size_t i = 0; i < cols.size(); ++i) {
        if (i) projected += ",";
        projected += dialect.quote(cols[i].name);
    }
    const std::string q = dialect.quote(pk);
    const std::string sql = "SELECT " + projected + " FROM " + dialect.quote_qualified(table) +
                            " WHERE " + q + " >= $1 AND " + q + " < $2 ORDER BY " + q + " LIMIT $3";

    const std::string slo = std::to_string(lo);
    const std::string shi = std::to_string(hi);
    const std::string smax = std::to_string(max_rows);
    const char* vals[3] = {slo.c_str(), shi.c_str(), smax.c_str()};
    PGresult* r = PQexecParams(conn, sql.c_str(), 3, nullptr, vals, nullptr, nullptr, 0);
    if (PQresultStatus(r) != PGRES_TUPLES_OK) {
        std::string e = PQerrorMessage(conn);
        PQclear(r);
        throw std::runtime_error("postgres range query: " + e);
    }

    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    for (const auto& col : cols) {
        std::unique_ptr<arrow::ArrayBuilder> b;
        auto s = arrow::MakeBuilder(arrow::default_memory_pool(), arrow_type_for(col.iceberg_type), &b);
        if (!s.ok()) throw std::runtime_error("MakeBuilder: " + s.ToString());
        builders.push_back(std::move(b));
    }

    const int nrows = PQntuples(r);
    for (int row = 0; row < nrows; ++row)
        for (int col = 0; col < static_cast<int>(cols.size()); ++col)
            pg_append_cell(builders[col].get(), cols[col].iceberg_type,
                           PQgetvalue(r, row, col), PQgetisnull(r, row, col) != 0);
    PQclear(r);

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
