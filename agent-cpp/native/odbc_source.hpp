// ODBC row source: read a table (by key ranges) into Arrow tables typed per the
// Iceberg schema, for MSSQL / Oracle (and any ANSI ODBC driver). Mirrors
// sql_source.hpp / pg_source.hpp but over the ODBC API with typed SQLGetData.
// Read-only: seeding is left to the test harness. C++17 + Arrow + ODBC.
//
// NOTE: MSSQL/Oracle are validated in CI (service container + driver), not
// locally. The dialect-specific range SQL is generated here (not via the tier1
// dialects, whose MSSQL range form emits an unsupported LIMIT).
#pragma once

#include <cstdint>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX  // keep windows.h from defining min/max macros
#include <windows.h>  // SQLHWND / LPWSTR used by the Windows ODBC headers
#endif
#include <sql.h>
#include <sqlext.h>

#include <arrow/api.h>

#include "iceberg_arrow.hpp"   // normalize_type, arrow_type_for
#include "iceberg_schema.hpp"  // fsp::IcebergColumn
#include "split_planner.hpp"   // days_from_civil

namespace fsp {
namespace native {

enum class OdbcKind { mssql, oracle, generic };

inline OdbcKind odbc_kind_from(const std::string& k) {
    if (k == "mssql") return OdbcKind::mssql;
    if (k == "oracle") return OdbcKind::oracle;
    return OdbcKind::generic;
}

inline std::string odbc_diag(SQLSMALLINT handle_type, SQLHANDLE handle) {
    SQLCHAR state[6] = {};
    SQLCHAR msg[1024] = {};
    SQLINTEGER native = 0;
    SQLSMALLINT len = 0;
    std::string out;
    for (SQLSMALLINT i = 1; SQLGetDiagRec(handle_type, handle, i, state, &native, msg, sizeof(msg), &len) == SQL_SUCCESS; ++i) {
        if (!out.empty()) out += "; ";
        out += std::string(reinterpret_cast<char*>(state)) + ": " + std::string(reinterpret_cast<char*>(msg));
    }
    return out.empty() ? "unknown ODBC error" : out;
}

// RAII for an ODBC connection (env + dbc).
struct OdbcConn {
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    ~OdbcConn() {
        if (dbc != SQL_NULL_HDBC) {
            SQLDisconnect(dbc);
            SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        }
        if (env != SQL_NULL_HENV) SQLFreeHandle(SQL_HANDLE_ENV, env);
    }
};

inline void odbc_connect(OdbcConn& c, const std::string& conn_str) {
    if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &c.env)))
        throw std::runtime_error("ODBC: alloc env failed");
    SQLSetEnvAttr(c.env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0);
    if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_DBC, c.env, &c.dbc)))
        throw std::runtime_error("ODBC: alloc dbc failed");

    SQLCHAR outstr[1024] = {};
    SQLSMALLINT outlen = 0;
    SQLRETURN r = SQLDriverConnect(
        c.dbc, nullptr, reinterpret_cast<SQLCHAR*>(const_cast<char*>(conn_str.c_str())), SQL_NTS,
        outstr, static_cast<SQLSMALLINT>(sizeof(outstr)), &outlen, SQL_DRIVER_NOPROMPT);
    if (!SQL_SUCCEEDED(r))
        throw std::runtime_error("ODBC connect: " + odbc_diag(SQL_HANDLE_DBC, c.dbc));
}

// Quote an identifier for the target dialect.
inline std::string odbc_quote(OdbcKind kind, const std::string& ident) {
    if (kind == OdbcKind::mssql) {
        std::string s = "[";
        for (char ch : ident) s += (ch == ']') ? "]]" : std::string(1, ch);
        return s + "]";
    }
    std::string s = "\"";  // Oracle / ANSI
    for (char ch : ident) s += (ch == '"') ? "\"\"" : std::string(1, ch);
    return s + "\"";
}

inline std::string odbc_quote_qualified(OdbcKind kind, const std::string& name) {
    std::string out;
    size_t start = 0;
    while (true) {
        size_t dot = name.find('.', start);
        std::string part = name.substr(start, dot == std::string::npos ? std::string::npos : dot - start);
        if (!out.empty()) out += ".";
        out += odbc_quote(kind, part);
        if (dot == std::string::npos) break;
        start = dot + 1;
    }
    return out;
}

inline int64_t odbc_scalar_i64(OdbcConn& c, const std::string& sql, int64_t def) {
    SQLHSTMT st = SQL_NULL_HSTMT;
    if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, c.dbc, &st)))
        throw std::runtime_error("ODBC: alloc stmt failed");
    if (!SQL_SUCCEEDED(SQLExecDirect(st, reinterpret_cast<SQLCHAR*>(const_cast<char*>(sql.c_str())), SQL_NTS))) {
        std::string e = odbc_diag(SQL_HANDLE_STMT, st);
        SQLFreeHandle(SQL_HANDLE_STMT, st);
        throw std::runtime_error("ODBC query: " + e);
    }
    int64_t v = def;
    if (SQLFetch(st) == SQL_SUCCESS) {
        SQLLEN ind = 0;
        SQLBIGINT tmp = 0;
        if (SQL_SUCCEEDED(SQLGetData(st, 1, SQL_C_SBIGINT, &tmp, sizeof(tmp), &ind)) && ind != SQL_NULL_DATA)
            v = static_cast<int64_t>(tmp);
    }
    SQLFreeHandle(SQL_HANDLE_STMT, st);
    return v;
}

inline std::pair<int64_t, int64_t> odbc_key_bounds(OdbcConn& c, OdbcKind kind,
                                                   const std::string& table, const std::string& pk) {
    const std::string src = odbc_quote_qualified(kind, table);
    const std::string col = odbc_quote(kind, pk);
    if (odbc_scalar_i64(c, "SELECT COUNT(*) FROM " + src, 0) == 0) return {0, -1};
    int64_t lo = odbc_scalar_i64(c, "SELECT MIN(" + col + ") FROM " + src, 0);
    int64_t hi = odbc_scalar_i64(c, "SELECT MAX(" + col + ") FROM " + src, -1);
    return {lo, hi};
}

inline void odbc_append_cell(arrow::ArrayBuilder* b, const std::string& itype, SQLHSTMT st, SQLUSMALLINT col) {
    auto ok = [](const arrow::Status& s) {
        if (!s.ok()) throw std::runtime_error("arrow append: " + s.ToString());
    };
    const std::string t = normalize_type(itype);
    SQLLEN ind = 0;
    if (t == "long") {
        SQLBIGINT v = 0;
        SQLGetData(st, col, SQL_C_SBIGINT, &v, sizeof(v), &ind);
        if (ind == SQL_NULL_DATA) { ok(b->AppendNull()); return; }
        ok(static_cast<arrow::Int64Builder*>(b)->Append(static_cast<int64_t>(v)));
    } else if (t == "int") {
        SQLINTEGER v = 0;
        SQLGetData(st, col, SQL_C_SLONG, &v, sizeof(v), &ind);
        if (ind == SQL_NULL_DATA) { ok(b->AppendNull()); return; }
        ok(static_cast<arrow::Int32Builder*>(b)->Append(static_cast<int32_t>(v)));
    } else if (t == "double" || t == "float") {
        double v = 0;
        SQLGetData(st, col, SQL_C_DOUBLE, &v, sizeof(v), &ind);
        if (ind == SQL_NULL_DATA) { ok(b->AppendNull()); return; }
        if (t == "double") ok(static_cast<arrow::DoubleBuilder*>(b)->Append(v));
        else ok(static_cast<arrow::FloatBuilder*>(b)->Append(static_cast<float>(v)));
    } else if (t == "boolean") {
        unsigned char v = 0;
        SQLGetData(st, col, SQL_C_BIT, &v, sizeof(v), &ind);
        if (ind == SQL_NULL_DATA) { ok(b->AppendNull()); return; }
        ok(static_cast<arrow::BooleanBuilder*>(b)->Append(v != 0));
    } else if (t == "date") {
        SQL_DATE_STRUCT v = {};
        SQLGetData(st, col, SQL_C_TYPE_DATE, &v, sizeof(v), &ind);
        if (ind == SQL_NULL_DATA) { ok(b->AppendNull()); return; }
        auto days = static_cast<int32_t>(days_from_civil(v.year, static_cast<unsigned>(v.month), static_cast<unsigned>(v.day)));
        ok(static_cast<arrow::Date32Builder*>(b)->Append(days));
    } else if (t == "string") {
        char buf[4096];
        SQLGetData(st, col, SQL_C_CHAR, buf, sizeof(buf), &ind);
        if (ind == SQL_NULL_DATA) { ok(b->AppendNull()); return; }
        const int n = (ind == SQL_NTS || ind < 0) ? static_cast<int>(std::char_traits<char>::length(buf))
                                                   : static_cast<int>(ind);
        ok(static_cast<arrow::StringBuilder*>(b)->Append(buf, n));
    } else {
        throw std::runtime_error("ODBC source: unsupported Iceberg type '" + itype + "'");
    }
}

// Read rows with pk in [lo, hi) into an Arrow table matching `schema`.
inline std::shared_ptr<arrow::Table> odbc_read_range(
    OdbcConn& c, OdbcKind kind, const std::vector<IcebergColumn>& cols,
    const std::shared_ptr<arrow::Schema>& schema, const std::string& table, const std::string& pk,
    int64_t lo, int64_t hi, int64_t max_rows) {
    std::string projected;
    for (size_t i = 0; i < cols.size(); ++i) {
        if (i) projected += ",";
        projected += odbc_quote(kind, cols[i].name);
    }
    const std::string src = odbc_quote_qualified(kind, table);
    const std::string q = odbc_quote(kind, pk);

    // Positional (?) params differ by dialect's row-limit clause placement.
    std::string sql;
    SQLBIGINT p1 = 0, p2 = 0, p3 = 0;  // bound in appearance order
    if (kind == OdbcKind::mssql) {
        sql = "SELECT TOP (?) " + projected + " FROM " + src + " WHERE " + q + " >= ? AND " + q + " < ? ORDER BY " + q;
        p1 = max_rows; p2 = lo; p3 = hi;
    } else if (kind == OdbcKind::oracle) {
        sql = "SELECT " + projected + " FROM " + src + " WHERE " + q + " >= ? AND " + q + " < ? ORDER BY " + q + " FETCH FIRST ? ROWS ONLY";
        p1 = lo; p2 = hi; p3 = max_rows;
    } else {
        sql = "SELECT " + projected + " FROM " + src + " WHERE " + q + " >= ? AND " + q + " < ? ORDER BY " + q + " LIMIT ?";
        p1 = lo; p2 = hi; p3 = max_rows;
    }

    SQLHSTMT st = SQL_NULL_HSTMT;
    if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, c.dbc, &st)))
        throw std::runtime_error("ODBC: alloc stmt failed");
    SQLBindParameter(st, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &p1, 0, nullptr);
    SQLBindParameter(st, 2, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &p2, 0, nullptr);
    SQLBindParameter(st, 3, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &p3, 0, nullptr);
    if (!SQL_SUCCEEDED(SQLExecDirect(st, reinterpret_cast<SQLCHAR*>(const_cast<char*>(sql.c_str())), SQL_NTS))) {
        std::string e = odbc_diag(SQL_HANDLE_STMT, st);
        SQLFreeHandle(SQL_HANDLE_STMT, st);
        throw std::runtime_error("ODBC range query: " + e);
    }

    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    for (const auto& col : cols) {
        std::unique_ptr<arrow::ArrayBuilder> bld;
        auto s = arrow::MakeBuilder(arrow::default_memory_pool(), arrow_type_for(col.iceberg_type), &bld);
        if (!s.ok()) throw std::runtime_error("MakeBuilder: " + s.ToString());
        builders.push_back(std::move(bld));
    }

    while (SQLFetch(st) == SQL_SUCCESS)
        for (size_t col = 0; col < cols.size(); ++col)
            odbc_append_cell(builders[col].get(), cols[col].iceberg_type, st, static_cast<SQLUSMALLINT>(col + 1));
    SQLFreeHandle(SQL_HANDLE_STMT, st);

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto& bld : builders) {
        std::shared_ptr<arrow::Array> a;
        auto s = bld->Finish(&a);
        if (!s.ok()) throw std::runtime_error("builder finish: " + s.ToString());
        arrays.push_back(a);
    }
    return arrow::Table::Make(schema, arrays);
}

}  // namespace native
}  // namespace fsp
