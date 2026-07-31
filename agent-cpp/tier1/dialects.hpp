// SQL dialect adapters for split-query generation (F6), ported from planner/dialects.py.
// Pure logic, no third-party dependencies. C++17.
#pragma once

#include <cctype>
#include <string>
#include <vector>

namespace fsp {

inline std::string replace_all(std::string s, const std::string& from, const std::string& to) {
    if (from.empty()) return s;
    std::string out;
    out.reserve(s.size());
    size_t pos = 0, prev = 0;
    while ((pos = s.find(from, prev)) != std::string::npos) {
        out.append(s, prev, pos - prev);
        out += to;
        prev = pos + from.size();
    }
    out.append(s, prev, std::string::npos);
    return out;
}

inline std::vector<std::string> split_str(const std::string& s, char delim) {
    std::vector<std::string> parts;
    size_t start = 0, pos;
    while ((pos = s.find(delim, start)) != std::string::npos) {
        parts.push_back(s.substr(start, pos - start));
        start = pos + 1;
    }
    parts.push_back(s.substr(start));
    return parts;
}

inline std::string lower(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

// Generic ANSI dialect: double-quoted identifiers, LIMIT suffix, INTEGER cast.
class Dialect {
public:
    virtual ~Dialect() = default;

    virtual std::string name() const { return "generic"; }
    virtual std::string int_cast_type() const { return "INTEGER"; }
    virtual std::string quote_open() const { return "\""; }
    virtual std::string quote_close() const { return "\""; }

    virtual std::string quote(const std::string& ident) const {
        const std::string qc = quote_close();
        return quote_open() + replace_all(ident, qc, qc + qc) + qc;
    }

    std::string quote_qualified(const std::string& name) const {
        std::string out;
        auto parts = split_str(name, '.');
        for (size_t i = 0; i < parts.size(); ++i) {
            if (i) out += ".";
            out += quote(parts[i]);
        }
        return out;
    }

    std::string cast_int(const std::string& expr) const {
        return "CAST(" + expr + " AS " + int_cast_type() + ")";
    }

    virtual std::string build_select(const std::string& projected, const std::string& source,
                                     const std::string& pk, const std::string& num_splits_param,
                                     const std::string& split_index_param,
                                     const std::string& max_rows_param) const {
        std::string predicate = "(" + cast_int(pk) + " % :" + num_splits_param + ") = :" + split_index_param;
        return "SELECT " + projected + " FROM " + source + " WHERE " + predicate +
               " ORDER BY " + pk + " LIMIT :" + max_rows_param;
    }

    virtual std::string build_select_range(const std::string& projected, const std::string& source,
                                           const std::string& pk, const std::string& key_lo_param,
                                           const std::string& key_hi_param,
                                           const std::string& max_rows_param) const {
        std::string predicate = pk + " >= :" + key_lo_param + " AND " + pk + " < :" + key_hi_param;
        return "SELECT " + projected + " FROM " + source + " WHERE " + predicate +
               " ORDER BY " + pk + " LIMIT :" + max_rows_param;
    }

    virtual std::string build_select_row_number(const std::string& projected, const std::string& source,
                                                const std::string& order_by,
                                                const std::string& num_splits_param,
                                                const std::string& split_index_param,
                                                const std::string& max_rows_param) const {
        std::string rownum = quote("__row_num");
        std::string inner = "SELECT " + projected + ", ROW_NUMBER() OVER (ORDER BY " + order_by +
                            ") AS " + rownum + " FROM " + source;
        std::string predicate = "((" + rownum + " - 1) % :" + num_splits_param + ") = :" + split_index_param;
        return "SELECT " + projected + " FROM (" + inner + ") AS q WHERE " + predicate +
               " ORDER BY " + rownum + " LIMIT :" + max_rows_param;
    }
};

class SQLiteDialect : public Dialect {
public:
    std::string name() const override { return "sqlite"; }
};

class PostgresDialect : public Dialect {
public:
    std::string name() const override { return "postgresql"; }
    std::string int_cast_type() const override { return "BIGINT"; }
};

class MSSQLDialect : public Dialect {
public:
    std::string name() const override { return "mssql"; }
    std::string int_cast_type() const override { return "BIGINT"; }
    std::string quote_open() const override { return "["; }
    std::string quote_close() const override { return "]"; }

    std::string quote(const std::string& ident) const override {
        return "[" + replace_all(ident, "]", "]]") + "]";
    }

    std::string build_select(const std::string& projected, const std::string& source,
                             const std::string& pk, const std::string& num_splits_param,
                             const std::string& split_index_param,
                             const std::string& max_rows_param) const override {
        std::string predicate = "(" + cast_int(pk) + " % :" + num_splits_param + ") = :" + split_index_param;
        return "SELECT TOP (:" + max_rows_param + ") " + projected + " FROM " + source +
               " WHERE " + predicate + " ORDER BY " + pk;
    }

    std::string build_select_row_number(const std::string& projected, const std::string& source,
                                        const std::string& order_by, const std::string& num_splits_param,
                                        const std::string& split_index_param,
                                        const std::string& max_rows_param) const override {
        std::string rownum = quote("__row_num");
        std::string inner = "SELECT " + projected + ", ROW_NUMBER() OVER (ORDER BY " + order_by +
                            ") AS " + rownum + " FROM " + source;
        std::string predicate = "((q." + rownum + " - 1) % :" + num_splits_param + ") = :" + split_index_param;
        return "SELECT TOP (:" + max_rows_param + ") " + projected + " FROM (" + inner + ") AS q WHERE " +
               predicate + " ORDER BY q." + rownum;
    }
};

class OracleDialect : public Dialect {
public:
    std::string name() const override { return "oracle"; }
    std::string int_cast_type() const override { return "NUMBER(19)"; }

    std::string build_select(const std::string& projected, const std::string& source,
                             const std::string& pk, const std::string& num_splits_param,
                             const std::string& split_index_param,
                             const std::string& max_rows_param) const override {
        std::string predicate = "(MOD(" + cast_int(pk) + ", :" + num_splits_param + ")) = :" + split_index_param;
        return "SELECT " + projected + " FROM " + source + " WHERE " + predicate +
               " ORDER BY " + pk + " FETCH FIRST :" + max_rows_param + " ROWS ONLY";
    }

    std::string build_select_range(const std::string& projected, const std::string& source,
                                   const std::string& pk, const std::string& key_lo_param,
                                   const std::string& key_hi_param,
                                   const std::string& max_rows_param) const override {
        std::string predicate = pk + " >= :" + key_lo_param + " AND " + pk + " < :" + key_hi_param;
        return "SELECT " + projected + " FROM " + source + " WHERE " + predicate +
               " ORDER BY " + pk + " FETCH FIRST :" + max_rows_param + " ROWS ONLY";
    }

    std::string build_select_row_number(const std::string& projected, const std::string& source,
                                        const std::string& order_by, const std::string& num_splits_param,
                                        const std::string& split_index_param,
                                        const std::string& max_rows_param) const override {
        std::string rownum = quote("__row_num");
        std::string inner = "SELECT " + projected + ", ROW_NUMBER() OVER (ORDER BY " + order_by +
                            ") AS " + rownum + " FROM " + source;
        std::string predicate = "(MOD((q." + rownum + " - 1), :" + num_splits_param + ")) = :" + split_index_param;
        return "SELECT " + projected + " FROM (" + inner + ") q WHERE " + predicate +
               " ORDER BY q." + rownum + " FETCH FIRST :" + max_rows_param + " ROWS ONLY";
    }
};

class DatabricksDialect : public Dialect {
public:
    std::string name() const override { return "databricks"; }
    std::string int_cast_type() const override { return "BIGINT"; }
    std::string quote_open() const override { return "`"; }
    std::string quote_close() const override { return "`"; }

    std::string build_select_range(const std::string& projected, const std::string& source,
                                   const std::string& pk, const std::string& key_lo_param,
                                   const std::string& key_hi_param,
                                   const std::string& max_rows_param) const override {
        std::string predicate = pk + " >= :" + key_lo_param + " AND " + pk + " < :" + key_hi_param;
        return "SELECT TOP (:" + max_rows_param + ") " + projected + " FROM " + source +
               " WHERE " + predicate + " ORDER BY " + pk;
    }
};

// Returns a reference to a process-wide dialect singleton for a SQLAlchemy DB URL.
inline const Dialect& get_dialect(const std::string& db_url) {
    static const SQLiteDialect sqlite;
    static const PostgresDialect postgres;
    static const MSSQLDialect mssql;
    static const OracleDialect oracle;
    static const DatabricksDialect databricks;
    static const Dialect generic;

    std::string scheme = lower(db_url);
    auto pos = scheme.find("://");
    if (pos != std::string::npos) scheme = scheme.substr(0, pos);

    if (scheme.find("mssql") != std::string::npos) return mssql;
    if (scheme.find("postgres") != std::string::npos) return postgres;
    if (scheme.find("oracle") != std::string::npos) return oracle;
    if (scheme.find("databricks") != std::string::npos) return databricks;
    if (scheme.find("sqlite") != std::string::npos) return sqlite;
    return generic;
}

}  // namespace fsp
