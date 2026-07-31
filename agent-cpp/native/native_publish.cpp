// Native Iceberg serving-image publisher. Assembles a complete, self-contained
// table image (Parquet splits + Avro manifests + metadata.json + version-hint)
// using the native modules and the tier1 iceberg headers, writing every object
// under its exact S3 key into a store directory. The zero-dependency C++ serving
// agent (agent.cpp) can then serve it, and pyiceberg reads it as a valid table.
//
//   native_publish <store_dir> [rows] [splits] [bucket] [table_path]
//
// Mirrors the Python materialization pipeline (iceberg/*, parquet/generator.py)
// that runtime/serving_image.py snapshots into the store.
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "avro_manifest.hpp"
#include "delta_log.hpp"        // fsp::DeltaLog (tier1)
#include "iceberg_arrow.hpp"
#include "iceberg_metadata.hpp"  // fsp::build_metadata_json (tier1)
#include "iceberg_schema.hpp"    // fsp::IcebergColumn (tier1)
#include "iceberg_state.hpp"     // fsp::build_snapshot_identity, split_object_key (tier1)
#include "parquet_writer.hpp"
#include "pg_source.hpp"
#include "split_planner.hpp"     // fsp::Column, pk_column, compute_key_ranges (tier1)
#include "split_stats.hpp"
#include "sql_source.hpp"

namespace fs = std::filesystem;

namespace {

// The default config.TABLE_SCHEMA, mirrored in C++.
std::vector<fsp::IcebergColumn> default_schema() {
    return {
        {1, "id", "long", false},
        {2, "order_date", "date", true},
        {3, "customer_id", "long", true},
        {4, "product", "string", true},
        {5, "quantity", "int", true},
        {6, "unit_price", "double", true},
        {7, "total", "double", true},
        {8, "region", "string", true},
    };
}

std::shared_ptr<arrow::Array> finish(arrow::ArrayBuilder& b) {
    std::shared_ptr<arrow::Array> a;
    auto st = b.Finish(&a);
    if (!st.ok()) throw std::runtime_error("builder finish: " + st.ToString());
    return a;
}

// Deterministic demo rows [start, start+count) for the default schema. Values are
// arbitrary; only the schema, field-ids, and row count matter for Iceberg reads.
std::shared_ptr<arrow::Table> build_split_table(const std::shared_ptr<arrow::Schema>& schema,
                                                int64_t start, int64_t count) {
    static const char* products[] = {"apple", "banana", "cherry", "date", "fig"};
    static const char* regions[] = {"east", "west", "north", "south"};

    arrow::Int64Builder id;
    arrow::Date32Builder order_date;
    arrow::Int64Builder customer_id;
    arrow::StringBuilder product;
    arrow::Int32Builder quantity;
    arrow::DoubleBuilder unit_price;
    arrow::DoubleBuilder total;
    arrow::StringBuilder region;

    auto ok = [](const arrow::Status& s) {
        if (!s.ok()) throw std::runtime_error("append: " + s.ToString());
    };
    for (int64_t i = start; i < start + count; ++i) {
        const int q = static_cast<int>(1 + (i % 100));
        const double up = 1.0 + static_cast<double>(i % 50) * 0.25;
        ok(id.Append(i + 1));
        ok(order_date.Append(static_cast<int32_t>(19000 + (i % 365))));
        ok(customer_id.Append(1000 + (i % 500)));
        ok(product.Append(products[i % 5]));
        ok(quantity.Append(q));
        ok(unit_price.Append(up));
        ok(total.Append(q * up));
        ok(region.Append(regions[i % 4]));
    }
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        finish(id), finish(order_date), finish(customer_id), finish(product),
        finish(quantity), finish(unit_price), finish(total), finish(region),
    };
    return arrow::Table::Make(schema, arrays);
}

void write_object(const std::string& store_dir, const std::string& key, const std::string& bytes) {
    fs::path path = fs::path(store_dir) / key;
    fs::create_directories(path.parent_path());
    std::ofstream f(path, std::ios::binary);
    if (!f) throw std::runtime_error("cannot open for write: " + path.string());
    f.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

int64_t arg_i64(int argc, char** argv, int idx, int64_t def) {
    if (argc > idx) return std::strtoll(argv[idx], nullptr, 10);
    return def;
}

// Last path segment, used as the Delta table name.
std::string table_basename(const std::string& table_path) {
    size_t p = table_path.find_last_of('/');
    return (p == std::string::npos) ? table_path : table_path.substr(p + 1);
}

// Write the Parquet splits (shared), then the format-specific metadata: Iceberg
// (manifests + metadata.json) or Delta (_delta_log commit 0). Both formats serve
// the same content-addressed Parquet split files.
void publish_image(const std::string& store_dir, const std::string& bucket,
                   const std::string& table_path, const std::vector<fsp::IcebergColumn>& cols,
                   const std::vector<std::shared_ptr<arrow::Table>>& split_tables,
                   const std::string& format) {
    using namespace fsp;
    using namespace fsp::native;

    SnapshotIdentity id = build_snapshot_identity(bucket, table_path);
    const bool is_delta = (format == "delta");

    struct SplitInfo {
        std::string object_key;
        int64_t records = 0;
        int64_t size = 0;
        std::map<int, ColumnStats> stats;
    };
    std::vector<SplitInfo> infos;
    int64_t total_rows = 0;
    for (int s = 0; s < static_cast<int>(split_tables.size()); ++s) {
        const std::shared_ptr<arrow::Table>& table = split_tables[s];
        std::string pq = write_table_to_parquet(table);
        const std::string object_key = split_object_key(table_path, s, id.snapshot_id);
        write_object(store_dir, object_key, pq);

        SplitInfo info;
        info.object_key = object_key;
        info.records = table->num_rows();
        info.size = static_cast<int64_t>(pq.size());
        if (!is_delta) info.stats = collect_split_stats(pq, cols);  // Iceberg manifest bounds
        infos.push_back(std::move(info));
        total_rows += table->num_rows();
    }

    if (is_delta) {
        std::vector<DeltaColumn> dcols;
        for (const auto& c : cols) dcols.push_back({c.name, c.iceberg_type, c.nullable});
        DeltaLog dlog(table_basename(table_path), table_path, dcols);
        DeltaVersion v;
        v.version = 1;
        v.watermark_ms = id.watermark_ms;
        for (const auto& info : infos) v.splits.push_back({info.object_key, info.size, info.records});
        dlog.register_version(v);

        const std::string log_key = table_path + "/_delta_log/00000000000000000000.json";
        write_object(store_dir, log_key, dlog.commits().at(0));
        std::printf("published (delta): rows=%lld splits=%zu table_path=%s\n",
                    static_cast<long long>(total_rows), split_tables.size(), table_path.c_str());
        std::printf("  delta_log: %s\n", log_key.c_str());
        std::printf("  store_dir: %s\n", store_dir.c_str());
        return;
    }

    ManifestSnapshot snap;
    snap.snapshot_id = static_cast<int64_t>(id.snapshot_id);
    snap.sequence_number = id.sequence_number;
    snap.bucket_name = bucket;
    snap.manifest_file_key = id.manifest_file_key;
    for (const auto& info : infos) {
        ManifestSplit split;
        split.object_key = info.object_key;
        split.record_count = info.records;
        split.file_size_in_bytes = info.size;
        split.stats = info.stats;
        snap.splits.push_back(split);
    }

    const std::string mf = build_manifest_file(snap);
    write_object(store_dir, id.manifest_file_key, mf);
    const std::string ml = build_manifest_list(snap, static_cast<int64_t>(mf.size()));
    write_object(store_dir, id.manifest_list_key, ml);
    const std::string metadata =
        build_metadata_json(bucket, table_path, cols, id, total_rows, static_cast<int>(split_tables.size()));
    write_object(store_dir, id.metadata_key, metadata);
    write_object(store_dir, id.version_hint_key, "1");

    std::printf("published (iceberg): rows=%lld splits=%zu bucket=%s table_path=%s\n",
                static_cast<long long>(total_rows), split_tables.size(), bucket.c_str(),
                table_path.c_str());
    std::printf("  metadata: %s\n", id.metadata_key.c_str());
    std::printf("  manifest_list: %s\n", id.manifest_list_key.c_str());
    std::printf("  store_dir: %s\n", store_dir.c_str());
}

std::vector<fsp::Column> as_planner_columns(const std::vector<fsp::IcebergColumn>& cols) {
    std::vector<fsp::Column> out;
    out.reserve(cols.size());
    for (const auto& c : cols) out.push_back({c.name, c.iceberg_type, c.nullable});
    return out;
}

std::string flag(int argc, char** argv, const std::string& name, const std::string& def) {
    for (int i = 1; i + 1 < argc; ++i)
        if (name == argv[i]) return argv[i + 1];
    return def;
}

bool has_flag(int argc, char** argv, const std::string& name) {
    for (int i = 1; i < argc; ++i)
        if (name == argv[i]) return true;
    return false;
}

// Demo mode: synthesize rows entirely in C++ (no database).
int run_demo(int argc, char** argv, const std::string& format) {
    using namespace fsp;
    using namespace fsp::native;

    const std::string store_dir = argv[1];
    const int64_t total_rows = arg_i64(argc, argv, 2, 50000);
    const int num_splits = static_cast<int>(arg_i64(argc, argv, 3, 5));
    const std::string bucket = argc > 4 ? argv[4] : "fabric-iceberg-poc";
    const std::string table_path = argc > 5 ? argv[5] : "warehouse/demo/orders";
    if (total_rows <= 0 || num_splits <= 0) {
        std::printf("rows and splits must be positive\n");
        return 2;
    }

    const std::vector<IcebergColumn> cols = default_schema();
    std::shared_ptr<arrow::Schema> schema = build_arrow_schema(cols);
    const int64_t base = total_rows / num_splits;
    const int64_t rem = total_rows % num_splits;

    std::vector<std::shared_ptr<arrow::Table>> tables;
    int64_t start = 0;
    for (int s = 0; s < num_splits; ++s) {
        const int64_t count = base + (s == num_splits - 1 ? rem : 0);
        tables.push_back(build_split_table(schema, start, count));
        start += count;
    }
    publish_image(store_dir, bucket, table_path, cols, tables, format);
    return 0;
}

// SQL mode: materialize from a SQLite table using range-based split queries.
int run_sql(int argc, char** argv, const std::string& format) {
    using namespace fsp;
    using namespace fsp::native;

    const std::string db_path = flag(argc, argv, "--sqlite", "");
    const std::string store_dir = flag(argc, argv, "--store", "");
    const std::string table = flag(argc, argv, "--table", "sales");
    const std::string key = flag(argc, argv, "--key", "");  // "" => planner default (id)
    const int num_splits = static_cast<int>(std::strtoll(flag(argc, argv, "--splits", "8").c_str(), nullptr, 10));
    const std::string bucket = flag(argc, argv, "--bucket", "fabric-iceberg-poc");
    const std::string table_path = flag(argc, argv, "--table-path", "warehouse/demo/orders");
    const int64_t seed_rows = std::strtoll(flag(argc, argv, "--seed", "0").c_str(), nullptr, 10);

    if (db_path.empty() || store_dir.empty()) {
        std::printf("usage: native_publish --sqlite <db> --store <dir> [--seed <rows>] "
                    "[--splits <n>] [--table <name>] [--key <col>] [--bucket <b>] [--table-path <p>]\n");
        return 2;
    }

    const std::vector<IcebergColumn> cols = default_schema();
    std::shared_ptr<arrow::Schema> schema = build_arrow_schema(cols);
    const SQLiteDialect dialect;
    const std::string pk = pk_column(key, as_planner_columns(cols));

    sqlite3* db = sql_open(db_path);
    try {
        if (seed_rows > 0) {
            int64_t seeded = seed_demo_table(db, table, seed_rows);
            std::printf("seeded: table=%s rows=%lld (db=%s)\n", table.c_str(),
                        static_cast<long long>(seeded), db_path.c_str());
        }

        std::pair<int64_t, int64_t> bounds = sql_key_bounds(db, dialect, table, pk);
        std::vector<std::pair<int64_t, int64_t>> ranges =
            compute_key_ranges(bounds.first, bounds.second, num_splits);

        std::vector<std::shared_ptr<arrow::Table>> tables;
        for (const auto& r : ranges) {
            const int64_t max_rows = r.second > r.first ? (r.second - r.first) : 1;
            tables.push_back(
                sql_read_range(db, dialect, cols, schema, table, pk, r.first, r.second, max_rows));
        }
        std::printf("sql: db=%s table=%s key=%s splits=%d key_range=[%lld,%lld]\n", db_path.c_str(),
                    table.c_str(), pk.c_str(), num_splits, static_cast<long long>(bounds.first),
                    static_cast<long long>(bounds.second));
        publish_image(store_dir, bucket, table_path, cols, tables, format);
    } catch (...) {
        sqlite3_close(db);
        throw;
    }
    sqlite3_close(db);
    return 0;
}

// Postgres mode: materialize from a PostgreSQL table using range-based split queries.
int run_pg(int argc, char** argv, const std::string& format) {
    using namespace fsp;
    using namespace fsp::native;

    const std::string conninfo = flag(argc, argv, "--postgres", "");
    const std::string store_dir = flag(argc, argv, "--store", "");
    const std::string table = flag(argc, argv, "--table", "sales");
    const std::string key = flag(argc, argv, "--key", "");  // "" => planner default (id)
    const int num_splits = static_cast<int>(std::strtoll(flag(argc, argv, "--splits", "8").c_str(), nullptr, 10));
    const std::string bucket = flag(argc, argv, "--bucket", "fabric-iceberg-poc");
    const std::string table_path = flag(argc, argv, "--table-path", "warehouse/demo/orders");
    const int64_t seed_rows = std::strtoll(flag(argc, argv, "--seed", "0").c_str(), nullptr, 10);

    if (conninfo.empty() || store_dir.empty()) {
        std::printf("usage: native_publish --postgres <conninfo> --store <dir> [--seed <rows>] "
                    "[--splits <n>] [--table <name>] [--key <col>] [--bucket <b>] [--table-path <p>]\n");
        return 2;
    }

    const std::vector<IcebergColumn> cols = default_schema();
    std::shared_ptr<arrow::Schema> schema = build_arrow_schema(cols);
    const PostgresDialect dialect;
    const std::string pk = pk_column(key, as_planner_columns(cols));

    PGconn* conn = pg_open(conninfo);
    try {
        if (seed_rows > 0) {
            int64_t seeded = seed_demo_table_pg(conn, table, seed_rows);
            std::printf("seeded: table=%s rows=%lld (postgres)\n", table.c_str(),
                        static_cast<long long>(seeded));
        }

        std::pair<int64_t, int64_t> bounds = pg_key_bounds(conn, dialect, table, pk);
        std::vector<std::pair<int64_t, int64_t>> ranges =
            compute_key_ranges(bounds.first, bounds.second, num_splits);

        std::vector<std::shared_ptr<arrow::Table>> tables;
        for (const auto& rg : ranges) {
            const int64_t max_rows = rg.second > rg.first ? (rg.second - rg.first) : 1;
            tables.push_back(
                pg_read_range(conn, dialect, cols, schema, table, pk, rg.first, rg.second, max_rows));
        }
        std::printf("postgres: table=%s key=%s splits=%d key_range=[%lld,%lld]\n", table.c_str(),
                    pk.c_str(), num_splits, static_cast<long long>(bounds.first),
                    static_cast<long long>(bounds.second));
        publish_image(store_dir, bucket, table_path, cols, tables, format);
    } catch (...) {
        PQfinish(conn);
        throw;
    }
    PQfinish(conn);
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::printf("usage:\n"
                    "  native_publish <store_dir> [rows] [splits] [bucket] [table_path]   (demo data)\n"
                    "  native_publish --sqlite <db> --store <dir> [--seed <rows>] [--splits <n>] "
                    "[--table <name>] [--key <col>] [--bucket <b>] [--table-path <p>]      (SQLite source)\n"
                    "  native_publish --postgres <conninfo> --store <dir> [--seed <rows>] [--splits <n>] "
                    "[--table <name>] [--key <col>] [--bucket <b>] [--table-path <p>]  (PostgreSQL source)\n"
                    "  add --format iceberg|delta (default iceberg) to any form.\n");
        return 2;
    }
    // Pull --format out of argv so it composes with the positional demo form.
    std::string format = "iceberg";
    std::vector<char*> filtered;
    filtered.push_back(argv[0]);
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--format" && i + 1 < argc) {
            format = argv[++i];
            continue;
        }
        filtered.push_back(argv[i]);
    }
    if (format != "iceberg" && format != "delta") {
        std::fprintf(stderr, "native_publish: --format must be 'iceberg' or 'delta'\n");
        return 2;
    }
    int fargc = static_cast<int>(filtered.size());
    char** fargv = filtered.data();

    try {
        if (has_flag(fargc, fargv, "--postgres")) return run_pg(fargc, fargv, format);
        if (has_flag(fargc, fargv, "--sqlite")) return run_sql(fargc, fargv, format);
        return run_demo(fargc, fargv, format);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "native_publish error: %s\n", e.what());
        return 1;
    }
}
