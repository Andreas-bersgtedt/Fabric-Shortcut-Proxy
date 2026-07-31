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
#include "iceberg_arrow.hpp"
#include "iceberg_metadata.hpp"  // fsp::build_metadata_json (tier1)
#include "iceberg_schema.hpp"    // fsp::IcebergColumn (tier1)
#include "iceberg_state.hpp"     // fsp::build_snapshot_identity, split_object_key (tier1)
#include "parquet_writer.hpp"
#include "split_stats.hpp"

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

}  // namespace

int main(int argc, char** argv) {
    using namespace fsp;
    using namespace fsp::native;

    if (argc < 2) {
        std::printf("usage: native_publish <store_dir> [rows] [splits] [bucket] [table_path]\n");
        return 2;
    }
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
    SnapshotIdentity id = build_snapshot_identity(bucket, table_path);

    // One split per (roughly) equal row range; last split absorbs the remainder.
    const int64_t base = total_rows / num_splits;
    const int64_t rem = total_rows % num_splits;

    ManifestSnapshot snap;
    snap.snapshot_id = static_cast<int64_t>(id.snapshot_id);
    snap.sequence_number = id.sequence_number;
    snap.bucket_name = bucket;
    snap.manifest_file_key = id.manifest_file_key;

    int64_t start = 0;
    for (int s = 0; s < num_splits; ++s) {
        const int64_t count = base + (s == num_splits - 1 ? rem : 0);
        std::shared_ptr<arrow::Table> table = build_split_table(schema, start, count);
        std::string pq = write_table_to_parquet(table);
        std::map<int, ColumnStats> stats = collect_split_stats(pq, cols);

        const std::string object_key = split_object_key(table_path, s, id.snapshot_id);
        write_object(store_dir, object_key, pq);

        ManifestSplit split;
        split.object_key = object_key;
        split.record_count = count;
        split.file_size_in_bytes = static_cast<int64_t>(pq.size());
        split.stats = stats;
        snap.splits.push_back(split);
        start += count;
    }

    const std::string mf = build_manifest_file(snap);
    write_object(store_dir, id.manifest_file_key, mf);

    const std::string ml = build_manifest_list(snap, static_cast<int64_t>(mf.size()));
    write_object(store_dir, id.manifest_list_key, ml);

    const std::string metadata =
        build_metadata_json(bucket, table_path, cols, id, total_rows, num_splits);
    write_object(store_dir, id.metadata_key, metadata);

    write_object(store_dir, id.version_hint_key, "1");

    std::printf("published: rows=%lld splits=%d bucket=%s table_path=%s\n",
                static_cast<long long>(total_rows), num_splits, bucket.c_str(), table_path.c_str());
    std::printf("  metadata: %s\n", id.metadata_key.c_str());
    std::printf("  manifest_list: %s\n", id.manifest_list_key.c_str());
    std::printf("  manifest_file: %s\n", id.manifest_file_key.c_str());
    std::printf("  store_dir: %s\n", store_dir.c_str());
    return 0;
}
