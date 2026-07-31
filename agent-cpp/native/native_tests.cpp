// Native round-trip harness: build a table for the default TABLE_SCHEMA, write
// Parquet (with PARQUET:field_id metadata), read the row-group stats back, and
// check the Iceberg-encoded bounds against the tier1 encoders. Optionally writes
// the Parquet bytes to argv[1] for the pyarrow/pyiceberg round-trip check.
#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "avro_manifest.hpp"
#include "iceberg_arrow.hpp"
#include "iceberg_schema.hpp"  // fsp::IcebergColumn
#include "iceberg_stats.hpp"   // fsp::encode_* + to_hex
#include "parquet_writer.hpp"
#include "split_stats.hpp"

namespace {

int g_pass = 0;
int g_fail = 0;

void check(bool ok, const std::string& what) {
    if (ok) {
        ++g_pass;
    } else {
        ++g_fail;
        std::printf("  FAIL: %s\n", what.c_str());
    }
}

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

}  // namespace

int main(int argc, char** argv) {
    using namespace fsp;
    using namespace fsp::native;

    const std::vector<IcebergColumn> cols = default_schema();
    std::shared_ptr<arrow::Schema> schema = build_arrow_schema(cols);

    // field_id metadata present on every field (drives Iceberg column mapping).
    for (int i = 0; i < schema->num_fields(); ++i) {
        auto md = schema->field(i)->metadata();
        check(md && md->Contains("PARQUET:field_id"), "field_id metadata on " + cols[i].name);
        if (md && md->Contains("PARQUET:field_id")) {
            check(md->Get("PARQUET:field_id").ValueOrDie() == std::to_string(cols[i].field_id),
                  "field_id value on " + cols[i].name);
        }
    }

    // Build a 3-row table with known min/max per column.
    arrow::Int64Builder id;
    check(id.Append(1).ok() && id.Append(2).ok() && id.Append(3).ok(), "append id");
    arrow::Date32Builder order_date;  // days since epoch
    check(order_date.Append(19000).ok() && order_date.Append(19001).ok() &&
              order_date.Append(19002).ok(), "append order_date");
    arrow::Int64Builder customer_id;
    check(customer_id.Append(100).ok() && customer_id.Append(100).ok() &&
              customer_id.Append(200).ok(), "append customer_id");
    arrow::StringBuilder product;
    check(product.Append("apple").ok() && product.Append("banana").ok() &&
              product.Append("cherry").ok(), "append product");
    arrow::Int32Builder quantity;
    check(quantity.Append(5).ok() && quantity.Append(10).ok() && quantity.Append(15).ok(),
          "append quantity");
    arrow::DoubleBuilder unit_price;
    check(unit_price.Append(1.5).ok() && unit_price.Append(2.5).ok() &&
              unit_price.Append(3.5).ok(), "append unit_price");
    arrow::DoubleBuilder total;
    check(total.Append(7.5).ok() && total.Append(25.0).ok() && total.Append(52.5).ok(),
          "append total");
    arrow::StringBuilder region;
    check(region.Append("east").ok() && region.Append("west").ok() &&
              region.Append("north").ok(), "append region");

    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        finish(id), finish(order_date), finish(customer_id), finish(product),
        finish(quantity), finish(unit_price), finish(total), finish(region),
    };
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, arrays);
    check(table->num_rows() == 3, "table has 3 rows");

    // Write Parquet, then read stats back.
    std::string pq = write_table_to_parquet(table);
    check(!pq.empty(), "parquet bytes non-empty");
    check(pq.rfind("PAR1", 0) == 0, "parquet magic header");

    std::map<int, ColumnStats> stats = collect_split_stats(pq, cols);
    check(stats.size() == cols.size(), "stats for every column");

    auto& s_id = stats[1];
    check(s_id.value_count == 3, "id value_count");
    check(s_id.null_count == 0, "id null_count");
    check(s_id.lower.has_value() && *s_id.lower == encode_long(1), "id lower == encode_long(1)");
    check(s_id.upper.has_value() && *s_id.upper == encode_long(3), "id upper == encode_long(3)");

    auto& s_date = stats[2];
    check(s_date.lower.has_value() && *s_date.lower == encode_date_days(19000),
          "order_date lower == encode_date_days(19000)");
    check(s_date.upper.has_value() && *s_date.upper == encode_date_days(19002),
          "order_date upper == encode_date_days(19002)");

    auto& s_cust = stats[3];
    check(s_cust.lower.has_value() && *s_cust.lower == encode_long(100), "customer_id lower");
    check(s_cust.upper.has_value() && *s_cust.upper == encode_long(200), "customer_id upper");

    auto& s_prod = stats[4];
    check(s_prod.lower.has_value() && *s_prod.lower == encode_string("apple"), "product lower");
    check(s_prod.upper.has_value() && *s_prod.upper == encode_string("cherry"), "product upper");

    auto& s_qty = stats[5];
    check(s_qty.lower.has_value() && *s_qty.lower == encode_int32(5), "quantity lower");
    check(s_qty.upper.has_value() && *s_qty.upper == encode_int32(15), "quantity upper");

    auto& s_price = stats[6];
    check(s_price.lower.has_value() && *s_price.lower == encode_double(1.5), "unit_price lower");
    check(s_price.upper.has_value() && *s_price.upper == encode_double(3.5), "unit_price upper");

    auto& s_region = stats[8];
    check(s_region.lower.has_value() && *s_region.lower == encode_string("east"), "region lower");
    check(s_region.upper.has_value() && *s_region.upper == encode_string("west"), "region upper");

    // Build Iceberg manifests (Avro) from a snapshot referencing this split.
    ManifestSnapshot snap;
    snap.snapshot_id = 8102938475610293847LL;
    snap.sequence_number = 1;
    snap.bucket_name = "warehouse";
    snap.manifest_file_key = "metadata/snap-1-m0.avro";
    ManifestSplit split;
    split.object_key = "data/split-0.parquet";
    split.record_count = 3;
    split.file_size_in_bytes = static_cast<int64_t>(pq.size());
    split.stats = stats;
    snap.splits.push_back(split);

    // Default (no-stats) manifest: the known-good path.
    std::string mf = build_manifest_file(snap);
    check(!mf.empty(), "manifest file non-empty");
    check(mf.rfind("Obj\x01", 0) == 0, "manifest file OCF magic");
    std::string ml = build_manifest_list(snap, static_cast<int64_t>(mf.size()));
    check(!ml.empty(), "manifest list non-empty");
    check(ml.rfind("Obj\x01", 0) == 0, "manifest list OCF magic");

    // Stats-in-manifest variant (F3).
    ManifestSnapshot snap_stats = snap;
    snap_stats.with_stats = true;
    std::string mf_stats = build_manifest_file(snap_stats);
    check(!mf_stats.empty(), "manifest file (stats) non-empty");
    check(mf_stats.rfind("Obj\x01", 0) == 0, "manifest file (stats) OCF magic");

    // Optionally dump the Parquet + manifests for the pyarrow/pyiceberg round-trip.
    if (argc > 1) {
        std::ofstream f(argv[1], std::ios::binary);
        f.write(pq.data(), static_cast<std::streamsize>(pq.size()));
        std::printf("wrote parquet: %s (%zu bytes)\n", argv[1], pq.size());
    }
    if (argc > 2) {
        std::ofstream f(argv[2], std::ios::binary);
        f.write(mf.data(), static_cast<std::streamsize>(mf.size()));
        std::printf("wrote manifest file: %s (%zu bytes)\n", argv[2], mf.size());
    }
    if (argc > 3) {
        std::ofstream f(argv[3], std::ios::binary);
        f.write(ml.data(), static_cast<std::streamsize>(ml.size()));
        std::printf("wrote manifest list: %s (%zu bytes)\n", argv[3], ml.size());
    }
    if (argc > 4) {
        std::ofstream f(argv[4], std::ios::binary);
        f.write(mf_stats.data(), static_cast<std::streamsize>(mf_stats.size()));
        std::printf("wrote manifest file (stats): %s (%zu bytes)\n", argv[4], mf_stats.size());
    }

    std::printf("native: %d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
