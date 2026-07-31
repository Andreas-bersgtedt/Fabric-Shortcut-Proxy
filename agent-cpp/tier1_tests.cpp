// Tier 1 conformance tests: assert the C++ ports match the Python golden vectors.
// Build with build_tier1.ps1 (MSVC) or build_tier1.sh (g++).
#include <cstdint>
#include <cstdio>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "tier1/dialects.hpp"
#include "tier1/split_planner.hpp"
#include "tier1/shard_weight.hpp"
#include "tier1/delta_log.hpp"
#include "tier1/lru_cache.hpp"
#include "tier1/sha256.hpp"
#include "tier1/iceberg_schema.hpp"
#include "tier1/iceberg_state.hpp"
#include "tier1/iceberg_metadata.hpp"

static int g_pass = 0, g_fail = 0;

static void check(const std::string& label, const std::string& got, const std::string& want) {
    if (got == want) {
        ++g_pass;
    } else {
        ++g_fail;
        std::printf("FAIL %s\n  got:  %s\n  want: %s\n", label.c_str(), got.c_str(), want.c_str());
    }
}

// --- Python-json-compatible formatters for the shapes used by the golden vectors ---

static std::string json_pairs_int(const std::vector<std::pair<int64_t, int64_t>>& v) {
    std::string s = "[";
    for (size_t i = 0; i < v.size(); ++i) {
        if (i) s += ", ";
        s += "[" + std::to_string(v[i].first) + ", " + std::to_string(v[i].second) + "]";
    }
    return s + "]";
}

static std::string json_pairs_str(const std::vector<std::pair<std::string, std::string>>& v) {
    std::string s = "[";
    for (size_t i = 0; i < v.size(); ++i) {
        if (i) s += ", ";
        s += "[\"" + v[i].first + "\", \"" + v[i].second + "\"]";
    }
    return s + "]";
}

static std::string json_map_int(const std::map<std::string, int>& m) {
    std::string s = "{";
    bool first = true;
    for (const auto& kv : m) {
        if (!first) s += ", ";
        first = false;
        s += "\"" + kv.first + "\": " + std::to_string(kv.second);
    }
    return s + "}";
}

static std::string json_loads(const std::vector<fsp::ShardLoad>& v) {
    std::string s = "[";
    for (size_t i = 0; i < v.size(); ++i) {
        if (i) s += ", ";
        s += "{\"shard\": " + std::to_string(v[i].shard) +
             ", \"splits\": " + std::to_string(v[i].splits) +
             ", \"bytes\": " + std::to_string(v[i].bytes) + "}";
    }
    return s + "]";
}

static void test_dialects() {
    struct Case { const char* scheme; const char* name; };
    const std::vector<Case> cases = {
        {"sqlite", "sqlite"}, {"postgresql", "postgresql"}, {"mssql", "mssql"},
        {"oracle", "oracle"}, {"databricks", "databricks"}, {"mysql", "generic"},
    };
    for (const auto& c : cases) {
        const auto& d = fsp::get_dialect(std::string(c.scheme) + "+drv://u:p@h/db");
        check(std::string("dialect.name.") + c.scheme, d.name(), c.name);
    }
    const auto& sqlite = fsp::get_dialect("sqlite:///x");
    check("dialect.quote.sqlite", sqlite.quote("my col"), "\"my col\"");
    check("dialect.qualified.sqlite", sqlite.quote_qualified("public.sales"), "\"public\".\"sales\"");
    check("dialect.select.sqlite",
          sqlite.build_select("\"a\", \"b\"", "\"public\".\"sales\"", "\"id\"", "num_splits", "split_index", "max_rows"),
          "SELECT \"a\", \"b\" FROM \"public\".\"sales\" WHERE (CAST(\"id\" AS INTEGER) % :num_splits) = :split_index ORDER BY \"id\" LIMIT :max_rows");

    const auto& mssql = fsp::get_dialect("mssql+aioodbc://x");
    check("dialect.quote.mssql", mssql.quote("my col"), "[my col]");
    check("dialect.qualified.mssql", mssql.quote_qualified("public.sales"), "[public].[sales]");
    check("dialect.select.mssql",
          mssql.build_select("\"a\", \"b\"", "\"public\".\"sales\"", "\"id\"", "num_splits", "split_index", "max_rows"),
          "SELECT TOP (:max_rows) \"a\", \"b\" FROM \"public\".\"sales\" WHERE (CAST(\"id\" AS BIGINT) % :num_splits) = :split_index ORDER BY \"id\"");
    check("dialect.range.mssql",
          mssql.build_select_range("\"a\", \"b\"", "\"public\".\"sales\"", "\"id\"", "key_lo", "key_hi", "max_rows"),
          "SELECT \"a\", \"b\" FROM \"public\".\"sales\" WHERE \"id\" >= :key_lo AND \"id\" < :key_hi ORDER BY \"id\" LIMIT :max_rows");
    check("dialect.rownum.mssql",
          mssql.build_select_row_number("\"a\", \"b\"", "\"public\".\"sales\"", "\"id\"", "num_splits", "split_index", "max_rows"),
          "SELECT TOP (:max_rows) \"a\", \"b\" FROM (SELECT \"a\", \"b\", ROW_NUMBER() OVER (ORDER BY \"id\") AS [__row_num] FROM \"public\".\"sales\") AS q WHERE ((q.[__row_num] - 1) % :num_splits) = :split_index ORDER BY q.[__row_num]");

    const auto& oracle = fsp::get_dialect("oracle+oracledb://x");
    check("dialect.select.oracle",
          oracle.build_select("\"a\", \"b\"", "\"public\".\"sales\"", "\"id\"", "num_splits", "split_index", "max_rows"),
          "SELECT \"a\", \"b\" FROM \"public\".\"sales\" WHERE (MOD(CAST(\"id\" AS NUMBER(19)), :num_splits)) = :split_index ORDER BY \"id\" FETCH FIRST :max_rows ROWS ONLY");
    check("dialect.rownum.oracle",
          oracle.build_select_row_number("\"a\", \"b\"", "\"public\".\"sales\"", "\"id\"", "num_splits", "split_index", "max_rows"),
          "SELECT \"a\", \"b\" FROM (SELECT \"a\", \"b\", ROW_NUMBER() OVER (ORDER BY \"id\") AS \"__row_num\" FROM \"public\".\"sales\") q WHERE (MOD((q.\"__row_num\" - 1), :num_splits)) = :split_index ORDER BY q.\"__row_num\" FETCH FIRST :max_rows ROWS ONLY");

    const auto& databricks = fsp::get_dialect("databricks://x");
    check("dialect.range.databricks",
          databricks.build_select_range("\"a\", \"b\"", "\"public\".\"sales\"", "\"id\"", "key_lo", "key_hi", "max_rows"),
          "SELECT TOP (:max_rows) \"a\", \"b\" FROM \"public\".\"sales\" WHERE \"id\" >= :key_lo AND \"id\" < :key_hi ORDER BY \"id\"");
    check("dialect.quote.databricks", databricks.quote("my col"), "`my col`");
    check("dialect.select.databricks",
          databricks.build_select("\"a\", \"b\"", "\"public\".\"sales\"", "\"id\"", "num_splits", "split_index", "max_rows"),
          "SELECT \"a\", \"b\" FROM \"public\".\"sales\" WHERE (CAST(\"id\" AS BIGINT) % :num_splits) = :split_index ORDER BY \"id\" LIMIT :max_rows");
}

static void test_split_math() {
    check("keyranges.1.100.8", json_pairs_int(fsp::compute_key_ranges(1, 100, 8)),
          "[[1, 13], [13, 26], [26, 38], [38, 51], [51, 63], [63, 76], [76, 88], [88, 101]]");
    check("keyranges.0.0.4", json_pairs_int(fsp::compute_key_ranges(0, 0, 4)),
          "[[0, 0], [0, 0], [0, 0], [0, 1]]");
    check("keyranges.5.5.1", json_pairs_int(fsp::compute_key_ranges(5, 5, 1)), "[[5, 6]]");
    check("keyranges.10.3.3", json_pairs_int(fsp::compute_key_ranges(10, 3, 3)),
          "[[10, 11], [11, 11], [11, 11]]");
    check("keyranges.1.1000000.7", json_pairs_int(fsp::compute_key_ranges(1, 1000000, 7)),
          "[[1, 142858], [142858, 285715], [285715, 428572], [428572, 571429], [571429, 714286], [714286, 857143], [857143, 1000001]]");

    check("splitcount.none", std::to_string(fsp::compute_split_count(std::nullopt, 1000, 1, 64, 8)), "8");
    check("splitcount.zero", std::to_string(fsp::compute_split_count(int64_t(0), 1000, 1, 64, 8)), "8");
    check("splitcount.big", std::to_string(fsp::compute_split_count(int64_t(100000), 1000, 1, 64, 8)), "64");
    check("splitcount.min", std::to_string(fsp::compute_split_count(int64_t(100), 1000, 4, 64, 8)), "4");
    check("splitcount.cap", std::to_string(fsp::compute_split_count(int64_t(10000000), 1000000, 1, 8, 8)), "8");

    check("temporal.date", json_pairs_str(fsp::compute_temporal_ranges_date("2020-01-01", "2020-12-31", 4)),
          "[[\"2020-01-01\", \"2020-04-01\"], [\"2020-04-01\", \"2020-07-02\"], [\"2020-07-02\", \"2020-10-01\"], [\"2020-10-01\", \"2021-01-01\"]]");
    check("temporal.ts", json_pairs_str(fsp::compute_temporal_ranges_ts("2020-01-01T00:00:00+00:00", "2020-01-02T00:00:00+00:00", 3)),
          "[[\"2020-01-01T00:00:00+00:00\", \"2020-01-01T08:00:00+00:00\"], [\"2020-01-01T08:00:00+00:00\", \"2020-01-01T16:00:00+00:00\"], [\"2020-01-01T16:00:00+00:00\", \"2020-01-02T00:00:00.000001+00:00\"]]");
}

static void test_shard_weight() {
    check("stablekey", fsp::stable_key("sales", 3), "sales#3");
    check("defaultweight", std::to_string(static_cast<int>(fsp::default_weight({2.0, 4.0, 0.0}))), "3");
    std::vector<std::string> keys = {"a", "b", "c", "d", "e"};
    std::map<std::string, double> w = {{"a", 10.0}, {"b", 1.0}, {"c", 5.0}, {"d", 5.0}, {"e", 1.0}};
    auto assign = fsp::assign_owners(keys, 3, w);
    check("assign", json_map_int(assign), "{\"a\": 0, \"b\": 1, \"c\": 1, \"d\": 2, \"e\": 2}");
    check("loads", json_loads(fsp::shard_loads(assign, 3, w)),
          "[{\"shard\": 0, \"splits\": 1, \"bytes\": 10}, {\"shard\": 1, \"splits\": 2, \"bytes\": 6}, {\"shard\": 2, \"splits\": 2, \"bytes\": 6}]");
}

static void test_delta() {
    check("deltatype.boolean", fsp::delta_type("boolean"), "boolean");
    check("deltatype.int", fsp::delta_type("int"), "integer");
    check("deltatype.timestamp", fsp::delta_type("timestamp"), "timestamp_ntz");
    check("deltatype.timestamptz", fsp::delta_type("timestamptz"), "timestamp");
    check("deltatype.decimal", fsp::delta_type("decimal(10, 2)"), "decimal(10,2)");
    check("deltatype.fixed", fsp::delta_type("fixed(16)"), "binary");
    check("deltatype.uuid", fsp::delta_type("uuid"), "string");
    check("deltatype.weird", fsp::delta_type("weirdtype"), "string");

    check("tableuuid", fsp::delta_table_uuid("sales"), "b18229a1-f4f6-adf8-4a96-e878516290a2");

    std::vector<fsp::DeltaColumn> cols = {
        {"id", "long", false}, {"name", "string", true}, {"amt", "decimal(10, 2)", true}};
    check("schemastring", fsp::delta_schema_string(cols),
          R"j({"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"amt","type":"decimal(10,2)","nullable":true,"metadata":{}}]})j");

    const std::string tp = "db/local/sales";
    fsp::DeltaLog dlog("sales", tp, cols);
    fsp::DeltaVersion v1{1, 1700000000000LL,
        {{tp + "/data/split-0.parquet", 100, 10}, {tp + "/data/split-1.parquet", 200, 20}}};
    fsp::DeltaVersion v2{2, 1700000100000LL,
        {{tp + "/data/split-0.parquet", 100, 10}, {tp + "/data/split-1b.parquet", 250, 25}}};
    fsp::DeltaVersion v3{3, 1700000200000LL,  // identical file set -> no-op, no commit
        {{tp + "/data/split-0.parquet", 100, 10}, {tp + "/data/split-1b.parquet", 250, 25}}};
    dlog.register_version(v1);
    dlog.register_version(v2);
    dlog.register_version(v3);

    check("commit.count", std::to_string(dlog.commits().size()), "2");

    const std::string want0 =
R"j({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"b18229a1-f4f6-adf8-4a96-e878516290a2","name":"sales","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"amt\",\"type\":\"decimal(10,2)\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
{"add":{"path":"data/split-0.parquet","partitionValues":{},"size":100,"modificationTime":1700000000000,"dataChange":true,"stats":"{\"numRecords\": 10}"}}
{"add":{"path":"data/split-1.parquet","partitionValues":{},"size":200,"modificationTime":1700000000000,"dataChange":true,"stats":"{\"numRecords\": 20}"}}
)j";
    const std::string want1 =
R"j({"add":{"path":"data/split-1b.parquet","partitionValues":{},"size":250,"modificationTime":1700000100000,"dataChange":true,"stats":"{\"numRecords\": 25}"}}
{"remove":{"path":"data/split-1.parquet","deletionTimestamp":1700000100000,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":200}}
)j";
    check("commit.0", dlog.commits().empty() ? "" : dlog.commits()[0], want0);
    check("commit.1", dlog.commits().size() < 2 ? "" : dlog.commits()[1], want1);
}

static void test_cache() {
    double now = 0.0;
    auto clock = [&now]() { return now; };

    // basic put/get + byte accounting
    fsp::BytesLruCache c(10, 100.0, clock);
    c.put("a", std::string(5, 'x'));
    check("cache.hit", c.get("a") ? "1" : "0", "1");
    check("cache.bytes", std::to_string(c.current_bytes()), "5");
    check("cache.entries", std::to_string(c.entries()), "1");

    // oversized value is rejected
    fsp::BytesLruCache c2(10, 100.0, clock);
    c2.put("big", std::string(11, 'x'));
    check("cache.oversize", std::to_string(c2.entries()), "0");

    // size-bound eviction of the oldest
    fsp::BytesLruCache c3(10, 100.0, clock);
    c3.put("a", std::string(4, 'x'));
    c3.put("b", std::string(4, 'x'));
    c3.put("c", std::string(4, 'x'));  // evicts oldest "a"
    check("cache.evict.a", c3.get("a") ? "1" : "0", "0");
    check("cache.evict.b", c3.get("b") ? "1" : "0", "1");
    check("cache.evict.bytes", std::to_string(c3.current_bytes()), "8");

    // LRU recency: touching "a" makes "b" the eviction victim
    fsp::BytesLruCache c4(10, 100.0, clock);
    c4.put("a", std::string(4, 'x'));
    c4.put("b", std::string(4, 'x'));
    c4.get("a");                       // touch a -> newest
    c4.put("c", std::string(4, 'x'));  // evicts oldest "b"
    check("cache.lru.b", c4.get("b") ? "1" : "0", "0");
    check("cache.lru.a", c4.get("a") ? "1" : "0", "1");

    // TTL expiry
    fsp::BytesLruCache c5(100, 10.0, clock);
    now = 0.0;
    c5.put("t", std::string(3, 'x'));
    now = 11.0;
    check("cache.ttl.expired", c5.get("t") ? "1" : "0", "0");
    check("cache.ttl.entries", std::to_string(c5.entries()), "0");

    // update existing key resizes byte accounting
    fsp::BytesLruCache c6(100, 100.0, clock);
    c6.put("k", std::string(4, 'x'));
    c6.put("k", std::string(6, 'x'));
    check("cache.update.bytes", std::to_string(c6.current_bytes()), "6");
    check("cache.update.entries", std::to_string(c6.entries()), "1");

    // pinned store: never expires, survives independently of the LRU
    fsp::PinnedStore pin;
    pin.pin("p", std::string(7, 'x'));
    check("pin.peek", pin.peek("p") ? "1" : "0", "1");
    check("pin.bytes", std::to_string(pin.bytes()), "7");
    pin.unpin("p");
    check("pin.unpinned", pin.peek("p") ? "1" : "0", "0");
}

static void test_iceberg() {
    // SHA-256 known-answer vectors.
    check("sha256.abc", fsp::sha256_hex("abc"),
          "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    check("sha256.empty", fsp::sha256_hex(""),
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

    std::vector<fsp::IcebergColumn> cols = {
        {1, "id", "long", false}, {2, "name", "string", true}, {3, "amt", "decimal(10, 2)", true}};
    check("iceberg.schema", fsp::iceberg_schema_json(0, cols),
          R"j({"schema-id":0,"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"},{"id":2,"name":"name","required":false,"type":"string"},{"id":3,"name":"amt","required":false,"type":"decimal(10, 2)"}]})j");

    check("safe.space", fsp::safe_segment("my table!", "table"), "my_table_");
    check("safe.dot", fsp::safe_segment("public.orders", "x"), "public.orders");
    check("safe.empty", fsp::safe_segment("", "def"), "def");
    check("safe.trim", fsp::safe_segment("  spaced  ", "y"), "spaced");
    check("safe.slashes", fsp::safe_segment("weird/\\name*", "z"), "weird_name_");
    check("safe.allbad", fsp::safe_segment("!!!", "keep"), "_");

    auto sp = fsp::split_source_table("public.sales");
    check("split.dotted", sp.first + "|" + sp.second, "public|sales");
    auto sp2 = fsp::split_source_table("sales");
    check("split.plain", sp2.first + "|" + sp2.second, "default|sales");
    auto sp3 = fsp::split_source_table("a.b.c");
    check("split.multi", sp3.first + "|" + sp3.second, "a.b|c");

    check("legacy.path", fsp::legacy_table_path("sales", "warehouse"), "warehouse/sales");
    check("canonical.path", fsp::canonical_table_path("myhost", "my db", "public.sales", "warehouse"),
          "warehouse/myhost/my_db/public/sales");

    auto id = fsp::build_snapshot_identity("fabric-iceberg-poc", "warehouse/sales");
    check("v1.snapshot_id", std::to_string(id.snapshot_id), "541830654599756294");
    check("v1.watermark_ms", std::to_string(id.watermark_ms), "1700003723585");
    check("v1.manifest_list_key", id.manifest_list_key,
          "warehouse/sales/metadata/snap-541830654599756294-1-784f81c0fd5fa06e1ee16f5415a37abd.avro");
    check("v1.manifest_file_key", id.manifest_file_key,
          "warehouse/sales/metadata/784f81c0fd5fa06e1ee16f5415a37abd-m0.avro");
    check("v1.metadata_key", id.metadata_key, "warehouse/sales/metadata/v1.metadata.json");
    check("v1.version_hint_key", id.version_hint_key, "warehouse/sales/metadata/version-hint.text");
    check("v1.split.0", fsp::split_object_key("warehouse/sales", 0, id.snapshot_id),
          "warehouse/sales/data/split-0-541830654599756294.parquet");
    check("v1.split.2", fsp::split_object_key("warehouse/sales", 2, id.snapshot_id),
          "warehouse/sales/data/split-2-541830654599756294.parquet");

    auto v2 = fsp::advance_snapshot_identity("fabric-iceberg-poc", "warehouse/sales", 2, id.watermark_ms, 1);
    check("v2.snapshot_id", std::to_string(v2.snapshot_id), "1138800673247596398");
    check("v2.watermark_ms", std::to_string(v2.watermark_ms), "1700003725585");
    check("v2.sequence_number", std::to_string(v2.sequence_number), "2");
    check("v2.manifest_list_key", v2.manifest_list_key,
          "warehouse/sales/metadata/snap-1138800673247596398-2-fcdd52dbee60f6e515d449ffb359f9cd.avro");
    check("v2.manifest_file_key", v2.manifest_file_key,
          "warehouse/sales/metadata/fcdd52dbee60f6e515d449ffb359f9cd-m2.avro");
    check("v2.metadata_key", v2.metadata_key, "warehouse/sales/metadata/v2.metadata.json");

    const std::string wantMeta =
R"j({
  "format-version": 2,
  "table-uuid": "a911a7a9-e911-0b07-4701-4188a811b6a7",
  "location": "s3://fabric-iceberg-poc/warehouse/sales",
  "last-sequence-number": 1,
  "last-updated-ms": 1700003723585,
  "last-column-id": 3,
  "current-schema-id": 0,
  "schemas": [
    {
      "schema-id": 0,
      "type": "struct",
      "fields": [
        {
          "id": 1,
          "name": "id",
          "required": true,
          "type": "long"
        },
        {
          "id": 2,
          "name": "name",
          "required": false,
          "type": "string"
        },
        {
          "id": 3,
          "name": "amt",
          "required": false,
          "type": "decimal(10, 2)"
        }
      ]
    }
  ],
  "partition-specs": [
    {
      "spec-id": 0,
      "fields": []
    }
  ],
  "default-spec-id": 0,
  "last-partition-id": 0,
  "sort-orders": [
    {
      "order-id": 0,
      "fields": []
    }
  ],
  "default-sort-order-id": 0,
  "snapshots": [
    {
      "snapshot-id": 541830654599756294,
      "sequence-number": 1,
      "timestamp-ms": 1700003723585,
      "summary": {
        "operation": "append",
        "added-data-files": "3",
        "added-records": "0",
        "total-records": "0",
        "total-data-files": "3",
        "total-delete-files": "0"
      },
      "manifest-list": "s3://fabric-iceberg-poc/warehouse/sales/metadata/snap-541830654599756294-1-784f81c0fd5fa06e1ee16f5415a37abd.avro",
      "schema-id": 0
    }
  ],
  "current-snapshot-id": 541830654599756294,
  "snapshot-log": [
    {
      "timestamp-ms": 1700003723585,
      "snapshot-id": 541830654599756294
    }
  ],
  "metadata-log": [],
  "refs": {
    "main": {
      "type": "branch",
      "snapshot-id": 541830654599756294
    }
  },
  "statistics": [],
  "partition-statistics": []
})j";
    check("metadata.json",
          fsp::build_metadata_json("fabric-iceberg-poc", "warehouse/sales", cols, id, 0, 3), wantMeta);
}

int main() {
    test_dialects();
    test_split_math();
    test_shard_weight();
    test_delta();
    test_cache();
    test_iceberg();
    std::printf("\ntier1: %d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
