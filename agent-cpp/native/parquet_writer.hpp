// Write an Arrow table to Parquet bytes, mirroring parquet/generator.py
// `_table_to_bytes`: snappy compression, statistics on, 65536 write-batch size,
// and the Arrow schema (with PARQUET:field_id metadata) preserved. The Parquet
// output is not byte-identical to PyArrow's, but is validated by round-tripping
// through pyarrow/pyiceberg. C++17 + Arrow/Parquet.
#pragma once

#include <memory>
#include <stdexcept>
#include <string>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/writer.h>

namespace fsp {
namespace native {

// PyArrow's row_group_size is a row count; parquet/generator.py uses this value.
constexpr int64_t kRowGroupSize = 128 * 1024 * 1024;
constexpr int64_t kWriteBatchSize = 65536;

inline void check_ok(const arrow::Status& st, const char* what) {
    if (!st.ok()) throw std::runtime_error(std::string(what) + ": " + st.ToString());
}

// Serialize a table to an in-memory Parquet file and return the raw bytes.
inline std::string write_table_to_parquet(const std::shared_ptr<arrow::Table>& table) {
    auto sink_res = arrow::io::BufferOutputStream::Create();
    check_ok(sink_res.status(), "BufferOutputStream::Create");
    std::shared_ptr<arrow::io::BufferOutputStream> sink = *sink_res;

    std::shared_ptr<parquet::WriterProperties> wprops =
        parquet::WriterProperties::Builder()
            .compression(parquet::Compression::SNAPPY)
            ->write_batch_size(kWriteBatchSize)
            ->enable_statistics()
            ->build();
    std::shared_ptr<parquet::ArrowWriterProperties> aprops =
        parquet::ArrowWriterProperties::Builder().store_schema()->build();

    check_ok(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink,
                                        kRowGroupSize, wprops, aprops),
             "parquet::arrow::WriteTable");

    auto buf_res = sink->Finish();
    check_ok(buf_res.status(), "BufferOutputStream::Finish");
    std::shared_ptr<arrow::Buffer> buf = *buf_res;
    return std::string(reinterpret_cast<const char*>(buf->data()), static_cast<size_t>(buf->size()));
}

}  // namespace native
}  // namespace fsp
