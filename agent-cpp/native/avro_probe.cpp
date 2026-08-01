// Probe: does avro-cpp preserve custom schema attributes (Iceberg `field-id`)
// in the Object Container File header schema? pyiceberg maps manifest columns
// via those field-ids, so they must survive a C++ write. Writes an OCF to
// argv[1]; a Python reader then checks the writer schema for `field-id`.
#include <cstdio>
#include <string>

#include <fmt/format.h>  // avro/Exception.hh uses fmt::format but only pulls fmt/base.h

#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/ValidSchema.hh>

static const char* kSchema = R"({
  "type": "record",
  "name": "probe",
  "fields": [
    {"name": "x", "type": "long", "field-id": 100},
    {"name": "s", "type": "string", "field-id": 101}
  ]
})";

int main(int argc, char** argv) {
    if (argc < 2) {
        std::printf("usage: avro_probe <out.avro>\n");
        return 2;
    }
    avro::ValidSchema schema = avro::compileJsonSchemaFromString(kSchema);

    avro::GenericDatum datum(schema);
    avro::GenericRecord& rec = datum.value<avro::GenericRecord>();
    rec.field("x").value<int64_t>() = 42;
    rec.field("s").value<std::string>() = "hello";

    avro::DataFileWriter<avro::GenericDatum> writer(argv[1], schema);
    writer.write(datum);
    writer.close();

    std::printf("wrote avro probe: %s\n", argv[1]);
    return 0;
}
