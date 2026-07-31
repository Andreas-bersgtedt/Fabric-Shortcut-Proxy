// Linkage smoke test for the native Arrow/Parquet toolchain. Prints the linked
// Arrow version. Expand into the parquet writer + stats reader once this builds.
#include <cstdio>

#include <arrow/util/config.h>

int main() {
    std::printf("Arrow C++ linked OK: version %s\n", ARROW_VERSION_STRING);
    return 0;
}
