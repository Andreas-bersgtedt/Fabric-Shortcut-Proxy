# Native Tier 1 modules (Arrow/Parquet + Avro). Requires vcpkg-provided Apache
# Arrow C++ (parquet feature) and Avro C++. Configure through the vcpkg toolchain
# (see build_native.ps1), which installs the manifest deps in vcpkg.json.
cmake_minimum_required(VERSION 3.21)
project(fsp_native LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

# Linkage smoke test: proves Arrow/Parquet resolve and link before porting the
# parquet writer / schema field-ids / stats reader / Avro manifests.
add_executable(native_smoke native_smoke.cpp)
target_link_libraries(native_smoke PRIVATE Arrow::arrow_shared Parquet::parquet_shared)

# Avro C++ target name varies by vcpkg port revision; wire it in once the smoke
# build succeeds, then add native_tests for the round-trip conformance harness.
# find_package(unofficial-avro-cpp CONFIG REQUIRED)
