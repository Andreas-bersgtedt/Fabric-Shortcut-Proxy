# Native Tier 1 modules (Arrow/Parquet + Avro). Requires vcpkg-provided Apache
# Arrow C++ (parquet feature) and Avro C++. Configure through the vcpkg toolchain
# (see build_native.ps1), which installs the manifest deps in vcpkg.json.
cmake_minimum_required(VERSION 3.21)
project(fsp_native LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

# Avro C++ for the Iceberg manifest list / manifest file writers.
# NOTE: avro-cpp's vcpkg CMake config is broken in the dynamic triplet: it
# unconditionally aliases avro-cpp::avrocpp_static -> avro-cpp::avrocpp_s, which
# does not exist in a shared build, so find_package(avro-cpp) errors. Link the
# installed import lib directly instead. All vcpkg headers (avro, boost, fmt)
# live under the same include root.
set(_avro_root "${CMAKE_BINARY_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}")
find_path(AVRO_INCLUDE_DIR NAMES avro/DataFile.hh PATHS "${_avro_root}/include" NO_DEFAULT_PATH REQUIRED)
find_library(AVRO_LIB_RELEASE NAMES avrocpp PATHS "${_avro_root}/lib" NO_DEFAULT_PATH REQUIRED)
find_library(AVRO_LIB_DEBUG NAMES avrocpp PATHS "${_avro_root}/debug/lib" NO_DEFAULT_PATH)
add_library(avrocpp_local INTERFACE)
target_include_directories(avrocpp_local INTERFACE "${AVRO_INCLUDE_DIR}")
if(AVRO_LIB_DEBUG)
    target_link_libraries(avrocpp_local INTERFACE optimized "${AVRO_LIB_RELEASE}" debug "${AVRO_LIB_DEBUG}")
else()
    target_link_libraries(avrocpp_local INTERFACE "${AVRO_LIB_RELEASE}")
endif()

# Linkage smoke test: proves Arrow/Parquet resolve and link.
add_executable(native_smoke native_smoke.cpp)
target_link_libraries(native_smoke PRIVATE Arrow::arrow_shared Parquet::parquet_shared)

# Probe: verify avro-cpp preserves Iceberg `field-id` custom schema attributes in
# the OCF header (pyiceberg needs them to map manifest columns).
add_executable(avro_probe avro_probe.cpp)
target_link_libraries(avro_probe PRIVATE avrocpp_local)
# avro-cpp includes fmt, which static_asserts unless compiled with /utf-8 on MSVC.
target_compile_options(avro_probe PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/utf-8>)

# Round-trip harness: Arrow schema (field-ids) + Parquet writer + stats reader +
# Avro manifest list / manifest file. Reuses the tier1 headers (IcebergColumn +
# Iceberg bound encoders).
add_executable(native_tests native_tests.cpp)
target_include_directories(native_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../tier1)
target_link_libraries(native_tests PRIVATE Arrow::arrow_shared Parquet::parquet_shared avrocpp_local)
target_compile_options(native_tests PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/utf-8>)

# Native serving-image publisher: assembles a full Iceberg table image (Parquet
# splits + Avro manifests + metadata.json) into a store dir for the C++ agent.
add_executable(native_publish native_publish.cpp)
target_include_directories(native_publish PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../tier1)
target_link_libraries(native_publish PRIVATE Arrow::arrow_shared Parquet::parquet_shared avrocpp_local)
target_compile_options(native_publish PRIVATE $<$<CXX_COMPILER_ID:MSVC>:/utf-8>)
