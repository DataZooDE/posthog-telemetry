cmake_minimum_required(VERSION 3.14)
cmake_policy(SET CMP0169 OLD)  # Allow FetchContent_Populate for header-only fetches

project(posthog_telemetry)

# OpenSSL is required for HTTPS support in httplib
find_package(OpenSSL REQUIRED)

set(DUCKDB_VERSION "v1.4.3")

# In standalone builds, fetch DuckDB source for headers (httplib, re2, duckdb.hpp, etc.)
# In submodule builds (DuckDB extension), the parent project provides DuckDB headers.
if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    include(FetchContent)
    message(STATUS "Standalone build: fetching DuckDB ${DUCKDB_VERSION} source for headers")
    FetchContent_Declare(duckdb_source
        URL https://github.com/duckdb/duckdb/archive/refs/tags/${DUCKDB_VERSION}.tar.gz
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    FetchContent_GetProperties(duckdb_source)
    if(NOT duckdb_source_POPULATED)
        FetchContent_Populate(duckdb_source)
    endif()
    set(DUCKDB_SRC_INCLUDE "${duckdb_source_SOURCE_DIR}/src/include")
    set(DUCKDB_THIRD_PARTY "${duckdb_source_SOURCE_DIR}/third_party")
endif()

# Define the library
add_library(posthog_telemetry STATIC
    src/telemetry.cpp
)

# Include directories - use BUILD_INTERFACE to avoid export issues
# The library headers need to be PUBLIC so consumers can include telemetry.hpp
target_include_directories(posthog_telemetry PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

# Build-only includes - these are only needed for compilation, not for consumers
target_include_directories(posthog_telemetry PRIVATE
    ${OPENSSL_INCLUDE_DIR}
)

# Add DuckDB paths when available (standalone fetches above; submodule gets from parent)
# PRIVATE: consumers only need telemetry.hpp, not DuckDB's internal headers
if(DEFINED DUCKDB_SRC_INCLUDE)
    target_include_directories(posthog_telemetry PRIVATE
        ${DUCKDB_SRC_INCLUDE}
        ${DUCKDB_THIRD_PARTY}
        ${DUCKDB_THIRD_PARTY}/httplib
    )
endif()

# Link OpenSSL libraries
target_link_libraries(posthog_telemetry PUBLIC ${OPENSSL_LIBRARIES})

if(WIN32)
    target_link_libraries(posthog_telemetry PUBLIC iphlpapi)
endif()

if(APPLE)
    target_link_libraries(posthog_telemetry PRIVATE
        "-framework IOKit"
        "-framework CoreFoundation")
endif()

# Standard C++17
target_compile_features(posthog_telemetry PUBLIC cxx_std_17)

# Install target for DuckDB export set integration
if(DEFINED DUCKDB_EXPORT_SET)
    install(
        TARGETS posthog_telemetry
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
    )
endif()

# Unit tests (optional) - use unique option name to avoid conflict with parent projects
option(POSTHOG_BUILD_TESTS "Build PostHog Telemetry Unit Tests." OFF)
if(${POSTHOG_BUILD_TESTS})
    add_subdirectory(test)
endif()
