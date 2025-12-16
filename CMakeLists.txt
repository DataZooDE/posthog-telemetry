cmake_minimum_required(VERSION 3.10)

project(posthog_telemetry)

# OpenSSL is required for HTTPS support in httplib
find_package(OpenSSL REQUIRED)

# For standalone build (CI), find DuckDB headers
# When used as submodule in a DuckDB extension, these paths come from parent project
if(EXISTS "${CMAKE_SOURCE_DIR}/duckdb/src/include")
    set(DUCKDB_SRC_INCLUDE "${CMAKE_SOURCE_DIR}/duckdb/src/include")
    set(DUCKDB_THIRD_PARTY "${CMAKE_SOURCE_DIR}/duckdb/third_party")
    message(STATUS "Found DuckDB headers at ${CMAKE_SOURCE_DIR}/duckdb")
endif()

# Define the library
add_library(posthog_telemetry STATIC
    src/telemetry.cpp
)

# Include directories
target_include_directories(posthog_telemetry PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${OPENSSL_INCLUDE_DIR}
)

# Add DuckDB paths if found (standalone build)
if(DUCKDB_SRC_INCLUDE)
    target_include_directories(posthog_telemetry PUBLIC
        ${DUCKDB_SRC_INCLUDE}
        ${DUCKDB_THIRD_PARTY}
        ${DUCKDB_THIRD_PARTY}/httplib
    )
endif()

# Link OpenSSL libraries
target_link_libraries(posthog_telemetry PUBLIC ${OPENSSL_LIBRARIES})

if (WIN32)
    target_link_libraries(posthog_telemetry PUBLIC iphlpapi)
endif()

# Standard C++17
target_compile_features(posthog_telemetry PUBLIC cxx_std_17)

# Unit tests (optional)
option(BUILD_UNITTESTS "Build PostHog Telemetry Unit Tests." OFF)
if(${BUILD_UNITTESTS})
    add_subdirectory(test)
endif()
