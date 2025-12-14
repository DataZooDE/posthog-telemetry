# PostHog Telemetry Unit Tests using Catch2
# Supports both:
# 1. Standalone build (CI) - DuckDB checked out to ${CMAKE_SOURCE_DIR}/duckdb
# 2. Submodule in erpl - DuckDB at ${CMAKE_SOURCE_DIR}/duckdb (erpl's root)

find_package(OpenSSL REQUIRED)

# Find DuckDB headers - try multiple locations
if(EXISTS "${CMAKE_SOURCE_DIR}/duckdb/third_party/catch")
    # Both standalone CI and erpl submodule use this path structure
    set(DUCKDB_THIRD_PARTY "${CMAKE_SOURCE_DIR}/duckdb/third_party")
    set(DUCKDB_SRC_INCLUDE "${CMAKE_SOURCE_DIR}/duckdb/src/include")
    message(STATUS "Found DuckDB at ${CMAKE_SOURCE_DIR}/duckdb")
else()
    message(FATAL_ERROR "Cannot find DuckDB headers. Please ensure DuckDB is available at ${CMAKE_SOURCE_DIR}/duckdb")
endif()

# Include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/../../include
    ${DUCKDB_THIRD_PARTY}/catch
    ${DUCKDB_THIRD_PARTY}/httplib
    ${DUCKDB_THIRD_PARTY}
    ${DUCKDB_SRC_INCLUDE}
    ${OPENSSL_INCLUDE_DIR}
)

set(TEST_SOURCES
    test_main.cpp
    test_event.cpp
    test_queue.cpp
    test_telemetry.cpp
    test_error_handling.cpp
)

add_executable(posthog_telemetry_tests ${TEST_SOURCES})

target_link_libraries(posthog_telemetry_tests
    posthog_telemetry
    ${OPENSSL_LIBRARIES}
)

# Platform-specific libraries
if(WIN32)
    target_link_libraries(posthog_telemetry_tests iphlpapi ws2_32)
endif()

# Enable threading
find_package(Threads REQUIRED)
target_link_libraries(posthog_telemetry_tests Threads::Threads)
