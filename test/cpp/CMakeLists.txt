# PostHog Telemetry Unit Tests using Catch2
# Supports both:
# 1. Standalone build (CI) - DuckDB checked out to ${CMAKE_SOURCE_DIR}/duckdb
# 2. Submodule in erpl - DuckDB at ${CMAKE_SOURCE_DIR}/duckdb (erpl's root)
# 3. Build from erpl where CMAKE_SOURCE_DIR is duckdb itself

# Note: OpenSSL is found and linked via posthog_telemetry target (PUBLIC dependency)

# Find DuckDB headers - try multiple locations
if(EXISTS "${CMAKE_SOURCE_DIR}/third_party/catch")
    # When CMAKE_SOURCE_DIR is the DuckDB directory itself (erpl build)
    set(DUCKDB_THIRD_PARTY "${CMAKE_SOURCE_DIR}/third_party")
    set(DUCKDB_SRC_INCLUDE "${CMAKE_SOURCE_DIR}/src/include")
    message(STATUS "Found DuckDB at ${CMAKE_SOURCE_DIR}")
elseif(EXISTS "${CMAKE_SOURCE_DIR}/duckdb/third_party/catch")
    # Both standalone CI and erpl submodule use this path structure
    set(DUCKDB_THIRD_PARTY "${CMAKE_SOURCE_DIR}/duckdb/third_party")
    set(DUCKDB_SRC_INCLUDE "${CMAKE_SOURCE_DIR}/duckdb/src/include")
    message(STATUS "Found DuckDB at ${CMAKE_SOURCE_DIR}/duckdb")
else()
    message(FATAL_ERROR "Cannot find DuckDB headers. Please ensure DuckDB is available at ${CMAKE_SOURCE_DIR}/duckdb or ${CMAKE_SOURCE_DIR}")
endif()

# Find DuckDB library for standalone builds
# DUCKDB_LIB_DIR can be set via cmake -DDUCKDB_LIB_DIR=path
if(DUCKDB_LIB_DIR)
    message(STATUS "Looking for DuckDB library in ${DUCKDB_LIB_DIR}")
    find_library(DUCKDB_LIBRARY
        NAMES duckdb libduckdb
        PATHS ${DUCKDB_LIB_DIR}
        NO_DEFAULT_PATH
    )
    if(DUCKDB_LIBRARY)
        message(STATUS "Found DuckDB library: ${DUCKDB_LIBRARY}")
    else()
        message(FATAL_ERROR "DuckDB library not found in ${DUCKDB_LIB_DIR}")
    endif()
endif()

# Include directories (OpenSSL includes inherited from posthog_telemetry target)
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/../../include
    ${DUCKDB_THIRD_PARTY}/catch
    ${DUCKDB_THIRD_PARTY}/httplib
    ${DUCKDB_THIRD_PARTY}
    ${DUCKDB_SRC_INCLUDE}
)

# Also include DuckDB lib headers if provided
if(DUCKDB_LIB_DIR)
    include_directories(${DUCKDB_LIB_DIR})
endif()

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
)

# Link DuckDB library for standalone builds
if(DUCKDB_LIBRARY)
    target_link_libraries(posthog_telemetry_tests ${DUCKDB_LIBRARY})
endif()

# Platform-specific libraries
if(WIN32)
    target_link_libraries(posthog_telemetry_tests iphlpapi ws2_32)
endif()

# Enable threading
find_package(Threads REQUIRED)
target_link_libraries(posthog_telemetry_tests Threads::Threads)
