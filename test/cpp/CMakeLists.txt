# PostHog Telemetry Unit Tests using Catch2
# DuckDB source (headers) are fetched by the root CMakeLists.txt.
# DuckDB prebuilt library (for duckdb_re2, exception types) is fetched here.

include(FetchContent)

# Platform-specific DuckDB prebuilt library
if(WIN32)
    set(DUCKDB_LIB_ZIP "libduckdb-windows-amd64.zip")
elseif(APPLE)
    set(DUCKDB_LIB_ZIP "libduckdb-osx-universal.zip")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
    set(DUCKDB_LIB_ZIP "libduckdb-linux-aarch64.zip")
else()
    set(DUCKDB_LIB_ZIP "libduckdb-linux-amd64.zip")
endif()

FetchContent_Declare(duckdb_prebuilt
    URL https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/${DUCKDB_LIB_ZIP}
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_GetProperties(duckdb_prebuilt)
if(NOT duckdb_prebuilt_POPULATED)
    FetchContent_Populate(duckdb_prebuilt)
endif()

find_library(DUCKDB_LIBRARY
    NAMES duckdb libduckdb
    PATHS ${duckdb_prebuilt_SOURCE_DIR}
    NO_DEFAULT_PATH
    REQUIRED
)
message(STATUS "Found DuckDB library: ${DUCKDB_LIBRARY}")

# Include directories (OpenSSL includes inherited via posthog_telemetry target)
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/../../include
    ${DUCKDB_THIRD_PARTY}/catch
)

set(TEST_SOURCES
    test_main.cpp
    test_event.cpp
    test_queue.cpp
    test_telemetry.cpp
    test_error_handling.cpp
    test_application_lifecycle.cpp
)

add_executable(posthog_telemetry_tests ${TEST_SOURCES})

target_link_libraries(posthog_telemetry_tests
    posthog_telemetry
    ${DUCKDB_LIBRARY}
)

# On Windows, copy duckdb.dll next to the test binary at build time
if(WIN32)
    add_custom_command(TARGET posthog_telemetry_tests POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${duckdb_prebuilt_SOURCE_DIR}/duckdb.dll"
            "$<TARGET_FILE_DIR:posthog_telemetry_tests>"
    )
endif()

# Platform-specific libraries
if(WIN32)
    target_link_libraries(posthog_telemetry_tests ws2_32)
endif()

# Enable threading
find_package(Threads REQUIRED)
target_link_libraries(posthog_telemetry_tests Threads::Threads)
