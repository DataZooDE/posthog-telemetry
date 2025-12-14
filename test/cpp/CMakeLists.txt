# PostHog Telemetry Unit Tests using Catch2
# This assumes the library is used as a submodule in a DuckDB extension project

find_package(OpenSSL REQUIRED)

# Include directories - these paths work when used as submodule in erpl
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/../../include
    ${CMAKE_SOURCE_DIR}/duckdb/third_party/catch
    ${CMAKE_SOURCE_DIR}/duckdb/third_party/httplib
    ${CMAKE_SOURCE_DIR}/duckdb/third_party
    ${CMAKE_SOURCE_DIR}/duckdb/src/include
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
