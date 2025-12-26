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

# Include directories - use BUILD_INTERFACE to avoid export issues
# The library headers need to be PUBLIC so consumers can include telemetry.hpp
target_include_directories(posthog_telemetry PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

# Build-only includes - these are only needed for compilation, not for consumers
target_include_directories(posthog_telemetry PRIVATE
    ${OPENSSL_INCLUDE_DIR}
)

# Add DuckDB paths if found (standalone build) - PRIVATE since not needed by consumers
if(DUCKDB_SRC_INCLUDE)
    target_include_directories(posthog_telemetry PRIVATE
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
