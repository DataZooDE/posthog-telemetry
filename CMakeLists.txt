cmake_minimum_required(VERSION 3.10)

project(posthog_telemetry)

# Define the library
add_library(posthog_telemetry STATIC
    src/telemetry.cpp
)

# Include directories
target_include_directories(posthog_telemetry PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# We assume DuckDB and its dependencies (RE2, OpenSSL, etc.) are available 
# in the parent project's scope or standard paths. 
# This library is intended to be statically linked into a DuckDB extension.

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
