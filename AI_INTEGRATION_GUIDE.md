# AI Integration Guide: Adding PostHog Telemetry to a DuckDB Extension

This guide provides step-by-step instructions for AI assistants or developers to integrate the `lib-posthog` telemetry library into a DuckDB extension.

## 1. Add the Submodule

Run the following command in the root of your extension repository:

```bash
git submodule add <URL_TO_THIS_REPO> lib-posthog
git submodule update --init --recursive
```

## 2. Update CMakeLists.txt

Open your extension's `CMakeLists.txt`. You need to add the subdirectory and link the library to your extension target.

**Example Modification:**

```cmake
# ... existing configuration ...

# Add the telemetry library
add_subdirectory(lib-posthog)

# ... definition of your extension library ...
add_library(my_extension_extension STATIC src/my_extension_extension.cpp)

# Link the telemetry library
target_link_libraries(my_extension_extension PRIVATE posthog_telemetry)

# Ensure includes are visible (usually handled by target_link_libraries, but can be explicit)
target_include_directories(my_extension_extension PRIVATE lib-posthog/include)
```

## 3. Update Makefile (If Applicable)

If your project uses a `Makefile` that wraps CMake (common in DuckDB extensions), usually no changes are needed if you modify `CMakeLists.txt`. However, ensure that `git submodule update` is part of your build or setup process.

```makefile
# Example: Add a target to init submodules
init:
	git submodule update --init --recursive
```

## 4. Configure and Use Telemetry

To ensure each extension is differentiable in PostHog, you **must** configure a unique **Extension Name** and **API Key** (if using separate projects) or use the same API Key and rely on the extension name property.

### Integration in C++

Locate your extension's entry point (usually `extension_load` or similar).

```cpp
#include "telemetry.hpp"

// ... inside your load function ...

void MyExtensionLoad(DuckDB &db) {
    // 1. Get the singleton instance
    auto& telemetry = duckdb::PostHogTelemetry::Instance();

    // 2. Configure the API Key
    // CRITICAL: Use the API Key specific to your PostHog project.
    // If sharing a project, the 'extension_name' property will differentiate.
    telemetry.SetAPIKey("phc_YOUR_PROJECT_API_KEY");

    // 3. Capture Load Event
    // CRITICAL: Provide the unique name of this extension.
    telemetry.CaptureExtensionLoad("my_unique_extension_name");

    // ... rest of your initialization ...
}
```

### Differentiating Extensions

Data sent to PostHog includes properties to filter by:

*   **`extension_name`**: The string passed to `CaptureExtensionLoad`.
*   **`extension_version`**: Currently defaults to "0.1.0" (can be modified in source or enhanced in future versions).
*   **`extension_platform`**: Automatically detected (linux, windows, macos).

**Best Practice:**
Define the extension name as a constant or macro to avoid typos.

```cpp
#define MY_EXTENSION_NAME "spatial_analytics"

// ...
telemetry.CaptureExtensionLoad(MY_EXTENSION_NAME);
```

## 5. Verification

1.  **Build** the extension to ensure linker errors are resolved.
2.  **Load** the extension in DuckDB.
3.  **Check** your PostHog dashboard for the `extension_load` event.
4.  **Verify** the `extension_name` property matches your configuration.

## 6. Troubleshooting

*   **Linker Errors**: Ensure `posthog_telemetry` is linked and that the DuckDB environment provides `httplib` and `openssl`.
*   **Missing Events**: 
    *   Check if `SetAPIKey` was called.
    *   Check if the device has network access.
    *   Telemetry fails silently to avoid crashing the database; check `src/telemetry.cpp` debug prints if you enable them manually.

