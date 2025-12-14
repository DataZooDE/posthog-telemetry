# AI Integration Guide: Adding PostHog Telemetry to a DuckDB Extension

This guide provides step-by-step instructions for AI assistants or developers to integrate the `posthog-telemetry` library into a DuckDB extension.

## 1. Add the Submodule

Run the following command in the root of your extension repository:

```bash
git submodule add git@github.com:DataZooDE/posthog-telemetry.git posthog-telemetry
git submodule update --init --recursive
```

## 2. Update CMakeLists.txt

Open your extension's `CMakeLists.txt`. You need to add the subdirectory and link the library to your extension target.

**Example Modification:**

```cmake
# ... existing configuration ...

# Add the telemetry library
add_subdirectory(posthog-telemetry)

# Include directory (may be handled by target_link_libraries)
include_directories(posthog-telemetry/include)

# ... definition of your extension library ...
add_library(my_extension_extension STATIC src/my_extension_extension.cpp)

# Link the telemetry library
target_link_libraries(my_extension_extension PRIVATE posthog_telemetry)
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

// Define constants for your extension
#define MY_EXTENSION_NAME "my_extension"
#define MY_EXTENSION_VERSION "1.0.0"
#define MY_POSTHOG_API_KEY "phc_YOUR_PROJECT_API_KEY"

// ... inside your load function ...

void MyExtensionLoad(DuckDB &db) {
    // 1. Get the singleton instance
    auto& telemetry = duckdb::PostHogTelemetry::Instance();

    // 2. Configure the API Key
    // CRITICAL: Use the API Key specific to your PostHog project.
    // If sharing a project, the 'extension_name' property will differentiate.
    telemetry.SetAPIKey(MY_POSTHOG_API_KEY);

    // 3. Capture Load Event with extension name and version
    telemetry.CaptureExtensionLoad(MY_EXTENSION_NAME, MY_EXTENSION_VERSION);

    // 4. Register a Configuration Option
    // Allow users to disable telemetry via SET my_extension_telemetry_enabled = false;
    // This option is MANDATORY to respect user privacy.
    auto &config = DBConfig::GetConfig(db);
    config.AddExtensionOption(
        "my_extension_telemetry_enabled",
        "Enable or disable anonymous usage telemetry for my_extension",
        LogicalType::BOOLEAN,
        Value::BOOLEAN(true),
        [](ClientContext &context, SetScope scope, Value &parameter) {
            auto& telemetry = duckdb::PostHogTelemetry::Instance();
            telemetry.SetEnabled(BooleanValue::Get(parameter));
        }
    );

    // ... rest of your initialization ...
}
```

### Tracking Function Execution (Optional)

To track which functions are being used:

```cpp
static unique_ptr<FunctionData> MyFunctionBind(ClientContext &context, ...) {
    duckdb::PostHogTelemetry::Instance().CaptureFunctionExecution("my_function_name", MY_EXTENSION_VERSION);
    // ... rest of bind logic
}
```

## 5. Differentiating Extensions

Data sent to PostHog includes properties to filter by:

*   **`extension_name`**: The string passed to `CaptureExtensionLoad`.
*   **`extension_version`**: The version string passed to `CaptureExtensionLoad`.
*   **`extension_platform`**: Automatically detected using `DuckDB::Platform()`.

**Best Practice:**
Define the extension name and version as constants or macros to avoid typos.

```cpp
#define EXTENSION_NAME "spatial_analytics"
#define EXTENSION_VERSION "2.1.0"

// ...
telemetry.CaptureExtensionLoad(EXTENSION_NAME, EXTENSION_VERSION);
```

## 6. Disabling Telemetry

### Environment Variable

Users can disable all DataZoo extension telemetry by setting:

```bash
export DATAZOO_DISABLE_TELEMETRY=1
# or
export DATAZOO_DISABLE_TELEMETRY=true
# or
export DATAZOO_DISABLE_TELEMETRY=yes
```

This environment variable is checked at event send time and will silently skip sending events.

### DuckDB Setting

Telemetry is **enabled by default** to assist with development and usage tracking. However, you **must** provide a way for users to opt-out.

Use `DBConfig::AddExtensionOption` to register a SET variable (e.g., `my_extension_telemetry_enabled`). The callback function should update the telemetry singleton state.

**User Usage Example (DuckDB Shell):**
```sql
-- Disable telemetry for this session
SET my_extension_telemetry_enabled = false;
```

## 7. Verification

1.  **Build** the extension to ensure linker errors are resolved.
2.  **Load** the extension in DuckDB.
3.  **Check** your PostHog dashboard for the `extension_load` event.
4.  **Verify** the `extension_name` and `extension_version` properties match your configuration.

## 8. Troubleshooting

*   **Linker Errors**: Ensure `posthog_telemetry` is linked and that the DuckDB environment provides `httplib` and `openssl`.
*   **Missing Events**:
    *   Check if `SetAPIKey` was called before `CaptureExtensionLoad`.
    *   Check if the device has network access.
    *   Check if `DATAZOO_DISABLE_TELEMETRY` environment variable is set.
    *   Telemetry fails silently to avoid crashing the database; errors are logged to stderr.

## 9. Example: Complete Integration

```cpp
#include "telemetry.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"

#define EXTENSION_NAME "my_cool_extension"
#define EXTENSION_VERSION "1.2.3"
#define POSTHOG_API_KEY "phc_xxxxxxxxxxxxxxxxxxxxxxxxxxxx"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
    auto &instance = loader.GetDatabaseInstance();

    // Initialize telemetry
    auto& telemetry = PostHogTelemetry::Instance();
    telemetry.SetAPIKey(POSTHOG_API_KEY);
    telemetry.CaptureExtensionLoad(EXTENSION_NAME, EXTENSION_VERSION);

    // Register telemetry opt-out setting
    auto &config = DBConfig::GetConfig(instance);
    config.AddExtensionOption(
        "my_cool_extension_telemetry_enabled",
        "Enable anonymous usage telemetry",
        LogicalTypeId::BOOLEAN,
        Value(true),
        [](ClientContext &context, SetScope scope, Value &parameter) {
            PostHogTelemetry::Instance().SetEnabled(parameter.GetValue<bool>());
        }
    );

    // Register your extension functions here...
}

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void my_cool_extension_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::MyCoolExtension>();
}

DUCKDB_EXTENSION_API const char *my_cool_extension_version() {
    return EXTENSION_VERSION;
}
}
```
