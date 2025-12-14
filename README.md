# DuckDB PostHog Telemetry Library

A reusable C++ library for integrating PostHog telemetry into DuckDB extensions. This library allows extensions to send anonymous usage data (extension load, function execution) to a configured PostHog instance.

## Features

- Asynchronous event queue (non-blocking).
- Thread-safe singleton access.
- Captures `extension_load` and `function_execution` events.
- Platform detection using `DuckDB::Platform()`.
- Platform-specific metadata (e.g., MAC address for anonymous identification).
- **Enabled by default**, with support for user opt-out via DuckDB settings or environment variable.
- Designed to be included as a git submodule.

## Dependencies

This library depends on the DuckDB extension environment, specifically:
- `duckdb` (headers)
- `duckdb_httplib_openssl` (HTTP client with SSL)

These are typically available in the DuckDB extension build system.

## Project Structure

```
.
├── include
│   └── telemetry.hpp    # Public header
├── src
│   └── telemetry.cpp    # Implementation
├── CMakeLists.txt       # Build configuration
├── LICENSE              # MIT License
├── AI_INTEGRATION_GUIDE.md  # Step-by-step integration guide
└── README.md            # This file
```

## Usage

1.  **Add as Submodule**:
    ```bash
    git submodule add git@github.com:DataZooDE/posthog-telemetry.git posthog-telemetry
    git submodule update --init --recursive
    ```

2.  **Configure Build**:
    Add `add_subdirectory(posthog-telemetry)` and link `posthog_telemetry` to your extension target.

    ```cmake
    add_subdirectory(posthog-telemetry)
    target_link_libraries(your_extension PRIVATE posthog_telemetry)
    ```

3.  **Code Integration**:
    Initialize and use the telemetry in your extension entry point.

    ```cpp
    #include "telemetry.hpp"

    // In your extension load function
    auto& telemetry = duckdb::PostHogTelemetry::Instance();
    telemetry.SetAPIKey("YOUR_POSTHOG_API_KEY");
    telemetry.CaptureExtensionLoad("your_extension_name", "1.0.0");
    ```

## API Reference

### PostHogTelemetry

```cpp
// Get singleton instance
static PostHogTelemetry& Instance();

// Set PostHog API key (required before capturing events)
void SetAPIKey(std::string new_key);

// Enable/disable telemetry
void SetEnabled(bool enabled);
bool IsEnabled();

// Capture extension load event
void CaptureExtensionLoad(const std::string& extension_name,
                          const std::string& extension_version = "0.1.0");

// Capture function execution event
void CaptureFunctionExecution(const std::string& function_name,
                              const std::string& function_version = "0.1.0");
```

## Disabling Telemetry

Users can disable telemetry in multiple ways:

### Environment Variable

Set `DATAZOO_DISABLE_TELEMETRY` to disable all telemetry:

```bash
export DATAZOO_DISABLE_TELEMETRY=1
# or
export DATAZOO_DISABLE_TELEMETRY=true
```

### DuckDB Setting

Extensions should register a DuckDB setting for user opt-out:

```sql
SET your_extension_telemetry_enabled = false;
```

See `AI_INTEGRATION_GUIDE.md` for implementation details.

## Event Properties

### extension_load
- `extension_name`: Name of the extension
- `extension_version`: Version string
- `extension_platform`: Detected platform from `DuckDB::Platform()`

### function_execution
- `function_name`: Name of the function called
- `function_version`: Version string

All events include:
- `distinct_id`: Anonymized MAC address for user identification
- `timestamp`: ISO8601 formatted timestamp

## License

MIT License. See [LICENSE](LICENSE) file.
