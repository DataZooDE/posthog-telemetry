# DuckDB PostHog Telemetry Library

A reusable C++ library for integrating PostHog telemetry into DuckDB extensions. This library allows extensions to send anonymous usage data (extension load, function execution) to a configured PostHog instance.

## Features

- Asynchronous event queue (non-blocking).
- Thread-safe singleton access.
- Captures `extension_load` and `function_execution` events.
- Platform-specific metadata (e.g., MAC address for anonymous identification).
- Designed to be included as a git submodule.

## Dependencies

This library depends on the DuckDB extension environment, specifically:
- `duckdb` (headers)
- `duckdb_re2` (regex)
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
└── README.md            # This file
```

## Usage

1.  **Add as Submodule**:
    ```bash
    git submodule add <repo-url> lib-posthog
    ```

2.  **Configure Build**:
    Add `add_subdirectory(lib-posthog)` and link `posthog_telemetry` to your extension target.

3.  **Code Integration**:
    Initialize and use the telemetry in your extension entry point.

    ```cpp
    #include "telemetry.hpp"

    // In your extension load function
    auto& telemetry = duckdb::PostHogTelemetry::Instance();
    telemetry.SetAPIKey("YOUR_POSTHOG_API_KEY");
    telemetry.CaptureExtensionLoad("your_extension_name");
    ```

## License

MIT License. See [LICENSE](LICENSE) file.

