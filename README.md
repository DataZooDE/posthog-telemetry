# DuckDB PostHog Telemetry Library

A reusable C++ library for integrating PostHog telemetry into DuckDB extensions. This library allows extensions to send anonymous usage data (extension load, function execution) to a configured PostHog instance.

## Features

- Asynchronous event queue (non-blocking) with real batch coalescing.
- Thread-safe singleton access.
- **Analysis-first schema (`telemetry_schema: 2`)**: a common envelope
  (`product`, `os`/`arch`, `is_ci`, `is_container`, `$session_id`, `$groups`, …)
  on every event; generalised `Capture`/`CaptureFeature`/`CaptureError`;
  aggregated `function_executed` (no per-call firehose); group analytics.
- Typed properties (`PropertyValue`): numbers and bools serialise as real JSON
  types for HogQL aggregation.
- Stable, pseudonymous `distinct_id` (SHA-256 machine id, MAC fallback).
- **Enabled by default**, with user opt-out via DuckDB settings or environment
  variable, enforced at the transport (nothing leaves the machine when disabled).
- Buffered events auto-send on a background interval (and at a size threshold);
  `Flush()` forces a synchronous drain for short-lived processes; the at-exit
  *discard* stays the safety net.
- Designed to be included as a git submodule; cross-language schema in
  [`TELEMETRY-SCHEMA.md`](TELEMETRY-SCHEMA.md).

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

Full event schema (envelope, event catalogue, per-product `feature` enums,
cardinality rule, opt-out contract) lives in
[`TELEMETRY-SCHEMA.md`](TELEMETRY-SCHEMA.md) — the single source of truth every
product vendors.

### PostHogTelemetry

```cpp
// Get singleton instance
static PostHogTelemetry& Instance();

// --- configuration ---
void SetProduct(const std::string& name, const std::string& version,
                const std::string& edition = "oss");   // envelope identity
void SetAPIKey(std::string new_key);                    // default: shared key
void SetHost(const std::string& host);                  // default eu.posthog.com
void SetSampling(double rate);                          // 0..1 for hot events
void SetEnabled(bool enabled);
bool IsEnabled();

// --- capture ---
void Capture(const std::string& event, PropertyMap props = {});      // general
void CaptureFeature(const std::string& feature, PropertyMap props = {});
void CaptureError(const std::string& error_class, PropertyMap props = {}); // -> $exception
void RecordFunctionCall(const std::string& fn, double duration_ms = 0);    // aggregated
void CaptureExtensionLoad(const std::string& extension_name,
                          const std::string& extension_version = "0.1.0");

// --- grouping (account-level analytics) ---
void AssociateGroup(const std::string& type, const std::string& key,
                    PropertyMap props = {});

// --- lifecycle ---
void Flush();   // synchronously drain buffered events before exit (bounded)
```

`PropertyMap` is `std::map<std::string, PropertyValue>`; `PropertyValue`
accepts `string`/`int`/`double`/`bool` and serialises numbers and bools as real
JSON types (so `is_ci`, `call_count`, `duration_ms` aggregate in HogQL).

### Migration to schema 2 (`2.0.0`)

The API is **additive** — existing embedders upgrade the library version
**without editing any call site**. `CaptureExtensionLoad` /
`CaptureFunctionExecution` keep working (they dual-emit the legacy
`extension_load` / `function_execution` names for one release). To adopt the new
analysis features:

1. **Call `SetProduct(name, version, edition)`** once at startup so events carry
   a stable `product` identity (otherwise it falls back to the extension name).
2. Replace per-call `CaptureFunctionExecution` with `RecordFunctionCall` to get
   aggregated `function_executed` events instead of a per-call firehose.
3. Emit `CaptureFeature(...)` / `CaptureError(...)` for your top capabilities and
   failure modes; associate an `account` group for enterprise builds.
4. Call `Flush()` before exit in short-lived processes/CLIs.

> **Host default:** stays `https://eu.posthog.com`. Flip to
> `https://eu.i.posthog.com` (PostHog's EU *ingestion* host) via `SetHost(...)`
> once verified against the project; the default will move in a later release.

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

Every event carries the common **envelope** (`product`, `product_version`,
`product_edition`, `duckdb_version`, `os`, `arch`, `platform`, `is_ci`,
`is_container`, `telemetry_schema`, `$session_id`, and `$groups` once a group is
associated), plus `distinct_id` (stable pseudonymous machine hash) and an
ISO8601 `timestamp`.

Event-specific properties and the full catalogue are documented in
[`TELEMETRY-SCHEMA.md`](TELEMETRY-SCHEMA.md). In brief:

| Event | Key properties |
|---|---|
| `extension_loaded` (+ legacy `extension_load`) | `extension_name`, `extension_version`, `extension_platform` |
| `feature_used` | `feature`, `feature_detail`, `duration_ms` |
| `function_executed` (aggregated; legacy `function_execution` retired) | `function_name`, `call_count`, `duration_ms_p50`, `extension_name`, `sample_rate?` |
| `$exception` | `error_class` (enum), `feature`, `phase` |
| `$groupidentify` | `$group_type`, `$group_key`, `$group_set` |

## License

MIT License. See [LICENSE](LICENSE) file.
