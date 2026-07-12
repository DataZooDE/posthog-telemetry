#pragma once

// Telemetry is compiled in by default. Consumers whose build cannot provide
// the real implementation (e.g. MinGW lanes where vcpkg cannot build OpenSSL)
// define POSTHOG_TELEMETRY_DISABLED on the TUs that call telemetry: every call
// then compiles to an inline no-op and telemetry.cpp must not be compiled.
#if !defined(POSTHOG_TELEMETRY_DISABLED)

#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>
#include <map>
#include <set>
#include <mutex>
#include <thread>
#include <queue>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <atomic>

namespace duckdb {

// A single property value that serialises to the correct JSON type.
// Numbers and bools are emitted *unquoted* so PostHog treats them as real
// numeric/boolean fields (needed for HogQL sum()/avg() over call_count,
// duration_ms, and for filtering on is_ci). The implicit string ctors keep
// every existing map<string,string>-style call site (`{"k","v"}`,
// `props["k"] = "v"`) compiling unchanged.
struct PropertyValue {
    // Json = an already-serialised JSON fragment emitted verbatim (used for
    // nested objects like $groups / $group_set that the scalar kinds can't hold).
    enum class Kind { String, Int, UInt, Double, Bool, Json } kind;
    std::string s;
    int64_t i = 0;
    uint64_t u = 0;
    double d = 0;
    bool b = false;

    PropertyValue() : kind(Kind::String) {}                 // for map::operator[]
    PropertyValue(const char* v) : kind(Kind::String), s(v ? v : "") {}
    PropertyValue(std::string v) : kind(Kind::String), s(std::move(v)) {}
    // One ctor for every integral or enum type (int, long, size_t, uint64_t,
    // unscoped enums, …) so no caller hits an ambiguous overload or a lost
    // enum→int promotion; bool has its own ctor below. Unsigned values that
    // don't fit in int64_t keep an UInt kind so they don't serialise negative.
    template <typename T,
              typename std::enable_if<(std::is_integral<T>::value ||
                                       std::is_enum<T>::value) &&
                                          !std::is_same<T, bool>::value,
                                      int>::type = 0>
    PropertyValue(T v) {
        if (std::is_unsigned<T>::value &&
            static_cast<uint64_t>(v) > static_cast<uint64_t>(INT64_MAX)) {
            kind = Kind::UInt;
            u = static_cast<uint64_t>(v);
        } else {
            kind = Kind::Int;
            i = static_cast<int64_t>(v);
        }
    }
    PropertyValue(double v) : kind(Kind::Double), d(v) {}
    PropertyValue(bool v) : kind(Kind::Bool), b(v) {}

    // Named ctor for a raw, already-valid JSON fragment (object/array/etc.).
    static PropertyValue Json(std::string raw) {
        PropertyValue v;
        v.kind = Kind::Json;
        v.s = std::move(raw);
        return v;
    }

    // Serialise this value as a JSON token (string escaped+quoted; number/bool
    // bare; Json verbatim). Never throws.
    std::string ToJson() const;
};

using PropertyMap = std::map<std::string, PropertyValue>;

struct PostHogEvent {
    std::string event_name;
    std::string distinct_id;
    PropertyMap properties;
    std::string timestamp;   // ISO8601, stamped at capture time; empty = stamp at send

    std::string GetPropertiesJson() const;
    std::string GetNowISO8601() const;
};

// Free function for processing events (exposed for testing). Single-event
// convenience: POSTs one event to the default host as a batch of one.
void PostHogProcess(const std::string api_key, const PostHogEvent &event);

// Coalesced transport: POST N events to `host` + "/batch/" as one request.
void PostHogProcessBatch(const std::string &api_key, const std::string &host,
                         const std::vector<PostHogEvent> &events);

// Simple thread-safe task queue for background telemetry processing
template<typename T>
class TelemetryTaskQueue {
public:
    using TaskFunction = std::function<void(T)>;

    TelemetryTaskQueue() : stop_processing(false) {
        worker_thread = std::thread(&TelemetryTaskQueue::ProcessQueue, this);
    }

    ~TelemetryTaskQueue() {
        Stop();
    }

    void EnqueueTask(TaskFunction task, T data) {
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            if (stop_processing) {
                return;
            }
            tasks.push({task, data});
        }
        condition.notify_one();
    }

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            stop_processing = true;
            // Discard pending tasks instead of draining them: Stop() runs from
            // the atexit shutdown handler, and executing queued tasks there
            // starts new HTTPS requests whose httplib function-local statics
            // (URL-parsing regexes) may already be destroyed at that point.
            std::queue<QueueItem>().swap(tasks);
        }
        condition.notify_all();
        idle_condition.notify_all();
        if (worker_thread.joinable()) {
            worker_thread.join();
        }
    }

    // Block until the queue is empty and no task is in flight, or until
    // timeout_ms elapses. Returns true if the queue drained, false on timeout.
    // Used by PostHogTelemetry::Flush() for a bounded synchronous drain.
    bool DrainFor(int timeout_ms) {
        std::unique_lock<std::mutex> lock(queue_mutex);
        return idle_condition.wait_for(
            lock, std::chrono::milliseconds(timeout_ms),
            [this] { return stop_processing || (tasks.empty() && !task_in_flight); });
    }

private:
    struct QueueItem {
        TaskFunction task;
        T data;
    };

    void ProcessQueue() {
        while (true) {
            QueueItem item;
            {
                std::unique_lock<std::mutex> lock(queue_mutex);
                condition.wait(lock, [this] { return stop_processing || !tasks.empty(); });

                // Exit as soon as a stop is requested; Stop() has already
                // discarded whatever was still queued.
                if (stop_processing) {
                    return;
                }

                if (!tasks.empty()) {
                    item = tasks.front();
                    tasks.pop();
                    task_in_flight = true;
                } else {
                    continue;
                }
            }

            try {
                item.task(item.data);
            } catch (...) {
                // Swallowing exceptions to prevent thread crash
            }

            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                task_in_flight = false;
                if (tasks.empty()) {
                    idle_condition.notify_all();
                }
            }
        }
    }

    std::queue<QueueItem> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::condition_variable idle_condition;
    std::thread worker_thread;
    bool stop_processing;
    bool task_in_flight = false;
};

class PostHogTelemetry {
public:
    static PostHogTelemetry& Instance();

    // Delete copy constructor and assignment operator
    PostHogTelemetry(const PostHogTelemetry&) = delete;
    PostHogTelemetry& operator=(const PostHogTelemetry&) = delete;

    // Identify the product for the analysis envelope. Attached to every event
    // as product / product_version / product_edition. If never called, product
    // falls back to the extension name from CaptureExtensionLoad.
    void SetProduct(const std::string& name, const std::string& version,
                    const std::string& edition = "oss");

    // Capture extension load event with name and version
    // Also stores extension_name as default for CaptureFunctionExecution
    void CaptureExtensionLoad(const std::string& extension_name,
                              const std::string& extension_version = "0.1.0");

    // Capture application lifecycle events
    void CaptureApplicationStart(const std::string& app_name,
                                  const std::string& app_version);
    void CaptureApplicationStop(const std::string& app_name,
                                 const std::string& app_version);

    // Generalised capture. The single choke point that reads the enabled flag
    // and opt-out env; all the typed convenience wrappers route through it.
    void Capture(const std::string& event, PropertyMap props = {});
    // Emits `feature_used` with a bounded, enumerated `feature` value.
    void CaptureFeature(const std::string& feature, PropertyMap props = {});
    // Emits `$exception` with an enumerated `error_class`. Pass ONLY a class
    // from a caller-controlled enum — never a free-form message or user data.
    void CaptureError(const std::string& error_class, PropertyMap props = {});

    // Associate this run with a group (e.g. type="deployment"/"account",
    // key=bounded non-PII id). The first association per (type,key) emits a
    // `$groupidentify`; afterwards every event carries `$groups` in its
    // envelope, enabling account-level analytics. ≤ 5 group types per project.
    void AssociateGroup(const std::string& type, const std::string& key,
                        PropertyMap props = {});

    // Capture function execution event - two overloads:
    // 1. Explicit extension_name
    void CaptureFunctionExecution(const std::string& function_name,
                                  const std::string& extension_name,
                                  const std::string& function_version);

    // 2. Uses stored default extension_name (from CaptureExtensionLoad or SetExtensionName)
    void CaptureFunctionExecution(const std::string& function_name,
                                  const std::string& function_version = "0.1.0");

    // Record a single function call into the in-process aggregator. Millions of
    // calls collapse into one `function_executed` event per function (carrying
    // call_count and duration_ms_p50), flushed on Flush()/session end — this is
    // what tames the per-call firehose. Cheap and lock-guarded.
    void RecordFunctionCall(const std::string& function_name,
                            double duration_ms = 0);

    // Client-side sampling for still-hot events: rate in [0,1]. Recorded events
    // are decimated and stamped with sample_rate so counts scale back up.
    void SetSampling(double rate);

    // Set/get default extension name for the instance
    void SetExtensionName(const std::string& name);
    std::string GetExtensionName();

    bool IsEnabled();
    void SetEnabled(bool enabled);

    std::string GetAPIKey();
    void SetAPIKey(std::string new_key);

    // Ingestion host, e.g. "https://eu.i.posthog.com" (EU) or a self-hosted
    // URL. PostHogProcessBatch posts to host + "/batch/".
    void SetHost(const std::string& host);
    std::string GetHost();

    // Coalesce and synchronously send all buffered events (and drain the
    // function aggregator), blocking up to a bounded timeout. CLIs/servers call
    // this before exit so short runs don't lose events. The at-exit *discard*
    // stays the default safety net; Flush() is the explicit opt-in drain.
    void Flush();

    // Testing seam: intercept the transport so tests can count /batch/ POSTs and
    // inspect coalesced payloads without any network I/O. Pass {} to restore the
    // real HTTPS transport.
    void SetTransportForTesting(
        std::function<void(const std::string& api_key, const std::string& host,
                           const std::vector<PostHogEvent>& events)> fn);

    // Testing seam: disable the automatic per-event / threshold flush so tests
    // buffer events and drive sending deterministically via Flush()/Drain.
    void SetAutoFlushEnabledForTesting(bool enabled);

    // Testing seam: number of first-per-function calls emitted per-call before
    // switching to aggregation. Set to 0 to make all calls aggregate (so tests
    // can assert exact aggregate counts).
    void SetPromptFunctionCallsForTesting(int n);

    // Testing seam: clear the cached is_ci detection so a test can re-fake the
    // environment (CI status is cached once in production).
    static void ResetDetectionCacheForTesting();

    // DuckDB version and platform (for telemetry)
    void SetDuckDBVersion(const std::string& version);
    void SetDuckDBPlatform(const std::string& platform);
    std::string GetDuckDBVersion();
    std::string GetDuckDBPlatform();

    // Public static methods for MAC address (exposed for testing)
    static std::string GetMacAddress();
    static std::string GetMacAddressSafe();

    // Public static methods for stable distinct ID
    static std::string GetDistinctId();
    static std::string GetMachineId();

    // Per-process session id (UUID), emitted as $session_id on every event.
    static std::string GetSessionId();

    // Testing seam: returns the fully enriched event (common envelope merged
    // with the given props, event props winning on collision, string values
    // length-clamped) WITHOUT checking the enabled flag or enqueuing. Not part
    // of the stable embedder API.
    PostHogEvent BuildEventForTesting(const std::string& event_name,
                                      PropertyMap props = {});

    // Testing seam: drain the function aggregator and return the raw (un-sent)
    // `function_executed` events it would produce. Clears the aggregator.
    std::vector<PostHogEvent> DrainFunctionAggregatesForTesting();

private:
    PostHogTelemetry();
    ~PostHogTelemetry();

    static void ShutdownAtExit();
    void Shutdown();
    void EnsureQueueInitialized();   // starts the worker queue (lazily)
    // True only when telemetry may do work: enabled and not shutting down.
    bool CanAcceptTelemetry();
    // Enrich + buffer an event; sends promptly (coalesced on the worker) unless
    // auto-flush is disabled for testing.
    void EnqueueTelemetryEvent(const PostHogEvent &event);
    // Append an already-enriched event to the buffer (no send).
    void BufferEvent(const PostHogEvent &enriched);
    // Schedule at most one pending drain task on the worker (coalesces bursts,
    // bounds the worker queue to O(1) tasks regardless of capture rate).
    void ScheduleSend();
    // Worker task body: swap the buffer and POST it as one /batch/ request.
    void DrainAndSend();

    // Merge the common envelope into a copy of the event (event props win),
    // then length-clamp every string value.
    PostHogEvent EnrichEvent(const PostHogEvent &event) const;
    // Build the common envelope (product / version / os / arch / is_ci /
    // is_container / telemetry_schema / $session_id). One choke point so every
    // event is stamped identically.
    PropertyMap BuildEnvelope() const;

    static bool DetectCI();
    static bool DetectContainer();
    static const std::string& DetectOs();
    static const std::string& DetectArch();

    // Function-call aggregation ------------------------------------------------
    struct FunctionStat {
        uint64_t count = 0;   // recorded after sampling (== call_count)
        std::vector<double> duration_samples;  // bounded reservoir for p50
    };
    // Drain the aggregator into raw `function_executed` events (clears it).
    std::vector<PostHogEvent> BuildFunctionAggregateEvents();
    // Drain the aggregator into the pending buffer (no send). Returns true if
    // anything was buffered.
    bool BufferFunctionAggregates();
    // BufferFunctionAggregates + ScheduleSend (one coalesced send task).
    void FlushFunctionAggregates();

    static std::string ComputeDistinctId();
    static std::string Sha256Hex(const std::string& input);

#ifdef __linux__
    static bool IsPhysicalDevice(const std::string& device);
    static std::string FindFirstPhysicalDevice();
#endif

    std::atomic<bool> _telemetry_enabled;
    bool _shutdown_requested;
    std::string _api_key;
    std::string _extension_name;   // Default extension name for CaptureFunctionExecution
    std::string _product;          // Envelope product; empty = fall back to _extension_name
    std::string _product_version;  // Envelope product_version
    std::string _product_edition;  // Envelope product_edition; empty = "oss"
    std::string _duckdb_version;   // Empty = "unknown"
    std::string _duckdb_platform;  // Empty = compile-time detected platform
    std::string _host;             // Ingestion host; empty = compiled-in default
    mutable std::mutex _thread_lock;
    // shared_ptr so Flush() can hold the queue alive across DrainFor even if a
    // concurrent Shutdown() resets the member (avoids a use-after-free). The
    // task payload is an unused signal — each task drains _pending itself.
    std::shared_ptr<TelemetryTaskQueue<int>> _queue;

    // Events are sent promptly per-capture (coalesced on the worker); tests can
    // disable this to buffer and drive sending explicitly.
    std::atomic<bool> _auto_flush{true};

    std::vector<PostHogEvent> _pending;   // buffered events awaiting a batch POST
    bool _flush_scheduled = false;        // a drain task is already queued (coalescing)
    std::mutex _batch_lock;
    std::function<void(const std::string&, const std::string&,
                       const std::vector<PostHogEvent>&)> _transport;  // test seam

    std::map<std::string, FunctionStat> _function_stats;
    std::map<std::string, uint64_t> _sample_seen;      // persistent per-fn decimation counter
    std::map<std::string, uint64_t> _prompt_recorded;  // per-fn recorded count (prompt phase)
    int _prompt_function_calls = 3;                    // first-N calls emitted per-call
    uint64_t _recorded_since_flush = 0;                // triggers volume-based aggregate flush
    std::mutex _agg_lock;
    double _sampling_rate = 1.0;          // requested rate; 1.0 = record every call
    uint64_t _sample_stride = 1;          // effective decimation: record 1 of N
    double _effective_sample_rate = 1.0;  // 1.0 / _sample_stride, stamped on events

    std::map<std::string, std::string> _groups;      // group type -> key ($groups)
    std::set<std::string> _identified_groups;        // (type,key) already $groupidentify'd
};

} // namespace duckdb

#else // POSTHOG_TELEMETRY_DISABLED

// No-op stubs: every telemetry call compiles to nothing. Keep this in sync
// with the real public API above.
#include <cstdint>
#include <map>
#include <string>
#include <type_traits>

namespace duckdb {

// Minimal PropertyValue/PropertyMap so call sites passing typed properties to
// the generalised Capture API compile unchanged when telemetry is disabled.
struct PropertyValue {
    PropertyValue() {}
    PropertyValue(const char*) {}
    PropertyValue(std::string) {}
    template <typename T,
              typename std::enable_if<(std::is_integral<T>::value ||
                                       std::is_enum<T>::value) &&
                                          !std::is_same<T, bool>::value,
                                      int>::type = 0>
    PropertyValue(T) {}
    PropertyValue(double) {}
    PropertyValue(bool) {}
    static PropertyValue Json(std::string) { return PropertyValue(); }
};
using PropertyMap = std::map<std::string, PropertyValue>;

class PostHogTelemetry {
public:
    static PostHogTelemetry& Instance() {
        static PostHogTelemetry instance;
        return instance;
    }

    PostHogTelemetry(const PostHogTelemetry&) = delete;
    PostHogTelemetry& operator=(const PostHogTelemetry&) = delete;

    void SetProduct(const std::string&, const std::string&, const std::string& = "oss") {}
    void CaptureExtensionLoad(const std::string&, const std::string& = "0.1.0") {}
    void CaptureApplicationStart(const std::string&, const std::string&) {}
    void CaptureApplicationStop(const std::string&, const std::string&) {}
    void Capture(const std::string&, PropertyMap = {}) {}
    void CaptureFeature(const std::string&, PropertyMap = {}) {}
    void CaptureError(const std::string&, PropertyMap = {}) {}
    void AssociateGroup(const std::string&, const std::string&, PropertyMap = {}) {}
    void CaptureFunctionExecution(const std::string&, const std::string&, const std::string&) {}
    void CaptureFunctionExecution(const std::string&, const std::string& = "0.1.0") {}
    void RecordFunctionCall(const std::string&, double = 0) {}
    void SetSampling(double) {}
    void SetExtensionName(const std::string&) {}
    std::string GetExtensionName() { return ""; }
    bool IsEnabled() { return false; }
    void SetEnabled(bool) {}
    std::string GetAPIKey() { return ""; }
    void SetAPIKey(std::string) {}
    void SetHost(const std::string&) {}
    std::string GetHost() { return ""; }
    void Flush() {}
    void SetDuckDBVersion(const std::string&) {}
    void SetDuckDBPlatform(const std::string&) {}
    std::string GetDuckDBVersion() { return ""; }
    std::string GetDuckDBPlatform() { return ""; }

    static std::string GetMacAddress() { return ""; }
    static std::string GetMacAddressSafe() { return ""; }
    static std::string GetDistinctId() { return ""; }
    static std::string GetMachineId() { return ""; }
    static std::string GetSessionId() { return ""; }

private:
    PostHogTelemetry() = default;
    ~PostHogTelemetry() = default;
};

} // namespace duckdb

#endif // POSTHOG_TELEMETRY_DISABLED
