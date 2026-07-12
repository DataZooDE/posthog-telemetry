#pragma once

// Telemetry is compiled in by default. Consumers whose build cannot provide
// the real implementation (e.g. MinGW lanes where vcpkg cannot build OpenSSL)
// define POSTHOG_TELEMETRY_DISABLED on the TUs that call telemetry: every call
// then compiles to an inline no-op and telemetry.cpp must not be compiled.
#if !defined(POSTHOG_TELEMETRY_DISABLED)

#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <thread>
#include <queue>
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
    enum class Kind { String, Int, Double, Bool } kind;
    std::string s;
    int64_t i = 0;
    double d = 0;
    bool b = false;

    PropertyValue() : kind(Kind::String) {}                 // for map::operator[]
    PropertyValue(const char* v) : kind(Kind::String), s(v ? v : "") {}
    PropertyValue(std::string v) : kind(Kind::String), s(std::move(v)) {}
    PropertyValue(int v) : kind(Kind::Int), i(v) {}
    PropertyValue(int64_t v) : kind(Kind::Int), i(v) {}
    PropertyValue(double v) : kind(Kind::Double), d(v) {}
    PropertyValue(bool v) : kind(Kind::Bool), b(v) {}

    // Serialise this value as a JSON token (string escaped+quoted; number/bool
    // bare). Never throws.
    std::string ToJson() const;
};

using PropertyMap = std::map<std::string, PropertyValue>;

struct PostHogEvent {
    std::string event_name;
    std::string distinct_id;
    PropertyMap properties;

    std::string GetPropertiesJson() const;
    std::string GetNowISO8601() const;
};

// Free function for processing events (exposed for testing)
void PostHogProcess(const std::string api_key, const PostHogEvent &event);

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
        if (worker_thread.joinable()) {
            worker_thread.join();
        }
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
                } else {
                    continue;
                }
            }

            try {
                item.task(item.data);
            } catch (...) {
                // Swallowing exceptions to prevent thread crash
            }
        }
    }

    std::queue<QueueItem> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::thread worker_thread;
    bool stop_processing;
};

class PostHogTelemetry {
public:
    static PostHogTelemetry& Instance();

    // Delete copy constructor and assignment operator
    PostHogTelemetry(const PostHogTelemetry&) = delete;
    PostHogTelemetry& operator=(const PostHogTelemetry&) = delete;

    // Capture extension load event with name and version
    // Also stores extension_name as default for CaptureFunctionExecution
    void CaptureExtensionLoad(const std::string& extension_name,
                              const std::string& extension_version = "0.1.0");

    // Capture application lifecycle events
    void CaptureApplicationStart(const std::string& app_name,
                                  const std::string& app_version);
    void CaptureApplicationStop(const std::string& app_name,
                                 const std::string& app_version);

    // Capture function execution event - two overloads:
    // 1. Explicit extension_name
    void CaptureFunctionExecution(const std::string& function_name,
                                  const std::string& extension_name,
                                  const std::string& function_version);

    // 2. Uses stored default extension_name (from CaptureExtensionLoad or SetExtensionName)
    void CaptureFunctionExecution(const std::string& function_name,
                                  const std::string& function_version = "0.1.0");

    // Set/get default extension name for the instance
    void SetExtensionName(const std::string& name);
    std::string GetExtensionName();

    bool IsEnabled();
    void SetEnabled(bool enabled);

    std::string GetAPIKey();
    void SetAPIKey(std::string new_key);

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

private:
    PostHogTelemetry();
    ~PostHogTelemetry();

    static void ShutdownAtExit();
    void Shutdown();
    void EnsureQueueInitialized();
    void EnqueueTelemetryEvent(const PostHogEvent &event);

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
    std::string _duckdb_version;   // Empty = "unknown"
    std::string _duckdb_platform;  // Empty = compile-time detected platform
    std::mutex _thread_lock;
    std::unique_ptr<TelemetryTaskQueue<PostHogEvent>> _queue;
};

} // namespace duckdb

#else // POSTHOG_TELEMETRY_DISABLED

// No-op stubs: every telemetry call compiles to nothing. Keep this in sync
// with the real public API above.
#include <string>

namespace duckdb {

class PostHogTelemetry {
public:
    static PostHogTelemetry& Instance() {
        static PostHogTelemetry instance;
        return instance;
    }

    PostHogTelemetry(const PostHogTelemetry&) = delete;
    PostHogTelemetry& operator=(const PostHogTelemetry&) = delete;

    void CaptureExtensionLoad(const std::string&, const std::string& = "0.1.0") {}
    void CaptureApplicationStart(const std::string&, const std::string&) {}
    void CaptureApplicationStop(const std::string&, const std::string&) {}
    void CaptureFunctionExecution(const std::string&, const std::string&, const std::string&) {}
    void CaptureFunctionExecution(const std::string&, const std::string& = "0.1.0") {}
    void SetExtensionName(const std::string&) {}
    std::string GetExtensionName() { return ""; }
    bool IsEnabled() { return false; }
    void SetEnabled(bool) {}
    std::string GetAPIKey() { return ""; }
    void SetAPIKey(std::string) {}
    void SetDuckDBVersion(const std::string&) {}
    void SetDuckDBPlatform(const std::string&) {}
    std::string GetDuckDBVersion() { return ""; }
    std::string GetDuckDBPlatform() { return ""; }

    static std::string GetMacAddress() { return ""; }
    static std::string GetMacAddressSafe() { return ""; }
    static std::string GetDistinctId() { return ""; }
    static std::string GetMachineId() { return ""; }

private:
    PostHogTelemetry() = default;
    ~PostHogTelemetry() = default;
};

} // namespace duckdb

#endif // POSTHOG_TELEMETRY_DISABLED
