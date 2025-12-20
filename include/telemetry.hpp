#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"

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

struct PostHogEvent {
    std::string event_name;
    std::string distinct_id;
    std::map<std::string, std::string> properties;

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
            tasks.push({task, data});
        }
        condition.notify_one();
    }

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            stop_processing = true;
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

                if (stop_processing && tasks.empty()) {
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
    void CaptureExtensionLoad(const std::string& extension_name,
                              const std::string& extension_version = "0.1.0");

    // Capture function execution event
    void CaptureFunctionExecution(const std::string& function_name,
                                  const std::string& function_version = "0.1.0");

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

private:
    PostHogTelemetry();
    ~PostHogTelemetry();

    void EnsureQueueInitialized();

#ifdef __linux__
    static bool IsPhysicalDevice(const std::string& device);
    static std::string FindFirstPhysicalDevice();
#endif

    std::atomic<bool> _telemetry_enabled;
    std::string _api_key;
    std::string _duckdb_version;   // Empty = use DuckDB::LibraryVersion()
    std::string _duckdb_platform;  // Empty = use DuckDB::Platform()
    std::mutex _thread_lock;
    std::unique_ptr<TelemetryTaskQueue<PostHogEvent>> _queue;
};

} // namespace duckdb
