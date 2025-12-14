#include "catch.hpp"
#include "telemetry.hpp"

#include <chrono>
#include <cstdlib>
#include <thread>

using namespace duckdb;

TEST_CASE("Error Handling - PostHogProcess with invalid data does not throw", "[error]") {
    PostHogEvent event = {
        "test_event",
        "",  // Empty distinct_id
        {}   // Empty properties
    };

    // PostHogProcess should handle invalid data gracefully
    REQUIRE_NOTHROW(PostHogProcess("invalid_api_key", event));

    // Wait for any async processing
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

TEST_CASE("Error Handling - PostHogProcess with empty API key", "[error]") {
    PostHogEvent event = {
        "test_event",
        "user_123",
        {{"key", "value"}}
    };

    // Empty API key should be handled gracefully
    REQUIRE_NOTHROW(PostHogProcess("", event));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

TEST_CASE("Error Handling - Network failure simulation (invalid endpoint)", "[error]") {
    // The PostHogProcess function sends to eu.posthog.com
    // If network is unavailable, it should fail silently
    PostHogEvent event = {
        "test_event",
        "user_123",
        {{"key", "value"}}
    };

    // This should not throw even if network is unavailable
    REQUIRE_NOTHROW(PostHogProcess("invalid_key_12345", event));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

TEST_CASE("Error Handling - Queue exception propagation prevention", "[error]") {
    std::atomic<int> post_exception_tasks{0};

    {
        TelemetryTaskQueue<int> queue;

        // Enqueue a task that throws
        queue.EnqueueTask([](int) {
            throw std::runtime_error("Intentional test exception");
        }, 1);

        // Enqueue tasks after the exception
        for (int i = 0; i < 5; i++) {
            queue.EnqueueTask([&post_exception_tasks](int) {
                post_exception_tasks++;
            }, i);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // All tasks after exception should still run
    REQUIRE(post_exception_tasks == 5);
}

TEST_CASE("Error Handling - Telemetry with rapid enable/disable", "[error]") {
    auto& telemetry = PostHogTelemetry::Instance();
    bool original = telemetry.IsEnabled();

    // Rapidly toggle enabled state while capturing
    for (int i = 0; i < 100; i++) {
        telemetry.SetEnabled(i % 2 == 0);
        REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("test"));
        REQUIRE_NOTHROW(telemetry.CaptureFunctionExecution("test"));
    }

    telemetry.SetEnabled(original);
}

TEST_CASE("Error Handling - Concurrent enable/disable and capture", "[error]") {
    auto& telemetry = PostHogTelemetry::Instance();
    bool original = telemetry.IsEnabled();
    std::atomic<int> errors{0};

    std::vector<std::thread> threads;

    // Thread toggling enabled
    threads.emplace_back([&telemetry]() {
        for (int i = 0; i < 1000; i++) {
            telemetry.SetEnabled(i % 2 == 0);
        }
    });

    // Threads capturing events
    for (int t = 0; t < 5; t++) {
        threads.emplace_back([&telemetry, &errors]() {
            try {
                for (int i = 0; i < 200; i++) {
                    telemetry.CaptureExtensionLoad("test");
                    telemetry.CaptureFunctionExecution("test");
                }
            } catch (...) {
                errors++;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    REQUIRE(errors == 0);
    telemetry.SetEnabled(original);
}

TEST_CASE("Error Handling - Malformed event names", "[error]") {
    auto& telemetry = PostHogTelemetry::Instance();

    // Various potentially problematic names
    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("\n\t\r"));
    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("name with spaces"));
    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("name\"with\"quotes"));
    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("name\\with\\backslashes"));
    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("{json:like}"));
    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("<xml>like</xml>"));
    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("null"));
    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("undefined"));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

TEST_CASE("Error Handling - Special characters in properties", "[error]") {
    PostHogEvent event = {
        "test_event",
        "user_123",
        {
            {"quote", "value\"with\"quotes"},
            {"backslash", "value\\with\\backslash"},
            {"newline", "value\nwith\nnewlines"},
            {"tab", "value\twith\ttabs"},
            {"null_char", std::string("before\0after", 12)},
            {"unicode", "日本語中文한국어"}
        }
    };

    // Should not throw when generating JSON
    REQUIRE_NOTHROW(event.GetPropertiesJson());

    // Should not throw when processing
    REQUIRE_NOTHROW(PostHogProcess("test_key", event));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

TEST_CASE("Error Handling - Very large property count", "[error]") {
    PostHogEvent event = {
        "test_event",
        "user_123",
        {}
    };

    // Add thousands of properties
    for (int i = 0; i < 10000; i++) {
        event.properties["key_" + std::to_string(i)] = "value_" + std::to_string(i);
    }

    REQUIRE_NOTHROW(event.GetPropertiesJson());
    REQUIRE_NOTHROW(PostHogProcess("test_key", event));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

TEST_CASE("Error Handling - Queue destruction during active processing", "[error]") {
    std::atomic<int> active_tasks{0};
    std::atomic<int> completed_tasks{0};

    {
        TelemetryTaskQueue<int> queue;

        // Enqueue slow tasks
        for (int i = 0; i < 50; i++) {
            queue.EnqueueTask([&active_tasks, &completed_tasks](int) {
                active_tasks++;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                completed_tasks++;
                active_tasks--;
            }, i);
        }

        // Immediately destroy queue while tasks are running
    }

    // No crash, and no tasks should be "stuck" in active state
    // (though some may not have started/completed)
    REQUIRE(active_tasks == 0);
}

TEST_CASE("Error Handling - MAC address retrieval never throws", "[error]") {
    // Call many times to ensure consistency
    for (int i = 0; i < 100; i++) {
        REQUIRE_NOTHROW(PostHogTelemetry::GetMacAddressSafe());
    }
}

TEST_CASE("Error Handling - Timestamp generation never throws", "[error]") {
    PostHogEvent event = {"test", "user", {}};

    for (int i = 0; i < 100; i++) {
        REQUIRE_NOTHROW(event.GetNowISO8601());
    }
}

TEST_CASE("Error Handling - Multiple std::exception types in queue", "[error]") {
    std::atomic<int> success_count{0};

    {
        TelemetryTaskQueue<int> queue;

        // Different exception types
        queue.EnqueueTask([](int) {
            throw std::runtime_error("runtime_error");
        }, 1);

        queue.EnqueueTask([](int) {
            throw std::logic_error("logic_error");
        }, 2);

        queue.EnqueueTask([](int) {
            throw std::out_of_range("out_of_range");
        }, 3);

        queue.EnqueueTask([](int) {
            throw std::invalid_argument("invalid_argument");
        }, 4);

        // Normal task after exceptions
        queue.EnqueueTask([&success_count](int) {
            success_count++;
        }, 5);

        queue.EnqueueTask([&success_count](int) {
            success_count++;
        }, 6);

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    REQUIRE(success_count == 2);
}

TEST_CASE("Error Handling - Non-std exception in queue", "[error]") {
    std::atomic<int> success_count{0};

    {
        TelemetryTaskQueue<int> queue;

        // Throw non-std exception (int)
        queue.EnqueueTask([](int) {
            throw 42;
        }, 1);

        // Throw non-std exception (string literal)
        queue.EnqueueTask([](int) {
            throw "string exception";
        }, 2);

        // Normal task should still run
        queue.EnqueueTask([&success_count](int) {
            success_count++;
        }, 3);

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    REQUIRE(success_count == 1);
}
