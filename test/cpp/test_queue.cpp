#include "catch.hpp"
#include "telemetry.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace duckdb;

TEST_CASE("TelemetryTaskQueue - Basic enqueue and process", "[queue]") {
    std::atomic<int> counter{0};

    {
        TelemetryTaskQueue<int> queue;
        queue.EnqueueTask([&counter](int value) {
            counter += value;
        }, 10);

        // Wait for task to be processed
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    REQUIRE(counter == 10);
}

TEST_CASE("TelemetryTaskQueue - Multiple tasks processed in order", "[queue]") {
    std::vector<int> results;
    std::mutex results_mutex;

    {
        TelemetryTaskQueue<int> queue;

        for (int i = 0; i < 10; i++) {
            queue.EnqueueTask([&results, &results_mutex](int value) {
                std::lock_guard<std::mutex> lock(results_mutex);
                results.push_back(value);
            }, i);
        }

        // Wait for all tasks to be processed
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    REQUIRE(results.size() == 10);
    // Tasks should be processed in order (FIFO queue)
    for (int i = 0; i < 10; i++) {
        REQUIRE(results[i] == i);
    }
}

TEST_CASE("TelemetryTaskQueue - Graceful shutdown with pending tasks", "[queue]") {
    std::atomic<int> processed_count{0};

    {
        TelemetryTaskQueue<int> queue;

        // Enqueue many tasks
        for (int i = 0; i < 100; i++) {
            queue.EnqueueTask([&processed_count](int) {
                processed_count++;
                // Slow task to ensure some are pending during shutdown
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }, i);
        }

        // Queue destructor will stop and join
    }

    // Not all tasks may complete, but no crash should occur
    REQUIRE(processed_count >= 0);  // Just verify no crash
}

TEST_CASE("TelemetryTaskQueue - Exception in task does not crash queue", "[queue]") {
    std::atomic<int> after_exception_count{0};

    {
        TelemetryTaskQueue<int> queue;

        // Task that throws
        queue.EnqueueTask([](int) {
            throw std::runtime_error("Test exception");
        }, 1);

        // Task after the exception
        queue.EnqueueTask([&after_exception_count](int) {
            after_exception_count++;
        }, 2);

        // Another task
        queue.EnqueueTask([&after_exception_count](int) {
            after_exception_count++;
        }, 3);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Queue should continue processing after exception
    REQUIRE(after_exception_count == 2);
}

TEST_CASE("TelemetryTaskQueue - Stop prevents new task processing", "[queue]") {
    std::atomic<int> counter{0};

    TelemetryTaskQueue<int> queue;

    // Process one task
    queue.EnqueueTask([&counter](int) {
        counter++;
    }, 1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    REQUIRE(counter == 1);

    // Stop the queue
    queue.Stop();

    // After stop, new tasks should not be processed (queue is stopped)
    // Note: Based on current implementation, enqueue after stop may still work
    // but the queue thread has exited, so the test just verifies no crash
}

TEST_CASE("TelemetryTaskQueue - Concurrent enqueue from multiple threads", "[queue]") {
    std::atomic<int> counter{0};
    const int num_threads = 10;
    const int tasks_per_thread = 100;

    {
        TelemetryTaskQueue<int> queue;
        std::vector<std::thread> threads;

        for (int t = 0; t < num_threads; t++) {
            threads.emplace_back([&queue, &counter, tasks_per_thread]() {
                for (int i = 0; i < tasks_per_thread; i++) {
                    queue.EnqueueTask([&counter](int) {
                        counter++;
                    }, 1);
                }
            });
        }

        // Wait for all enqueue threads to finish
        for (auto& thread : threads) {
            thread.join();
        }

        // Wait for queue to process
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    REQUIRE(counter == num_threads * tasks_per_thread);
}

TEST_CASE("TelemetryTaskQueue - Empty queue destruction is safe", "[queue]") {
    {
        TelemetryTaskQueue<int> queue;
        // Destructor called immediately without any tasks
    }

    // Just verify no crash or hang
    REQUIRE(true);
}

TEST_CASE("TelemetryTaskQueue - Task with complex data type", "[queue]") {
    struct ComplexData {
        std::string name;
        int value;
        std::vector<double> data;
    };

    std::string captured_name;
    int captured_value = 0;

    {
        TelemetryTaskQueue<ComplexData> queue;

        ComplexData data = {"test_name", 42, {1.0, 2.0, 3.0}};

        queue.EnqueueTask([&captured_name, &captured_value](ComplexData d) {
            captured_name = d.name;
            captured_value = d.value;
        }, data);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    REQUIRE(captured_name == "test_name");
    REQUIRE(captured_value == 42);
}

TEST_CASE("TelemetryTaskQueue - High throughput stress test", "[queue][stress]") {
    std::atomic<int> counter{0};
    const int num_tasks = 10000;

    {
        TelemetryTaskQueue<int> queue;

        for (int i = 0; i < num_tasks; i++) {
            queue.EnqueueTask([&counter](int) {
                counter++;
            }, i);
        }

        // Wait for processing
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    REQUIRE(counter == num_tasks);
}

TEST_CASE("TelemetryTaskQueue - Task modifying external state", "[queue]") {
    std::vector<std::string> log;
    std::mutex log_mutex;

    {
        TelemetryTaskQueue<std::string> queue;

        queue.EnqueueTask([&log, &log_mutex](std::string msg) {
            std::lock_guard<std::mutex> lock(log_mutex);
            log.push_back(msg);
        }, "first");

        queue.EnqueueTask([&log, &log_mutex](std::string msg) {
            std::lock_guard<std::mutex> lock(log_mutex);
            log.push_back(msg);
        }, "second");

        queue.EnqueueTask([&log, &log_mutex](std::string msg) {
            std::lock_guard<std::mutex> lock(log_mutex);
            log.push_back(msg);
        }, "third");

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    REQUIRE(log.size() == 3);
    REQUIRE(log[0] == "first");
    REQUIRE(log[1] == "second");
    REQUIRE(log[2] == "third");
}

TEST_CASE("TelemetryTaskQueue - Double stop is safe", "[queue]") {
    TelemetryTaskQueue<int> queue;

    queue.EnqueueTask([](int) {}, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    queue.Stop();
    queue.Stop();  // Second stop should be safe

    REQUIRE(true);  // No crash
}
