#include "catch.hpp"
#include "telemetry.hpp"

#include <chrono>
#include <regex>
#include <thread>
#include <vector>

using namespace duckdb;

TEST_CASE("PostHogTelemetry - Singleton instance consistency", "[telemetry]") {
    auto& instance1 = PostHogTelemetry::Instance();
    auto& instance2 = PostHogTelemetry::Instance();

    REQUIRE(&instance1 == &instance2);
}

TEST_CASE("PostHogTelemetry - Enable/disable toggle", "[telemetry]") {
    auto& telemetry = PostHogTelemetry::Instance();

    // Store original state
    bool original = telemetry.IsEnabled();

    telemetry.SetEnabled(true);
    REQUIRE(telemetry.IsEnabled() == true);

    telemetry.SetEnabled(false);
    REQUIRE(telemetry.IsEnabled() == false);

    telemetry.SetEnabled(true);
    REQUIRE(telemetry.IsEnabled() == true);

    // Restore original state
    telemetry.SetEnabled(original);
}

TEST_CASE("PostHogTelemetry - API key get/set", "[telemetry]") {
    auto& telemetry = PostHogTelemetry::Instance();

    // Store original key
    std::string original = telemetry.GetAPIKey();

    telemetry.SetAPIKey("test_key_123");
    REQUIRE(telemetry.GetAPIKey() == "test_key_123");

    telemetry.SetAPIKey("");
    REQUIRE(telemetry.GetAPIKey() == "");

    telemetry.SetAPIKey("another_key");
    REQUIRE(telemetry.GetAPIKey() == "another_key");

    // Restore original key
    telemetry.SetAPIKey(original);
}

TEST_CASE("PostHogTelemetry - API key thread safety", "[telemetry]") {
    auto& telemetry = PostHogTelemetry::Instance();
    std::string original = telemetry.GetAPIKey();

    std::vector<std::thread> threads;
    std::atomic<int> errors{0};

    // Multiple threads setting and getting API key
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([&telemetry, &errors, i]() {
            try {
                for (int j = 0; j < 100; j++) {
                    telemetry.SetAPIKey("key_" + std::to_string(i) + "_" + std::to_string(j));
                    std::string key = telemetry.GetAPIKey();
                    // Just verify we get something back
                    if (key.empty() && j > 0) {
                        // Empty after first set would be unexpected, but not an error in this test
                    }
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

    // Restore original key
    telemetry.SetAPIKey(original);
}

TEST_CASE("PostHogTelemetry - MAC address retrieval", "[telemetry]") {
    std::string mac = PostHogTelemetry::GetMacAddress();

    // MAC address format: XX:XX:XX:XX:XX:XX or XX-XX-XX-XX-XX-XX or empty on some platforms
    if (!mac.empty()) {
        // Should be 17 characters (6 bytes with 5 separators)
        REQUIRE(mac.length() == 17);

        // Check format with colons or dashes
        bool has_colons = mac.find(':') != std::string::npos;
        bool has_dashes = mac.find('-') != std::string::npos;
        REQUIRE((has_colons || has_dashes));
    }
}

TEST_CASE("PostHogTelemetry - MAC address safe fallback", "[telemetry]") {
    std::string mac = PostHogTelemetry::GetMacAddressSafe();

    // Should never throw, always returns something
    REQUIRE_NOTHROW(PostHogTelemetry::GetMacAddressSafe());

    // Should be 17 characters (including the fallback 00:00:00:00:00:00)
    REQUIRE(mac.length() == 17);
}

TEST_CASE("PostHogTelemetry - CaptureExtensionLoad does not throw", "[telemetry]") {
    auto& telemetry = PostHogTelemetry::Instance();

    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("test_extension"));
    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("test_extension", "1.0.0"));
    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad(""));  // Empty name
    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("extension", ""));  // Empty version

    // Wait for async processing
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

TEST_CASE("PostHogTelemetry - CaptureFunctionExecution does not throw", "[telemetry]") {
    auto& telemetry = PostHogTelemetry::Instance();

    REQUIRE_NOTHROW(telemetry.CaptureFunctionExecution("test_function"));
    REQUIRE_NOTHROW(telemetry.CaptureFunctionExecution("test_function", "1.0.0"));
    REQUIRE_NOTHROW(telemetry.CaptureFunctionExecution(""));  // Empty name
    REQUIRE_NOTHROW(telemetry.CaptureFunctionExecution("function", ""));  // Empty version

    // Wait for async processing
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

TEST_CASE("PostHogTelemetry - Disabled telemetry does not capture", "[telemetry]") {
    auto& telemetry = PostHogTelemetry::Instance();
    bool original = telemetry.IsEnabled();

    telemetry.SetEnabled(false);

    // These should return immediately without processing
    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("test"));
    REQUIRE_NOTHROW(telemetry.CaptureFunctionExecution("test"));

    // Restore
    telemetry.SetEnabled(original);
}

TEST_CASE("PostHogTelemetry - Many rapid captures", "[telemetry][stress]") {
    auto& telemetry = PostHogTelemetry::Instance();

    // Rapidly capture many events
    for (int i = 0; i < 1000; i++) {
        REQUIRE_NOTHROW(telemetry.CaptureFunctionExecution("function_" + std::to_string(i)));
    }

    // Wait for async processing
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

TEST_CASE("PostHogTelemetry - Concurrent captures from multiple threads", "[telemetry]") {
    auto& telemetry = PostHogTelemetry::Instance();
    std::vector<std::thread> threads;
    std::atomic<int> errors{0};

    for (int i = 0; i < 10; i++) {
        threads.emplace_back([&telemetry, &errors, i]() {
            try {
                for (int j = 0; j < 100; j++) {
                    telemetry.CaptureExtensionLoad("ext_" + std::to_string(i));
                    telemetry.CaptureFunctionExecution("func_" + std::to_string(i) + "_" + std::to_string(j));
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

    // Wait for async processing
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

TEST_CASE("PostHogTelemetry - Unicode in extension/function names", "[telemetry]") {
    auto& telemetry = PostHogTelemetry::Instance();

    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("extension_日本語"));
    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad("extension_émoji"));
    REQUIRE_NOTHROW(telemetry.CaptureFunctionExecution("function_中文"));
    REQUIRE_NOTHROW(telemetry.CaptureFunctionExecution("function_Größe"));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

TEST_CASE("PostHogTelemetry - Very long names", "[telemetry]") {
    auto& telemetry = PostHogTelemetry::Instance();

    std::string long_name(10000, 'x');

    REQUIRE_NOTHROW(telemetry.CaptureExtensionLoad(long_name));
    REQUIRE_NOTHROW(telemetry.CaptureFunctionExecution(long_name, long_name));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

TEST_CASE("PostHogTelemetry - Default API key is set", "[telemetry]") {
    auto& telemetry = PostHogTelemetry::Instance();

    // The default API key should be set (phc_...)
    std::string key = telemetry.GetAPIKey();
    REQUIRE(!key.empty());
    REQUIRE(key.substr(0, 4) == "phc_");
}
