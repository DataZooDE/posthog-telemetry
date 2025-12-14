#include "catch.hpp"
#include "telemetry.hpp"

#include <regex>
#include <string>

using namespace duckdb;

TEST_CASE("PostHogEvent - Basic JSON serialization", "[event]") {
    PostHogEvent event = {
        "test_event",
        "user_123",
        {
            {"property1", "value1"},
            {"property2", "value2"}
        }
    };

    std::string json = event.GetPropertiesJson();

    REQUIRE(json.find("\"property1\"") != std::string::npos);
    REQUIRE(json.find("\"value1\"") != std::string::npos);
    REQUIRE(json.find("\"property2\"") != std::string::npos);
    REQUIRE(json.find("\"value2\"") != std::string::npos);
    REQUIRE(json.front() == '{');
    REQUIRE(json.back() == '}');
}

TEST_CASE("PostHogEvent - Empty properties", "[event]") {
    PostHogEvent event = {
        "test_event",
        "user_123",
        {}  // Empty properties map
    };

    std::string json = event.GetPropertiesJson();

    REQUIRE(json == "{}");
}

TEST_CASE("PostHogEvent - Single property", "[event]") {
    PostHogEvent event = {
        "test_event",
        "user_123",
        {{"only_key", "only_value"}}
    };

    std::string json = event.GetPropertiesJson();

    // Should not have commas for single property
    REQUIRE(json.find(',') == std::string::npos);
    REQUIRE(json.find("\"only_key\"") != std::string::npos);
    REQUIRE(json.find("\"only_value\"") != std::string::npos);
}

TEST_CASE("PostHogEvent - Many properties", "[event]") {
    PostHogEvent event = {
        "test_event",
        "user_123",
        {}
    };

    // Add 100 properties
    for (int i = 0; i < 100; i++) {
        event.properties["key_" + std::to_string(i)] = "value_" + std::to_string(i);
    }

    std::string json = event.GetPropertiesJson();

    REQUIRE(json.front() == '{');
    REQUIRE(json.back() == '}');
    REQUIRE(json.find("\"key_0\"") != std::string::npos);
    REQUIRE(json.find("\"key_99\"") != std::string::npos);
}

TEST_CASE("PostHogEvent - Timestamp format ISO8601", "[event]") {
    PostHogEvent event = {
        "test_event",
        "user_123",
        {}
    };

    std::string timestamp = event.GetNowISO8601();

    // ISO8601 format: YYYY-MM-DDTHH:MM:SSZ
    // Should be 20 characters: 2024-12-14T10:30:45Z
    REQUIRE(timestamp.length() == 20);
    REQUIRE(timestamp[4] == '-');
    REQUIRE(timestamp[7] == '-');
    REQUIRE(timestamp[10] == 'T');
    REQUIRE(timestamp[13] == ':');
    REQUIRE(timestamp[16] == ':');
    REQUIRE(timestamp[19] == 'Z');
}

TEST_CASE("PostHogEvent - Numeric values in properties", "[event]") {
    PostHogEvent event = {
        "test_event",
        "user_123",
        {
            {"count", "42"},
            {"version", "1.0.0"},
            {"negative", "-100"}
        }
    };

    std::string json = event.GetPropertiesJson();

    REQUIRE(json.find("\"42\"") != std::string::npos);
    REQUIRE(json.find("\"1.0.0\"") != std::string::npos);
    REQUIRE(json.find("\"-100\"") != std::string::npos);
}

TEST_CASE("PostHogEvent - Unicode property values", "[event]") {
    PostHogEvent event = {
        "test_event",
        "user_123",
        {
            {"german", "Größe"},
            {"emoji", "test"},  // Avoid actual emoji for compatibility
            {"chinese", "中文"}
        }
    };

    std::string json = event.GetPropertiesJson();

    // Should contain the Unicode strings
    REQUIRE(json.find("Größe") != std::string::npos);
    REQUIRE(json.find("中文") != std::string::npos);
}

TEST_CASE("PostHogEvent - Empty string values", "[event]") {
    PostHogEvent event = {
        "test_event",
        "user_123",
        {
            {"empty", ""},
            {"not_empty", "value"}
        }
    };

    std::string json = event.GetPropertiesJson();

    // Should have empty string value
    REQUIRE(json.find("\"empty\": \"\"") != std::string::npos);
}

TEST_CASE("PostHogEvent - Long property values", "[event]") {
    std::string long_value(10000, 'x');  // 10KB string

    PostHogEvent event = {
        "test_event",
        "user_123",
        {{"long_key", long_value}}
    };

    std::string json = event.GetPropertiesJson();

    REQUIRE(json.find(long_value) != std::string::npos);
    REQUIRE(json.length() > 10000);
}
