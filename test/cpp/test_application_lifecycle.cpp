#include "catch.hpp"
#include "telemetry.hpp"

#include <string>

using namespace duckdb;

TEST_CASE("PostHogTelemetry - CaptureApplicationStart does not throw when enabled", "[application_lifecycle]") {
    auto& telemetry = PostHogTelemetry::Instance();
    bool original = telemetry.IsEnabled();
    telemetry.SetEnabled(true);

    REQUIRE_NOTHROW(telemetry.CaptureApplicationStart("flapi", "0.3.0"));

    telemetry.SetEnabled(original);
}

TEST_CASE("PostHogTelemetry - CaptureApplicationStop does not throw when enabled", "[application_lifecycle]") {
    auto& telemetry = PostHogTelemetry::Instance();
    bool original = telemetry.IsEnabled();
    telemetry.SetEnabled(true);

    REQUIRE_NOTHROW(telemetry.CaptureApplicationStop("flapi", "0.3.0"));

    telemetry.SetEnabled(original);
}

TEST_CASE("PostHogTelemetry - CaptureApplicationStart is no-op when disabled", "[application_lifecycle]") {
    auto& telemetry = PostHogTelemetry::Instance();
    bool original = telemetry.IsEnabled();
    telemetry.SetEnabled(false);

    // Should return immediately without enqueuing, no throw
    REQUIRE_NOTHROW(telemetry.CaptureApplicationStart("flapi", "0.3.0"));

    telemetry.SetEnabled(original);
}

TEST_CASE("PostHogTelemetry - CaptureApplicationStop is no-op when disabled", "[application_lifecycle]") {
    auto& telemetry = PostHogTelemetry::Instance();
    bool original = telemetry.IsEnabled();
    telemetry.SetEnabled(false);

    REQUIRE_NOTHROW(telemetry.CaptureApplicationStop("flapi", "0.3.0"));

    telemetry.SetEnabled(original);
}

TEST_CASE("PostHogEvent - application_start event has correct structure", "[application_lifecycle]") {
    PostHogEvent event;
    event.event_name = "application_start";
    event.distinct_id = "aa:bb:cc:dd:ee:ff";
    event.properties = {
        {"app_name", "flapi"},
        {"app_version", "0.3.0"},
        {"platform", "linux_amd64"},
        {"duckdb_version", "1.4.4"}
    };

    std::string json = event.GetPropertiesJson();

    REQUIRE(json.find("\"app_name\"") != std::string::npos);
    REQUIRE(json.find("\"flapi\"") != std::string::npos);
    REQUIRE(json.find("\"app_version\"") != std::string::npos);
    REQUIRE(json.find("\"0.3.0\"") != std::string::npos);
    REQUIRE(json.find("\"platform\"") != std::string::npos);
    REQUIRE(json.find("\"duckdb_version\"") != std::string::npos);
}

TEST_CASE("PostHogEvent - application_stop event has correct structure", "[application_lifecycle]") {
    PostHogEvent event;
    event.event_name = "application_stop";
    event.distinct_id = "aa:bb:cc:dd:ee:ff";
    event.properties = {
        {"app_name", "flapi"},
        {"app_version", "0.3.0"},
        {"platform", "linux_amd64"},
        {"duckdb_version", "1.4.4"}
    };

    std::string json = event.GetPropertiesJson();

    REQUIRE(json.find("\"app_name\"") != std::string::npos);
    REQUIRE(json.find("\"app_version\"") != std::string::npos);
}

TEST_CASE("PostHogTelemetry - CaptureApplicationStart with empty version does not throw", "[application_lifecycle]") {
    auto& telemetry = PostHogTelemetry::Instance();
    bool original = telemetry.IsEnabled();
    telemetry.SetEnabled(true);

    REQUIRE_NOTHROW(telemetry.CaptureApplicationStart("flapi", ""));

    telemetry.SetEnabled(original);
}
