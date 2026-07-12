// Tests for the analysis-first telemetry schema (envelope, is_ci, typed
// properties, cardinality guard, batching, Flush, groups, aggregation).
#include "catch.hpp"
#include "telemetry.hpp"

#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>

using namespace duckdb;

namespace {

// Portable setenv/unsetenv wrappers for the tests.
void SetEnv(const char* name, const char* value) {
#ifdef _WIN32
    _putenv_s(name, value);
#else
    setenv(name, value, 1);
#endif
}

void UnsetEnv(const char* name) {
#ifdef _WIN32
    _putenv_s(name, "");
#else
    unsetenv(name);
#endif
}

// Substring probe against a serialized properties JSON object.
bool Contains(const std::string& haystack, const std::string& needle) {
    return haystack.find(needle) != std::string::npos;
}

} // namespace

TEST_CASE("Envelope - present on every payload", "[envelope]") {
    auto& t = PostHogTelemetry::Instance();
    PostHogEvent ev = t.BuildEventForTesting("extension_loaded", {});
    std::string json = ev.GetPropertiesJson();

    for (const char* key : {"\"product\"", "\"product_version\"", "\"product_edition\"",
                            "\"duckdb_version\"", "\"os\"", "\"arch\"", "\"platform\"",
                            "\"is_ci\"", "\"is_container\"", "\"telemetry_schema\"",
                            "\"$session_id\""}) {
        INFO("missing envelope key: " << key);
        REQUIRE(Contains(json, key));
    }
}

TEST_CASE("Envelope - telemetry_schema is the unquoted number 2", "[envelope]") {
    auto& t = PostHogTelemetry::Instance();
    std::string json = t.BuildEventForTesting("extension_loaded", {}).GetPropertiesJson();

    // Number, not a quoted string.
    REQUIRE(Contains(json, "\"telemetry_schema\": 2"));
    REQUIRE_FALSE(Contains(json, "\"telemetry_schema\": \"2\""));
}

TEST_CASE("Envelope - is_ci is true under faked GITHUB_ACTIONS", "[envelope][is_ci]") {
    auto& t = PostHogTelemetry::Instance();

    SetEnv("GITHUB_ACTIONS", "true");
    std::string json_ci = t.BuildEventForTesting("extension_loaded", {}).GetPropertiesJson();
    // Unquoted JSON boolean true.
    REQUIRE(Contains(json_ci, "\"is_ci\": true"));
    REQUIRE_FALSE(Contains(json_ci, "\"is_ci\": \"true\""));

    UnsetEnv("GITHUB_ACTIONS");
    // Clear any other CI vars that might be set in the real environment so the
    // "false" branch is deterministic.
    for (const char* v : {"CI", "GITLAB_CI", "BUILDKITE", "JENKINS_URL",
                          "TEAMCITY_VERSION", "TF_BUILD", "CIRCLECI"}) {
        UnsetEnv(v);
    }
    std::string json_no_ci = t.BuildEventForTesting("extension_loaded", {}).GetPropertiesJson();
    REQUIRE(Contains(json_no_ci, "\"is_ci\": false"));
}

TEST_CASE("Envelope - product falls back to extension name", "[envelope]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetExtensionName("erpl");
    // Ensure SetProduct hasn't been called by clearing to empty product.
    // (SetProduct with empty name makes fallback kick in.)
    t.SetProduct("", "", "");

    std::string json = t.BuildEventForTesting("extension_loaded", {}).GetPropertiesJson();
    REQUIRE(Contains(json, "\"product\": \"erpl\""));
    // Edition defaults to oss even when unset.
    REQUIRE(Contains(json, "\"product_edition\": \"oss\""));
}

TEST_CASE("Envelope - SetProduct overrides product/version/edition", "[envelope]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetProduct("flapi", "1.4.2", "enterprise");

    std::string json = t.BuildEventForTesting("server_started", {}).GetPropertiesJson();
    REQUIRE(Contains(json, "\"product\": \"flapi\""));
    REQUIRE(Contains(json, "\"product_version\": \"1.4.2\""));
    REQUIRE(Contains(json, "\"product_edition\": \"enterprise\""));

    // Restore default so later tests see the fallback behaviour.
    t.SetProduct("", "", "");
}

TEST_CASE("PropertyValue - JSON typing: bool/number unquoted, string quoted", "[envelope][typing]") {
    auto& t = PostHogTelemetry::Instance();
    PropertyMap props;
    props["a_bool"]   = true;
    props["a_false"]  = false;
    props["an_int"]   = int64_t{42};
    props["a_double"] = 1.5;
    props["a_string"] = "hello";

    std::string json = t.BuildEventForTesting("feature_used", props).GetPropertiesJson();

    REQUIRE(Contains(json, "\"a_bool\": true"));
    REQUIRE(Contains(json, "\"a_false\": false"));
    REQUIRE(Contains(json, "\"an_int\": 42"));
    REQUIRE(Contains(json, "\"a_string\": \"hello\""));
    // Double serialises as a bare number (starts with 1.5).
    REQUIRE(Contains(json, "\"a_double\": 1.5"));
    // None of the numerics/bools are quoted.
    REQUIRE_FALSE(Contains(json, "\"an_int\": \"42\""));
    REQUIRE_FALSE(Contains(json, "\"a_bool\": \"true\""));
}

TEST_CASE("Cardinality guard - long property values are clamped", "[envelope][cardinality]") {
    auto& t = PostHogTelemetry::Instance();

    // Simulate an accidental PII/SQL leak: a huge free-form value.
    std::string huge(10000, 'x');
    PropertyMap props;
    props["leak"] = huge;

    PostHogEvent ev = t.BuildEventForTesting("feature_used", props);

    // The clamped value must be present and bounded; the full 10k string must
    // NOT survive into the outgoing payload.
    const std::string& out = ev.properties.at("leak").s;
    REQUIRE(out.size() <= 512);

    std::string json = ev.GetPropertiesJson();
    REQUIRE_FALSE(Contains(json, huge));
}

TEST_CASE("Aggregation - 1e6 calls collapse to O(#functions) events", "[aggregation]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);
    t.SetSampling(1.0);
    t.DrainFunctionAggregatesForTesting();  // clear any prior state

    const int kFns = 5;
    const int kCalls = 1000000;
    std::vector<std::string> names;
    for (int i = 0; i < kFns; i++) {
        names.push_back("fn_" + std::to_string(i));
    }
    for (int i = 0; i < kCalls; i++) {
        t.RecordFunctionCall(names[i % kFns]);
    }

    auto events = t.DrainFunctionAggregatesForTesting();
    REQUIRE(events.size() == static_cast<size_t>(kFns));  // NOT 1e6 events

    uint64_t total = 0;
    for (auto& ev : events) {
        REQUIRE(ev.event_name == "function_executed");
        const auto& cc = ev.properties.at("call_count");
        REQUIRE(cc.kind == PropertyValue::Kind::Int);  // numeric, not quoted
        total += static_cast<uint64_t>(cc.i);
        REQUIRE(ev.properties.count("function_name") == 1);
        REQUIRE(ev.properties.count("duration_ms_p50") == 1);
    }
    REQUIRE(total == static_cast<uint64_t>(kCalls));  // no calls lost
}

TEST_CASE("Aggregation - duration_ms_p50 reflects recorded durations", "[aggregation]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);
    t.SetSampling(1.0);
    t.DrainFunctionAggregatesForTesting();

    for (int i = 0; i < 9; i++) {
        t.RecordFunctionCall("timed", 10.0);
    }
    auto events = t.DrainFunctionAggregatesForTesting();
    REQUIRE(events.size() == 1);
    const auto& p50 = events[0].properties.at("duration_ms_p50");
    REQUIRE(p50.kind == PropertyValue::Kind::Double);
    REQUIRE(p50.d == Approx(10.0));
}

TEST_CASE("Aggregation - sampling decimates and stamps sample_rate", "[aggregation][sampling]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);
    t.DrainFunctionAggregatesForTesting();

    t.SetSampling(0.1);  // ~1 in 10
    for (int i = 0; i < 1000; i++) {
        t.RecordFunctionCall("hot");
    }
    auto events = t.DrainFunctionAggregatesForTesting();
    REQUIRE(events.size() == 1);
    const auto& cc = events[0].properties.at("call_count");
    REQUIRE(cc.i >= 50);
    REQUIRE(cc.i <= 150);                            // decimated, not 1000
    REQUIRE(events[0].properties.count("sample_rate") == 1);

    t.SetSampling(1.0);  // restore
}

TEST_CASE("Session id - stable within a process, UUID-shaped", "[envelope][session]") {
    std::string a = PostHogTelemetry::GetSessionId();
    std::string b = PostHogTelemetry::GetSessionId();
    REQUIRE(a == b);
    REQUIRE(a.size() == 36);           // 8-4-4-4-12
    REQUIRE(a[8] == '-');
    REQUIRE(a[13] == '-');
    REQUIRE(a[18] == '-');
    REQUIRE(a[23] == '-');
}
