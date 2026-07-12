// Tests for the analysis-first telemetry schema (envelope, is_ci, typed
// properties, cardinality guard, batching, Flush, groups, aggregation).
#include "catch.hpp"
#include "telemetry.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <clocale>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <limits>
#include <map>
#include <mutex>
#include <string>
#include <thread>
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

    // Save & restore every CI var this test touches so it doesn't leak into the
    // process environment seen by later test cases.
    const char* const ci_vars[] = {"GITHUB_ACTIONS", "CI", "GITLAB_CI", "BUILDKITE",
                                    "JENKINS_URL", "TEAMCITY_VERSION", "TF_BUILD", "CIRCLECI"};
    std::map<std::string, std::string> saved;
    std::vector<std::string> was_unset;
    for (const char* v : ci_vars) {
        if (const char* cur = std::getenv(v)) saved[v] = cur;
        else was_unset.push_back(v);
    }

    SetEnv("GITHUB_ACTIONS", "true");
    PostHogTelemetry::ResetDetectionCacheForTesting();   // is_ci is cached now
    std::string json_ci = t.BuildEventForTesting("extension_loaded", {}).GetPropertiesJson();
    // Unquoted JSON boolean true.
    REQUIRE(Contains(json_ci, "\"is_ci\": true"));
    REQUIRE_FALSE(Contains(json_ci, "\"is_ci\": \"true\""));

    // Clear every CI var so the "false" branch is deterministic.
    for (const char* v : ci_vars) UnsetEnv(v);
    PostHogTelemetry::ResetDetectionCacheForTesting();
    std::string json_no_ci = t.BuildEventForTesting("extension_loaded", {}).GetPropertiesJson();
    REQUIRE(Contains(json_no_ci, "\"is_ci\": false"));

    // Restore original environment.
    for (auto& kv : saved) SetEnv(kv.first.c_str(), kv.second.c_str());
    for (auto& v : was_unset) UnsetEnv(v.c_str());
    PostHogTelemetry::ResetDetectionCacheForTesting();
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

TEST_CASE("PropertyValue - unsigned above INT64_MAX serialises positive", "[typing]") {
    auto& t = PostHogTelemetry::Instance();
    PropertyMap props;
    uint64_t big = 18446744073709551615ULL;  // UINT64_MAX, > INT64_MAX
    props["big"] = big;
    std::string json = t.BuildEventForTesting("feature_used", props).GetPropertiesJson();
    // Serialised as the full positive value, not signed-narrowed to -1.
    REQUIRE(Contains(json, "\"big\": 18446744073709551615"));
    REQUIRE_FALSE(Contains(json, "\"big\": -"));
}

TEST_CASE("SetSampling - NaN/Inf are handled safely", "[aggregation][sampling]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);
    t.DrainFunctionAggregatesForTesting();

    REQUIRE_NOTHROW(t.SetSampling(std::nan("")));   // must not UB/crash
    // std::numeric_limits, not 1.0/0.0: MSVC rejects the constant div-by-zero.
    REQUIRE_NOTHROW(t.SetSampling(std::numeric_limits<double>::infinity()));
    // NaN treated as rate 1.0 (record all): 5 calls -> 5 recorded.
    t.SetSampling(std::nan(""));
    for (int i = 0; i < 5; i++) t.RecordFunctionCall("nan_fn");
    int64_t count = 0;
    for (auto& e : t.DrainFunctionAggregatesForTesting())
        if (e.event_name == "function_executed" && e.properties.at("function_name").s == "nan_fn")
            count = e.properties.at("call_count").i;
    REQUIRE(count == 5);
    t.SetSampling(1.0);
}

TEST_CASE("Opt-out - Flush after runtime disable discards buffered events", "[capture]") {
    auto& t = PostHogTelemetry::Instance();
    std::atomic<int> sent{0};
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            sent += static_cast<int>(evs.size());
        });

    t.SetEnabled(true);
    t.Flush();
    sent = 0;

    t.CaptureFeature("buffered_then_optout", {});   // buffered (auto-flush off in tests)
    t.SetEnabled(false);
    t.Flush();   // opted out: must discard, not send
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    REQUIRE(sent == 0);

    t.SetEnabled(true);
    t.SetTransportForTesting({});
}

TEST_CASE("Coalescing - a burst collapses to few POSTs, no loss", "[batch][coalesce]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);

    std::atomic<int> posts{0};
    std::atomic<int> events{0};
    std::promise<void> release;
    auto fut = release.get_future().share();
    std::atomic<bool> block_armed{false};   // disarmed during the clearing flush
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            if (block_armed.exchange(false)) fut.wait();   // block the first burst POST
            posts++;
            events += static_cast<int>(evs.size());
        });

    // Clear any prior-test leftovers WITHOUT blocking, then reset counters and
    // arm the block only for the burst so nothing extra is counted.
    t.Flush();
    posts = 0; events = 0; block_armed = true;

    t.SetAutoFlushEnabledForTesting(true);
    const int N = 50;
    for (int i = 0; i < N; i++) t.CaptureFeature("burst", {});
    release.set_value();
    t.SetAutoFlushEnabledForTesting(false);
    t.Flush();   // drain the remainder

    REQUIRE(events.load() == N);   // every event delivered exactly once (no loss/dup)
    REQUIRE(posts.load() <= 5);    // coalesced, NOT one POST per capture

    t.SetTransportForTesting({});
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

    // One `function_executed` per function — O(#functions), never 1e6. The
    // legacy per-call `function_execution` name is intentionally NOT dual-emitted
    // (aggregation changes its shape).
    uint64_t total = 0;
    for (auto& ev : events) {
        REQUIRE(ev.event_name == "function_executed");
        const auto& cc = ev.properties.at("call_count");
        REQUIRE(cc.kind == PropertyValue::Kind::Int);  // numeric, not quoted
        total += static_cast<uint64_t>(cc.i);
        REQUIRE(ev.properties.count("function_name") == 1);
        REQUIRE(ev.properties.count("duration_ms_p50") == 1);
    }
    REQUIRE(events.size() == static_cast<size_t>(kFns));  // NOT 1e6 events
    REQUIRE(total == static_cast<uint64_t>(kCalls));      // no calls lost
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
    const PostHogEvent* executed = nullptr;
    for (auto& ev : events) {
        if (ev.event_name == "function_executed") { executed = &ev; break; }
    }
    REQUIRE(executed != nullptr);
    const auto& p50 = executed->properties.at("duration_ms_p50");
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
    const PostHogEvent* executed = nullptr;
    for (auto& ev : events) {
        if (ev.event_name == "function_executed") { executed = &ev; break; }
    }
    REQUIRE(executed != nullptr);
    const auto& cc = executed->properties.at("call_count");
    REQUIRE(cc.i >= 50);
    REQUIRE(cc.i <= 150);                            // decimated, not 1000
    REQUIRE(executed->properties.count("sample_rate") == 1);

    t.SetSampling(1.0);  // restore
}

TEST_CASE("Batching - buffered captures coalesce into one /batch/ POST", "[batch]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);

    std::atomic<int> post_count{0};
    std::atomic<int> total_events{0};
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            post_count++;
            total_events += static_cast<int>(evs.size());
        });

    // Drain anything earlier tests left buffered, then reset counters.
    t.Flush();
    post_count = 0;
    total_events = 0;

    const int N = 5;  // < kBatchMaxN, so one Flush coalesces them into one POST
    for (int i = 0; i < N; i++) {
        t.CaptureExtensionLoad("batch_ext");
    }
    t.Flush();

    REQUIRE(post_count == 1);            // exactly one /batch/ POST for N events
    REQUIRE(total_events >= N);          // all N events rode in that one POST

    t.SetTransportForTesting({});        // restore real transport
}

TEST_CASE("Flush - blocks until buffered events are sent", "[flush]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);

    std::atomic<int> sent{0};
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));  // slow send
            sent += static_cast<int>(evs.size());
        });

    t.Flush();      // clear prior
    sent = 0;

    t.CaptureExtensionLoad("flush_ext");
    t.Flush();      // must block until the slow transport has finished

    REQUIRE(sent >= 1);

    t.SetTransportForTesting({});
}

TEST_CASE("SetHost - host is configurable and reaches the transport", "[host]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);

    std::string seen_host;
    t.SetTransportForTesting(
        [&](const std::string&, const std::string& host, const std::vector<PostHogEvent>&) {
            seen_host = host;
        });

    t.Flush();  // clear prior (may set seen_host to the default; overwritten below)

    t.SetHost("https://custom.example.com");
    REQUIRE(t.GetHost() == "https://custom.example.com");

    t.CaptureExtensionLoad("host_ext");
    t.Flush();
    REQUIRE(seen_host == "https://custom.example.com");

    // Default stays eu.posthog.com until eu.i.posthog.com is verified.
    t.SetHost("");
    REQUIRE(t.GetHost() == std::string("https://eu.posthog.com"));

    t.SetTransportForTesting({});
}

TEST_CASE("Capture - feature_used and $exception shapes", "[capture]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);

    std::vector<PostHogEvent> captured;
    std::mutex m;
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            std::lock_guard<std::mutex> lk(m);
            for (auto& e : evs) captured.push_back(e);
        });

    t.Flush();
    { std::lock_guard<std::mutex> lk(m); captured.clear(); }

    t.CaptureFeature("sap_rfc", {{"duration_ms", 12.0}});
    t.CaptureError("connection_timeout", {{"phase", "connect"}});
    t.Flush();

    bool feature_ok = false, err_ok = false;
    for (auto& e : captured) {
        if (e.event_name == "feature_used" &&
            e.properties.count("feature") && e.properties.at("feature").s == "sap_rfc") {
            feature_ok = true;
        }
        if (e.event_name == "$exception" &&
            e.properties.count("error_class") &&
            e.properties.at("error_class").s == "connection_timeout") {
            err_ok = true;
        }
    }
    REQUIRE(feature_ok);
    REQUIRE(err_ok);

    t.SetTransportForTesting({});
}

TEST_CASE("Dual-emit - extension load emits new and legacy names", "[capture][compat]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);

    std::vector<std::string> names;
    std::mutex m;
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            std::lock_guard<std::mutex> lk(m);
            for (auto& e : evs) names.push_back(e.event_name);
        });

    t.Flush();
    { std::lock_guard<std::mutex> lk(m); names.clear(); }

    t.CaptureExtensionLoad("erpl", "1.0.0");
    t.Flush();

    REQUIRE(std::count(names.begin(), names.end(), "extension_loaded") == 1);
    REQUIRE(std::count(names.begin(), names.end(), "extension_load") == 1);

    t.SetTransportForTesting({});
}

TEST_CASE("Capture - disabled telemetry produces no events", "[capture]") {
    auto& t = PostHogTelemetry::Instance();

    std::atomic<int> count{0};
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            count += static_cast<int>(evs.size());
        });

    t.SetEnabled(true);
    t.Flush();       // drain aggregator + buffer
    count = 0;

    t.SetEnabled(false);
    t.Capture("should_not_send", {});
    t.CaptureFeature("nope", {});
    t.CaptureError("nope", {});
    t.Flush();

    REQUIRE(count == 0);

    t.SetEnabled(true);
    t.SetTransportForTesting({});
}

TEST_CASE("Groups - $groupidentify once, then $groups on later events", "[groups]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);

    std::vector<PostHogEvent> captured;
    std::mutex m;
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            std::lock_guard<std::mutex> lk(m);
            for (auto& e : evs) captured.push_back(e);
        });

    t.Flush();
    { std::lock_guard<std::mutex> lk(m); captured.clear(); }

    t.AssociateGroup("account", "acct_hash_123", {{"edition", "enterprise"}});
    t.AssociateGroup("account", "acct_hash_123", {{"edition", "enterprise"}});  // again
    t.CaptureFeature("sap_rfc", {});
    t.Flush();

    int account_identifies = 0;
    bool feature_has_groups = false;
    for (auto& e : captured) {
        if (e.event_name == "$groupidentify" &&
            e.properties.count("$group_key") &&
            e.properties.at("$group_key").s == "acct_hash_123") {
            account_identifies++;
        }
        if (e.event_name == "feature_used") {
            auto it = e.properties.find("$groups");
            if (it != e.properties.end() &&
                it->second.kind == PropertyValue::Kind::Json &&
                it->second.s.find("account") != std::string::npos &&
                it->second.s.find("acct_hash_123") != std::string::npos) {
                feature_has_groups = true;
            }
        }
    }
    REQUIRE(account_identifies == 1);   // identify emitted exactly once
    REQUIRE(feature_has_groups);        // later event carries $groups

    t.SetTransportForTesting({});
}

TEST_CASE("Groups - $group_set is a nested JSON object, not a quoted string", "[groups]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);

    std::vector<PostHogEvent> captured;
    std::mutex m;
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            std::lock_guard<std::mutex> lk(m);
            for (auto& e : evs) captured.push_back(e);
        });
    t.Flush();
    { std::lock_guard<std::mutex> lk(m); captured.clear(); }

    t.AssociateGroup("deployment", "dep_xyz", {{"first_seen_version", "1.2.3"}});
    t.Flush();

    bool ok = false;
    for (auto& e : captured) {
        if (e.event_name == "$groupidentify" && e.properties.count("$group_set") &&
            e.properties.at("$group_set").kind == PropertyValue::Kind::Json) {
            std::string set_json = e.properties.at("$group_set").ToJson();
            // Unquoted object containing the version.
            if (set_json.front() == '{' && set_json.find("first_seen_version") != std::string::npos) {
                ok = true;
            }
        }
    }
    REQUIRE(ok);

    t.SetTransportForTesting({});
}

TEST_CASE("PropertyValue - unsigned/size_t/long compile unambiguously", "[typing]") {
    auto& t = PostHogTelemetry::Instance();
    PropertyMap props;
    props["u"]      = static_cast<unsigned>(7);
    props["sz"]     = static_cast<size_t>(1234567);
    props["l"]      = static_cast<long>(-5);
    props["ull"]    = static_cast<unsigned long long>(9);
    props["plain"]  = 42;      // int literal
    std::string json = t.BuildEventForTesting("feature_used", props).GetPropertiesJson();
    // All land as unquoted JSON numbers.
    REQUIRE(Contains(json, "\"sz\": 1234567"));
    REQUIRE(Contains(json, "\"l\": -5"));
    REQUIRE(Contains(json, "\"plain\": 42"));
    REQUIRE_FALSE(Contains(json, "\"sz\": \"1234567\""));
}

namespace { enum ConnAuth { Basic, Sso, X509 }; }

TEST_CASE("PropertyValue - unscoped enum compiles and serialises as a number", "[typing]") {
    auto& t = PostHogTelemetry::Instance();
    PropertyMap props;
    props["auth_kind"] = Sso;   // unscoped enum, value 1
    std::string json = t.BuildEventForTesting("feature_used", props).GetPropertiesJson();
    REQUIRE(Contains(json, "\"auth_kind\": 1"));
    REQUIRE_FALSE(Contains(json, "\"auth_kind\": \"1\""));
}

TEST_CASE("Sampling - decimation state persists across flushes", "[aggregation][sampling]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);
    t.DrainFunctionAggregatesForTesting();
    t.SetSampling(0.1);   // stride 10

    // 5 windows of 4 calls = 20 calls, draining between each. With a persistent
    // per-function counter this records ~2 total; a per-flush-reset counter
    // would force-record the first call of every window (~5).
    int64_t recorded_total = 0;
    for (int w = 0; w < 5; w++) {
        for (int i = 0; i < 4; i++) t.RecordFunctionCall("g");
        for (auto& e : t.DrainFunctionAggregatesForTesting()) {
            if (e.event_name == "function_executed" &&
                e.properties.at("function_name").s == "g") {
                recorded_total += e.properties.at("call_count").i;
            }
        }
    }
    REQUIRE(recorded_total <= 3);   // ~2 with persistence, not ~5 per-window

    t.SetSampling(1.0);
}

TEST_CASE("Sampling - rate 0 emits no zero-count events", "[aggregation][sampling]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);
    t.DrainFunctionAggregatesForTesting();

    t.SetSampling(0.0);
    for (int i = 0; i < 5; i++) t.RecordFunctionCall("z");
    auto events = t.DrainFunctionAggregatesForTesting();
    for (auto& e : events) {
        // No spurious call_count==0 function events.
        REQUIRE(e.properties.at("function_name").s != "z");
    }

    t.SetSampling(1.0);
}

TEST_CASE("Timestamp - stamped at capture time on the event", "[envelope][timestamp]") {
    auto& t = PostHogTelemetry::Instance();
    PostHogEvent ev = t.BuildEventForTesting("feature_used", {});
    // Capture path stamps an ISO8601 timestamp; not left for send time.
    REQUIRE(ev.timestamp.size() == 20);
    REQUIRE(ev.timestamp[10] == 'T');
    REQUIRE(ev.timestamp[19] == 'Z');
}

TEST_CASE("Auto-flush - captures send promptly without explicit Flush", "[flush][autoflush]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);

    std::atomic<int> sent{0};
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            sent += static_cast<int>(evs.size());
        });

    t.Flush();          // clear prior
    sent = 0;

    // Enable auto-flush and capture WITHOUT calling Flush.
    t.SetAutoFlushEnabledForTesting(true);
    t.CaptureFeature("auto_flushed", {});

    // Poll briefly for the worker to send it on its own.
    for (int i = 0; i < 200 && sent.load() == 0; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    REQUIRE(sent.load() >= 1);   // sent promptly, no Flush() called

    // Restore deterministic test default.
    t.SetAutoFlushEnabledForTesting(false);
    t.SetTransportForTesting({});
}

TEST_CASE("Groups - associate while disabled, then enable, still identifies", "[groups]") {
    auto& t = PostHogTelemetry::Instance();

    std::vector<PostHogEvent> captured;
    std::mutex m;
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            std::lock_guard<std::mutex> lk(m);
            for (auto& e : evs) captured.push_back(e);
        });

    t.SetEnabled(true);
    t.Flush();
    { std::lock_guard<std::mutex> lk(m); captured.clear(); }

    // Associate while disabled: must NOT permanently suppress the identify.
    t.SetEnabled(false);
    t.AssociateGroup("account", "acct_toggle", {{"edition", "enterprise"}});
    t.SetEnabled(true);
    // Re-associate now that telemetry is enabled.
    t.AssociateGroup("account", "acct_toggle", {{"edition", "enterprise"}});
    t.Flush();

    int identifies = 0;
    for (auto& e : captured) {
        if (e.event_name == "$groupidentify" &&
            e.properties.count("$group_key") &&
            e.properties.at("$group_key").s == "acct_toggle") {
            identifies++;
        }
    }
    REQUIRE(identifies == 1);

    t.SetTransportForTesting({});
}

TEST_CASE("Sampling - per-function decimation is unbiased across functions", "[aggregation][sampling]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);
    t.DrainFunctionAggregatesForTesting();

    t.SetSampling(0.5);  // stride 2
    // Strict alternation A,B,A,B,... — the old shared counter recorded only one.
    for (int i = 0; i < 1000; i++) {
        t.RecordFunctionCall("A");
        t.RecordFunctionCall("B");
    }
    auto events = t.DrainFunctionAggregatesForTesting();

    int64_t a = 0, b = 0;
    for (auto& e : events) {
        if (e.event_name != "function_executed") continue;
        if (e.properties.at("function_name").s == "A") a = e.properties.at("call_count").i;
        if (e.properties.at("function_name").s == "B") b = e.properties.at("call_count").i;
    }
    REQUIRE(a >= 400);
    REQUIRE(b >= 400);   // both recorded, not one starved to ~0

    t.SetSampling(1.0);
}

TEST_CASE("Sampling - non-reciprocal rate stamps the effective rate", "[aggregation][sampling]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);
    t.DrainFunctionAggregatesForTesting();

    t.SetSampling(0.3);  // stride round(1/0.3)=3 -> effective 0.3333...
    for (int i = 0; i < 300; i++) t.RecordFunctionCall("f");
    auto events = t.DrainFunctionAggregatesForTesting();

    double stamped = -1;
    int64_t count = 0;
    for (auto& e : events) {
        if (e.event_name != "function_executed") continue;
        stamped = e.properties.at("sample_rate").d;
        count = e.properties.at("call_count").i;
    }
    // Effective rate is 1/3, and count/stamped reconstructs ~300 exactly.
    REQUIRE(stamped == Approx(1.0 / 3.0).epsilon(0.001));
    REQUIRE(static_cast<double>(count) / stamped == Approx(300).epsilon(0.02));

    t.SetSampling(1.0);
}

TEST_CASE("Aggregation - piggybacks on a regular event under auto-flush", "[aggregation][autoflush]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);
    t.SetSampling(1.0);
    t.DrainFunctionAggregatesForTesting();

    std::vector<PostHogEvent> captured;
    std::mutex m;
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            std::lock_guard<std::mutex> lk(m);
            for (auto& e : evs) captured.push_back(e);
        });
    t.Flush();
    { std::lock_guard<std::mutex> lk(m); captured.clear(); }

    // Record a few function calls (well below the flush threshold), then emit a
    // regular event with auto-flush on. The function stats should ride along.
    t.SetAutoFlushEnabledForTesting(true);
    for (int i = 0; i < 3; i++) t.RecordFunctionCall("sub_threshold_fn");
    t.CaptureFeature("some_feature", {});

    for (int i = 0; i < 100; i++) {
        {
            std::lock_guard<std::mutex> lk(m);
            bool has_fn = false;
            for (auto& e : captured)
                if (e.event_name == "function_executed") has_fn = true;
            if (has_fn) break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    bool shipped = false;
    {
        std::lock_guard<std::mutex> lk(m);
        for (auto& e : captured)
            if (e.event_name == "function_executed" &&
                e.properties.at("function_name").s == "sub_threshold_fn") shipped = true;
    }
    REQUIRE(shipped);   // function stats shipped alongside the feature event

    t.SetAutoFlushEnabledForTesting(false);
    t.SetTransportForTesting({});
}

TEST_CASE("Hybrid - first N function calls emit promptly per-call", "[aggregation][prompt]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);
    t.SetSampling(1.0);
    t.DrainFunctionAggregatesForTesting();

    std::vector<PostHogEvent> captured;
    std::mutex m;
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            std::lock_guard<std::mutex> lk(m);
            for (auto& e : evs) captured.push_back(e);
        });
    t.Flush();
    { std::lock_guard<std::mutex> lk(m); captured.clear(); }

    t.SetAutoFlushEnabledForTesting(true);
    t.SetPromptFunctionCallsForTesting(3);

    // Two calls (< N) of a fresh function: both ship promptly as call_count=1
    // events with NO explicit Flush() — this is what saves short sessions.
    t.RecordFunctionCall("promptfn");
    t.RecordFunctionCall("promptfn");

    for (int i = 0; i < 100; i++) {
        {
            std::lock_guard<std::mutex> lk(m);
            int c = 0;
            for (auto& e : captured)
                if (e.event_name == "function_executed" &&
                    e.properties.at("function_name").s == "promptfn") c++;
            if (c >= 2) break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    int prompt_count = 0;
    {
        std::lock_guard<std::mutex> lk(m);
        for (auto& e : captured) {
            if (e.event_name == "function_executed" &&
                e.properties.at("function_name").s == "promptfn") {
                prompt_count++;
                REQUIRE(e.properties.at("call_count").i == 1);  // per-call, not aggregated
            }
        }
    }
    REQUIRE(prompt_count == 2);   // both sent, no Flush() needed

    t.SetAutoFlushEnabledForTesting(false);
    t.SetPromptFunctionCallsForTesting(0);
    t.SetTransportForTesting({});
}

TEST_CASE("RecordFunctionCall - NaN/negative duration is sanitised, no crash", "[aggregation]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);
    t.SetSampling(1.0);
    t.DrainFunctionAggregatesForTesting();

    // A NaN reaching MedianOf's std::sort would be UB (hang/crash). Guarded.
    REQUIRE_NOTHROW(t.RecordFunctionCall("nanfn", std::nan("")));
    for (int i = 0; i < 5; i++) t.RecordFunctionCall("nanfn", std::nan(""));
    t.RecordFunctionCall("nanfn", -123.0);   // negative also sanitised

    auto events = t.DrainFunctionAggregatesForTesting();
    bool checked = false;
    for (auto& e : events) {
        if (e.event_name == "function_executed" &&
            e.properties.at("function_name").s == "nanfn") {
            REQUIRE(std::isfinite(e.properties.at("duration_ms_p50").d));  // finite median
            checked = true;
        }
    }
    REQUIRE(checked);
}

TEST_CASE("ToJson - doubles are locale-independent (dot, not comma)", "[typing]") {
    auto& t = PostHogTelemetry::Instance();
    char* prev = std::setlocale(LC_NUMERIC, nullptr);
    std::string saved = prev ? prev : "C";
    // Try a comma-decimal locale; if unavailable the test still asserts the dot.
    std::setlocale(LC_NUMERIC, "de_DE.UTF-8");

    PropertyMap props;
    props["d"] = 1.5;
    std::string json = t.BuildEventForTesting("feature_used", props).GetPropertiesJson();
    REQUIRE(Contains(json, "\"d\": 1.5"));
    REQUIRE_FALSE(Contains(json, "\"d\": 1,5"));

    std::setlocale(LC_NUMERIC, saved.c_str());
}

TEST_CASE("ClampProperty - truncates on a UTF-8 char boundary", "[envelope][cardinality]") {
    auto& t = PostHogTelemetry::Instance();
    // 200 EUR signs = 600 bytes, all 3-byte chars; clamp at 512 must land on a
    // char boundary (multiple of 3), never mid-character.
    std::string euros;
    for (int i = 0; i < 200; i++) euros += "\xE2\x82\xAC";  // U+20AC
    PropertyMap props;
    props["u"] = euros;

    PostHogEvent ev = t.BuildEventForTesting("feature_used", props);
    const std::string& out = ev.properties.at("u").s;
    REQUIRE(out.size() <= 512);
    REQUIRE(out.size() % 3 == 0);            // whole 3-byte chars only
    // Last byte must be a UTF-8 lead byte's tail, i.e. not a dangling continuation.
    if (!out.empty()) {
        REQUIRE((static_cast<unsigned char>(out.back()) & 0xC0) == 0x80);  // 0xAC is a cont. byte,
        // but the char is complete (…E2 82 AC); verify the triple is intact:
        size_t n = out.size();
        REQUIRE((static_cast<unsigned char>(out[n - 3]) & 0xF0) == 0xE0);  // lead byte E2
    }
}

TEST_CASE("Sampling - extreme rate does not overflow into a firehose", "[aggregation][sampling]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);
    t.DrainFunctionAggregatesForTesting();

    t.SetSampling(1e-300);   // absurdly small; 1/rate overflows uint64 if unclamped
    for (int i = 0; i < 1000; i++) t.RecordFunctionCall("firehose_guard");
    auto events = t.DrainFunctionAggregatesForTesting();
    for (auto& e : events) {
        // Must NOT record everything (which the UB cast would cause).
        REQUIRE(e.properties.at("call_count").i < 1000);
    }

    t.SetSampling(1.0);
}

TEST_CASE("Aggregation - function events carry extension_name", "[aggregation]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);
    t.SetSampling(1.0);
    t.SetExtensionName("erpl");
    t.DrainFunctionAggregatesForTesting();

    t.RecordFunctionCall("sap_read");
    auto events = t.DrainFunctionAggregatesForTesting();
    bool ok = false;
    for (auto& e : events) {
        if (e.event_name == "function_executed" &&
            e.properties.count("extension_name") &&
            e.properties.at("extension_name").s == "erpl") {
            ok = true;
        }
    }
    REQUIRE(ok);
}

TEST_CASE("Cleanup - stops the worker, is idempotent, disables capture", "[lifecycle]") {
    auto& t = PostHogTelemetry::Instance();
    t.SetEnabled(true);

    std::atomic<int> sent{0};
    t.SetTransportForTesting(
        [&](const std::string&, const std::string&, const std::vector<PostHogEvent>& evs) {
            sent += static_cast<int>(evs.size());
        });

    // Cleanup joins the worker and drops buffered work; safe to call twice.
    REQUIRE_NOTHROW(PostHogTelemetry::Cleanup());
    REQUIRE_NOTHROW(PostHogTelemetry::Cleanup());

    // After Cleanup, captures are no-ops (shutdown requested) and nothing sends.
    sent = 0;
    t.CaptureFeature("after_cleanup", {});
    t.Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    REQUIRE(sent == 0);

    // Restore the shared singleton for subsequent test cases.
    t.ResetShutdownForTesting();
    t.SetTransportForTesting({});
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
