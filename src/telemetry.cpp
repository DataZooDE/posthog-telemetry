#define NOMINMAX

#include "telemetry.hpp"

#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <limits>
#include <random>
#include <string>
#include <vector>

#include <openssl/ssl.h>

#ifdef __linux__
#include <unistd.h>
#include <dirent.h>
#endif

#ifdef _WIN32
#include <iomanip>
#include <sstream>
#include <winsock2.h>
#include <iphlpapi.h>
#ifndef _MSC_VER
#define _MSC_VER 1936
#endif
#endif

#ifdef __APPLE__
#include <iostream>
#include <stdexcept>
#include <ifaddrs.h>
#include <net/if.h>
#include <net/if_dl.h>
#include <cstring>
#include <IOKit/IOKitLib.h>
#include <CoreFoundation/CoreFoundation.h>
#endif

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"
#include <openssl/sha.h>

namespace duckdb {

static std::string FormatStr(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    int n = std::vsnprintf(nullptr, 0, fmt, args);
    va_end(args);
    std::string buf(n + 1, '\0');
    va_start(args, fmt);
    std::vsnprintf(&buf[0], n + 1, fmt, args);
    va_end(args);
    buf.resize(n);
    return buf;
}

static std::string DetectPlatform() {
#if   defined(_WIN32) && defined(_M_ARM64)
    return "windows_arm64";
#elif defined(_WIN32)
    return "windows_amd64";
#elif defined(__APPLE__) && defined(__arm64__)
    return "osx_arm64";
#elif defined(__APPLE__)
    return "osx_amd64";
#elif defined(__linux__) && defined(__aarch64__)
    return "linux_arm64";
#elif defined(__linux__)
    return "linux_amd64";
#else
    return "unknown";
#endif
}

// os / arch split for breakdowns (the envelope keeps `platform` too, for
// continuity with existing dashboards).
static std::string ComputeOs()
{
#if   defined(_WIN32)
    return "windows";
#elif defined(__APPLE__)
    return "macos";
#elif defined(__linux__)
    return "linux";
#else
    return "unknown";
#endif
}

static std::string ComputeArch()
{
#if   defined(__aarch64__) || defined(_M_ARM64) || defined(__arm64__)
    return "arm64";
#elif defined(__x86_64__) || defined(_M_X64) || defined(__amd64__)
    return "amd64";
#else
    return "unknown";
#endif
}

// True when a well-known CI environment variable is present and truthy. Not
// cached, so a test can fake the environment at runtime.
static bool ComputeCI()
{
    static const char* const vars[] = {
        "CI", "GITHUB_ACTIONS", "GITLAB_CI", "BUILDKITE",
        "JENKINS_URL", "TEAMCITY_VERSION", "TF_BUILD", "CIRCLECI"};
    for (const char* name : vars) {
        const char* val = std::getenv(name);
        if (val && *val) {
            std::string v(val);
            if (v != "false" && v != "0") {
                return true;
            }
        }
    }
    return false;
}

// True when running inside a container. Cheap file probes; cached once since a
// process cannot migrate in or out of a container.
static bool ComputeContainer()
{
#ifdef __linux__
    if (access("/.dockerenv", F_OK) == 0) {
        return true;
    }
    std::ifstream f("/proc/1/cgroup");
    std::string line;
    while (std::getline(f, line)) {
        if (line.find("docker")     != std::string::npos ||
            line.find("kubepods")   != std::string::npos ||
            line.find("containerd") != std::string::npos ||
            line.find("libpod")     != std::string::npos) {
            return true;
        }
    }
    return false;
#else
    return false;
#endif
}

static std::string GenerateSessionId()
{
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist;
    uint64_t a = dist(gen);
    uint64_t b = dist(gen);
    char buf[37];
    std::snprintf(buf, sizeof(buf), "%08x-%04x-%04x-%04x-%012llx",
                  static_cast<unsigned>(a >> 32),
                  static_cast<unsigned>((a >> 16) & 0xFFFF),
                  static_cast<unsigned>(a & 0xFFFF),
                  static_cast<unsigned>(b >> 48),
                  static_cast<unsigned long long>(b & 0xFFFFFFFFFFFFULL));
    return std::string(buf);
}

// Maximum length of any string property value that leaves the process. This is
// the load-bearing cardinality/PII guard: even if a caller accidentally passes
// a table name, SQL text, or free-form message, only a bounded prefix escapes.
static constexpr size_t kMaxPropertyValueLen = 512;

static void ClampProperty(PropertyValue &v)
{
    if (v.kind == PropertyValue::Kind::String && v.s.size() > kMaxPropertyValueLen) {
        v.s.resize(kMaxPropertyValueLen);
    }
}

// Escape and quote an arbitrary byte string as a JSON string literal. Handles
// embedded NULs and control characters; UTF-8 continuation bytes (>= 0x20) pass
// through untouched. Never throws.
static std::string EscapeJsonString(const std::string& in)
{
    std::string out;
    out.reserve(in.size() + 2);
    out += '"';
    for (unsigned char c : in) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b";  break;
            case '\f': out += "\\f";  break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += static_cast<char>(c);
                }
        }
    }
    out += '"';
    return out;
}

std::string PropertyValue::ToJson() const
{
    switch (kind) {
        case Kind::Bool:
            return b ? "true" : "false";
        case Kind::Int:
            return std::to_string(i);
        case Kind::Double: {
            // JSON has no NaN/Inf; emit 0 rather than invalid JSON.
            if (!(d == d) || d == std::numeric_limits<double>::infinity() ||
                d == -std::numeric_limits<double>::infinity()) {
                return "0";
            }
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%.17g", d);
            return std::string(buf);
        }
        case Kind::Json:
            return s;  // already-valid JSON fragment, emitted verbatim
        case Kind::String:
        default:
            return EscapeJsonString(s);
    }
}

// Serialise a PropertyMap as a JSON object. Shared by event serialization and
// group-set construction.
static std::string PropertyMapToJson(const PropertyMap &props)
{
    std::string json = "{";
    bool first = true;
    for (auto &kv : props) {
        if (!first) {
            json += ",";
        }
        json += EscapeJsonString(kv.first);
        json += ": ";
        json += kv.second.ToJson();
        first = false;
    }
    json += "}";
    return json;
}

std::string PostHogEvent::GetPropertiesJson() const
{
    return PropertyMapToJson(properties);
}

std::string PostHogEvent::GetNowISO8601() const
{
    std::time_t t = std::time(nullptr);
    std::tm tm{};
    // Use UTC (the "Z" suffix asserts UTC) via the reentrant converter — this is
    // now called from multiple threads, so the shared-static std::gmtime/
    // std::localtime would be a data race, and localtime would also be wrong by
    // the machine's timezone offset.
#ifdef _WIN32
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%FT%TZ", &tm);
    return std::string(buffer);
}

// Default ingestion host. Kept at eu.posthog.com until eu.i.posthog.com is
// verified against the project; SetHost() lets callers point elsewhere today.
static const char* const kDefaultHost = "https://eu.posthog.com";

static bool TelemetryDisabledByEnv()
{
    const char* disable_telemetry = std::getenv("DATAZOO_DISABLE_TELEMETRY");
    return disable_telemetry && (std::string(disable_telemetry) == "1" ||
                                 std::string(disable_telemetry) == "true" ||
                                 std::string(disable_telemetry) == "yes");
}

// Coalesced transport: serialise N events into one {api_key, batch:[…]} body
// and POST it to host + "/batch/". This is the only place that touches the
// network. Best-effort; never throws.
void PostHogProcessBatch(const std::string &api_key, const std::string &host,
                         const std::vector<PostHogEvent> &events)
{
    if (TelemetryDisabledByEnv() || events.empty()) {
        return;
    }

    std::string batch;
    for (size_t i = 0; i < events.size(); i++) {
        const PostHogEvent &e = events[i];
        if (i) {
            batch += ",";
        }
        // Prefer the capture-time timestamp; fall back to now for events built
        // without one (e.g. direct PostHogEvent construction in tests).
        const std::string ts = e.timestamp.empty() ? e.GetNowISO8601() : e.timestamp;
        batch += "{\"event\":"        + EscapeJsonString(e.event_name);
        batch += ",\"distinct_id\":"  + EscapeJsonString(e.distinct_id);
        batch += ",\"properties\":"   + e.GetPropertiesJson();
        batch += ",\"timestamp\":"    + EscapeJsonString(ts);
        batch += "}";
    }
    std::string payload = "{\"api_key\":" + EscapeJsonString(api_key) +
                          ",\"batch\":[" + batch + "]}";

    try {
        std::string h = host.empty() ? kDefaultHost : host;
        auto cli = duckdb_httplib_openssl::Client(h.c_str());
        if (cli.is_valid() == false) {
            return;
        }
        // Short timeouts so the worker thread doesn't outlive DuckDB's shutdown
        // sequence.  In short-lived processes (smoke tests, unit tests) the SSL
        // teardown in dlclose() context crashes if the thread is still mid-request
        // when the extension is unloaded.  Best-effort telemetry is acceptable.
        cli.set_connection_timeout(3);
        cli.set_read_timeout(3);
        cli.set_write_timeout(3);
        auto res = cli.Post("/batch/", payload, "application/json");
        (void)res;
        cli.stop();
    } catch (...) {
        return;
    }
}

// Single-event convenience (kept for back-compat / direct tests).
void PostHogProcess(const std::string api_key, const PostHogEvent &event)
{
    PostHogProcessBatch(api_key, kDefaultHost, {event});
}

// PostHogTelemetry Implementation --------------------------------------------------------

PostHogTelemetry::PostHogTelemetry()
    : _telemetry_enabled(true),
      _shutdown_requested(false),
      _api_key("phc_t3wwRLtpyEmLHYaZCSszG0MqVr74J6wnCrj9D41zk2t"),
      _queue(nullptr)
{  }

PostHogTelemetry::~PostHogTelemetry()
{
    Shutdown();
}

void PostHogTelemetry::Shutdown()
{
    std::shared_ptr<TelemetryTaskQueue<std::vector<PostHogEvent>>> queue;
    {
        std::lock_guard<std::mutex> t(_thread_lock);
        _shutdown_requested = true;
        _telemetry_enabled = false;
        queue = std::move(_queue);
    }
    if (queue) {
        queue->Stop();  // discards pending tasks + joins the worker
    }
}

PostHogTelemetry& PostHogTelemetry::Instance()
{
    static PostHogTelemetry* instance = []() {
        OPENSSL_init_ssl(0, nullptr);
        // Construct (and discard) a client once so httplib's function-local
        // statics — notably the URL-parsing regex in the Client constructor —
        // are initialized before the atexit handler below is registered.
        // Statics initialized later than the handler are destroyed before it
        // runs, and an in-flight POST at process exit would touch them dead.
        // No network I/O happens here; the constructor only parses the URL.
        { duckdb_httplib_openssl::Client warmup("https://eu.posthog.com"); }
        auto *telemetry = new PostHogTelemetry();
        std::atexit(&PostHogTelemetry::ShutdownAtExit);
        return telemetry;
    }();
    return *instance;
}

void PostHogTelemetry::ShutdownAtExit()
{
    Instance().Shutdown();
}

// Must be called under _thread_lock. Starts the background worker queue lazily.
void PostHogTelemetry::EnsureQueueInitialized()
{
    if (!_queue) {
        _queue = std::make_shared<TelemetryTaskQueue<std::vector<PostHogEvent>>>();
    }
}

void PostHogTelemetry::BufferEvent(const PostHogEvent &enriched)
{
    {
        std::lock_guard<std::mutex> t(_thread_lock);
        if (_shutdown_requested || !_telemetry_enabled) {
            return;
        }
        EnsureQueueInitialized();
    }
    std::lock_guard<std::mutex> b(_batch_lock);
    _pending.push_back(enriched);
}

void PostHogTelemetry::EnqueueTelemetryEvent(const PostHogEvent &event)
{
    // Piggyback: drain any pending function aggregates into the same batch so
    // they ride along with promptly-sent regular events. This ships function
    // stats for interleaved workloads without waiting for the volume threshold
    // or an explicit Flush(). (FlushFunctionAggregates uses BufferEvent, not
    // this method, so there's no recursion.)
    if (_auto_flush.load()) {
        BufferFunctionAggregates();
    }

    // Merge the common envelope in exactly one place (EnrichEvent/BuildEnvelope
    // acquire _thread_lock themselves), buffer it, then send promptly.
    BufferEvent(EnrichEvent(event));

    // Send right away (coalescing any burst on the worker), matching the
    // original per-event promptness so short-lived embedders don't lose events.
    // Tests disable auto-flush to buffer and drain deterministically.
    if (_auto_flush.load()) {
        FlushBatch();
    }
}

void PostHogTelemetry::FlushBatch()
{
    std::vector<PostHogEvent> batch;
    {
        std::lock_guard<std::mutex> b(_batch_lock);
        if (_pending.empty()) {
            return;
        }
        batch.swap(_pending);
    }

    std::string api_key, host;
    std::function<void(const std::string&, const std::string&,
                       const std::vector<PostHogEvent>&)> transport;
    {
        std::lock_guard<std::mutex> t(_thread_lock);
        if (_shutdown_requested) {
            return;  // never start a new send during shutdown (batch is discarded)
        }
        api_key   = _api_key;
        host      = _host.empty() ? kDefaultHost : _host;
        transport = _transport;
        EnsureQueueInitialized();
        _queue->EnqueueTask(
            [api_key, host, transport](std::vector<PostHogEvent> b) {
                if (transport) {
                    transport(api_key, host, b);
                } else {
                    PostHogProcessBatch(api_key, host, b);
                }
            },
            std::move(batch));
    }
}

bool PostHogTelemetry::DetectCI()
{
    return ComputeCI();
}

bool PostHogTelemetry::DetectContainer()
{
    static bool cached = ComputeContainer();
    return cached;
}

const std::string& PostHogTelemetry::DetectOs()
{
    static const std::string os = ComputeOs();
    return os;
}

const std::string& PostHogTelemetry::DetectArch()
{
    static const std::string arch = ComputeArch();
    return arch;
}

std::string PostHogTelemetry::GetSessionId()
{
    static std::string id = GenerateSessionId();
    return id;
}

void PostHogTelemetry::SetProduct(const std::string& name,
                                  const std::string& version,
                                  const std::string& edition)
{
    std::lock_guard<std::mutex> t(_thread_lock);
    _product = name;
    _product_version = version;
    _product_edition = edition;
}

PropertyMap PostHogTelemetry::BuildEnvelope() const
{
    std::string product, product_version, product_edition, duckdb_version, platform;
    std::string groups_json;
    {
        std::lock_guard<std::mutex> t(_thread_lock);
        product         = _product.empty() ? _extension_name : _product;
        product_version = _product_version;
        product_edition = _product_edition.empty() ? "oss" : _product_edition;
        duckdb_version  = _duckdb_version.empty() ? "unknown" : _duckdb_version;
        platform        = _duckdb_platform.empty() ? DetectPlatform() : _duckdb_platform;
        if (!_groups.empty()) {
            groups_json = "{";
            bool first = true;
            for (auto &g : _groups) {
                if (!first) groups_json += ",";
                groups_json += EscapeJsonString(g.first) + ":" + EscapeJsonString(g.second);
                first = false;
            }
            groups_json += "}";
        }
    }

    PropertyMap env;
    env["product"]          = product;
    env["product_version"]  = product_version;
    env["product_edition"]  = product_edition;
    env["duckdb_version"]   = duckdb_version;
    env["os"]               = DetectOs();
    env["arch"]             = DetectArch();
    env["platform"]         = platform;
    env["is_ci"]            = DetectCI();          // JSON bool
    env["is_container"]     = DetectContainer();   // JSON bool
    env["telemetry_schema"] = 2;                   // JSON number
    env["$session_id"]      = GetSessionId();
    if (!groups_json.empty()) {
        env["$groups"] = PropertyValue::Json(groups_json);  // nested object
    }
    return env;
}

PostHogEvent PostHogTelemetry::EnrichEvent(const PostHogEvent &event) const
{
    PostHogEvent enriched = event;
    // Stamp the capture time now so buffered/coalesced events keep their real
    // occurrence time instead of the flush time. Respect a pre-set timestamp.
    if (enriched.timestamp.empty()) {
        enriched.timestamp = enriched.GetNowISO8601();
    }
    PropertyMap env = BuildEnvelope();
    // Envelope fills only keys the event did not already set: event-specific
    // properties win on collision.
    for (auto &kv : env) {
        enriched.properties.emplace(kv.first, kv.second);
    }
    for (auto &kv : enriched.properties) {
        ClampProperty(kv.second);
    }
    return enriched;
}

PostHogEvent PostHogTelemetry::BuildEventForTesting(const std::string& event_name,
                                                    PropertyMap props)
{
    PostHogEvent event = { event_name, GetDistinctId(), std::move(props), "" };
    return EnrichEvent(event);
}

void PostHogTelemetry::Capture(const std::string& event, PropertyMap props)
{
    // The single choke point for the runtime enabled flag; all the convenience
    // wrappers route through here. The DATAZOO_DISABLE_TELEMETRY opt-out is
    // enforced at the transport (PostHogProcessBatch) so it is a hard
    // "nothing leaves the machine" guarantee regardless of this path.
    if (!_telemetry_enabled) {
        return;
    }
    PostHogEvent ev = { event, GetDistinctId(), std::move(props), "" };
    EnqueueTelemetryEvent(ev);
}

void PostHogTelemetry::CaptureFeature(const std::string& feature, PropertyMap props)
{
    props["feature"] = feature;
    Capture("feature_used", std::move(props));
}

void PostHogTelemetry::CaptureError(const std::string& error_class, PropertyMap props)
{
    // error_class must be an enumerated class, never a free-form message; the
    // length clamp is a backstop but callers are responsible for the contract.
    props["error_class"] = error_class;
    Capture("$exception", std::move(props));
}

void PostHogTelemetry::AssociateGroup(const std::string& type,
                                      const std::string& key,
                                      PropertyMap props)
{
    // Bound the set values (defence-in-depth against accidental PII).
    for (auto& kv : props) {
        ClampProperty(kv.second);
    }

    // Record the mapping so later events carry $groups even if we don't emit a
    // $groupidentify right now.
    {
        std::lock_guard<std::mutex> t(_thread_lock);
        _groups[type] = key;
    }

    // Only mark the group identified when we can actually emit the
    // $groupidentify; otherwise a disabled-at-associate-time group would be
    // permanently suppressed and never re-identified once telemetry is enabled.
    if (!_telemetry_enabled) {
        return;
    }

    bool need_identify = false;
    {
        std::lock_guard<std::mutex> t(_thread_lock);
        // The 0x1f unit separator can't appear in a well-formed type/key.
        std::string composite = type + "\x1f" + key;
        need_identify = _identified_groups.insert(composite).second;
    }

    if (need_identify) {
        PropertyMap gi;
        gi["$group_type"] = type;
        gi["$group_key"]  = key;
        gi["$group_set"]  = PropertyValue::Json(PropertyMapToJson(props));
        Capture("$groupidentify", std::move(gi));
    }
}

void PostHogTelemetry::CaptureExtensionLoad(const std::string& extension_name,
                                            const std::string& extension_version)
{
    // Store extension name as default for CaptureFunctionExecution
    SetExtensionName(extension_name);

    // Attribute this install to a `deployment` group (machine hash) so
    // deployment-level analytics work out of the box, no call-site edits.
    AssociateGroup("deployment", GetDistinctId());

    PropertyMap props;
    props["extension_name"]     = extension_name;
    props["extension_version"]  = extension_version;
    props["extension_platform"] = GetDuckDBPlatform();

    Capture("extension_loaded", props);   // new schema name
    Capture("extension_load", props);     // legacy dual-emit for one release
}

void PostHogTelemetry::CaptureApplicationStart(const std::string& app_name,
                                               const std::string& app_version)
{
    PropertyMap props;
    props["app_name"]    = app_name;
    props["app_version"] = app_version;
    Capture("application_start", props);
}

void PostHogTelemetry::CaptureApplicationStop(const std::string& app_name,
                                              const std::string& app_version)
{
    PropertyMap props;
    props["app_name"]    = app_name;
    props["app_version"] = app_version;
    Capture("application_stop", props);
}

// Overload 1: Explicit extension_name.
// Deprecated shim: per-call function events were a firehose. Route into the
// aggregator; one aggregated `function_executed` event per function is emitted
// on flush (see FlushFunctionAggregates). extension_name/function_version are
// no longer carried by the aggregated event.
void PostHogTelemetry::CaptureFunctionExecution(const std::string& function_name,
                                                const std::string& /*extension_name*/,
                                                const std::string& /*function_version*/)
{
    RecordFunctionCall(function_name);
}

// Overload 2: Uses stored default extension_name
void PostHogTelemetry::CaptureFunctionExecution(const std::string& function_name,
                                                const std::string& /*function_version*/)
{
    RecordFunctionCall(function_name);
}

void PostHogTelemetry::SetSampling(double rate)
{
    if (rate < 0.0) rate = 0.0;
    if (rate > 1.0) rate = 1.0;
    std::lock_guard<std::mutex> lock(_agg_lock);
    _sampling_rate = rate;
    // Derive the integer decimation stride once and stamp the *effective* rate
    // (1/stride), not the requested rate, so scaled-up counts are exact.
    if (rate >= 1.0) {
        _sample_stride = 1;
        _effective_sample_rate = 1.0;
    } else if (rate <= 0.0) {
        _sample_stride = 0;            // 0 => drop everything
        _effective_sample_rate = 0.0;
    } else {
        // Clamp before the double->uint64 cast: a tiny rate makes 1/rate exceed
        // UINT64_MAX, where the cast is UB and can wrap to 0 -> stride 1 -> a
        // full firehose (the opposite of what was asked). Cap the stride instead.
        double inv = 1.0 / rate + 0.5;
        static constexpr double kMaxStride = 1e15;  // well within uint64, ~none recorded
        if (inv > kMaxStride) inv = kMaxStride;
        uint64_t stride = static_cast<uint64_t>(inv);
        if (stride < 1) stride = 1;
        _sample_stride = stride;
        _effective_sample_rate = 1.0 / static_cast<double>(stride);
    }
}

// Recorded-call count that triggers a volume-based aggregate flush, so
// function-heavy or capture-free workloads still ship stats without waiting for
// an explicit Flush().
static constexpr uint64_t kAggFlushThreshold = 256;

void PostHogTelemetry::RecordFunctionCall(const std::string& function_name,
                                          double duration_ms)
{
    if (!_telemetry_enabled) {
        return;
    }

    bool flush_now = false;
    {
        std::lock_guard<std::mutex> lock(_agg_lock);

        // Per-function decimation counter that PERSISTS across flushes (a
        // separate map from the drained stats), so decimation stays accurate
        // and doesn't reset every flush; per function keeps it unbiased across
        // interleaved functions.
        uint64_t seen = ++_sample_seen[function_name];

        if (_sample_stride != 1) {
            if (_sample_stride == 0) {
                return;  // rate 0 => record nothing (and DON'T create a stat entry)
            }
            if ((seen - 1) % _sample_stride != 0) {
                return;
            }
        }

        // Only create/touch the stat entry when the call is actually recorded,
        // so dropped calls never leave count==0 entries in the aggregator.
        static constexpr size_t kMaxDurationSamples = 256;
        auto& st = _function_stats[function_name];
        st.count++;
        if (st.duration_samples.size() < kMaxDurationSamples) {
            st.duration_samples.push_back(duration_ms);
        } else {
            st.duration_samples[st.count % kMaxDurationSamples] = duration_ms;
        }

        if (++_recorded_since_flush >= kAggFlushThreshold) {
            flush_now = true;
        }
    }

    if (flush_now && _auto_flush.load()) {
        FlushFunctionAggregates();  // outside _agg_lock (it re-locks)
    }
}

static double MedianOf(std::vector<double> v)
{
    if (v.empty()) {
        return 0.0;
    }
    std::sort(v.begin(), v.end());
    size_t n = v.size();
    return (n % 2) ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2.0;
}

std::vector<PostHogEvent> PostHogTelemetry::BuildFunctionAggregateEvents()
{
    std::map<std::string, FunctionStat> snapshot;
    double sample_rate;
    {
        std::lock_guard<std::mutex> lock(_agg_lock);
        snapshot.swap(_function_stats);
        _recorded_since_flush = 0;
        sample_rate = _effective_sample_rate;  // 1/stride, not the requested rate
    }

    std::string distinct = GetDistinctId();
    std::string extension_name = GetExtensionName();  // continuity dimension
    std::vector<PostHogEvent> events;
    events.reserve(snapshot.size());
    for (auto& kv : snapshot) {
        PropertyMap props;
        props["function_name"]   = kv.first;
        props["call_count"]      = static_cast<int64_t>(kv.second.count);
        props["duration_ms_p50"] = MedianOf(kv.second.duration_samples);
        if (!extension_name.empty()) {
            props["extension_name"] = extension_name;
        }
        if (sample_rate < 1.0) {
            props["sample_rate"] = sample_rate;
        }
        // Only the new `function_executed` name. We deliberately do NOT dual-emit
        // the legacy `function_execution`: aggregation changes its shape from
        // per-call to per-function-count, so reusing the old name would silently
        // corrupt count-based dashboards (worse than a clean rename).
        events.push_back(PostHogEvent{"function_executed", distinct, std::move(props), ""});
    }
    return events;
}

bool PostHogTelemetry::BufferFunctionAggregates()
{
    auto events = BuildFunctionAggregateEvents();
    if (events.empty()) {
        return false;
    }
    for (auto& ev : events) {
        BufferEvent(EnrichEvent(ev));
    }
    return true;
}

void PostHogTelemetry::FlushFunctionAggregates()
{
    // Buffer all aggregated events, then flush once so a session's function
    // stats coalesce into a single /batch/ POST instead of one POST per event.
    if (BufferFunctionAggregates()) {
        FlushBatch();
    }
}

std::vector<PostHogEvent> PostHogTelemetry::DrainFunctionAggregatesForTesting()
{
    return BuildFunctionAggregateEvents();
}


bool PostHogTelemetry::IsEnabled()
{
    return _telemetry_enabled;
}

void PostHogTelemetry::SetEnabled(bool enabled)
{
    std::lock_guard<std::mutex> t(_thread_lock);
    _telemetry_enabled = enabled;
}

std::string PostHogTelemetry::GetAPIKey()
{
    std::lock_guard<std::mutex> t(_thread_lock);
    return _api_key;
}

void PostHogTelemetry::SetAPIKey(std::string new_key)
{
    std::lock_guard<std::mutex> t(_thread_lock);
    _api_key = new_key;
}

void PostHogTelemetry::SetHost(const std::string& host)
{
    std::lock_guard<std::mutex> t(_thread_lock);
    _host = host;
}

std::string PostHogTelemetry::GetHost()
{
    std::lock_guard<std::mutex> t(_thread_lock);
    return _host.empty() ? kDefaultHost : _host;
}

void PostHogTelemetry::SetTransportForTesting(
    std::function<void(const std::string&, const std::string&,
                       const std::vector<PostHogEvent>&)> fn)
{
    std::lock_guard<std::mutex> t(_thread_lock);
    _transport = std::move(fn);
}

void PostHogTelemetry::SetAutoFlushEnabledForTesting(bool enabled)
{
    _auto_flush.store(enabled);
}

void PostHogTelemetry::Flush()
{
    // Drain the aggregator into the buffer, then coalesce EVERYTHING (aggregates
    // + any buffered events) into ONE send task before draining. Enqueuing a
    // single task means a slow in-flight POST can't strand a second queued task
    // that at-exit Shutdown would then discard.
    BufferFunctionAggregates();
    FlushBatch();

    // Hold a shared_ptr copy so the queue can't be destroyed by a concurrent
    // Shutdown() while we block in DrainFor (avoids a use-after-free).
    std::shared_ptr<TelemetryTaskQueue<std::vector<PostHogEvent>>> q;
    {
        std::lock_guard<std::mutex> t(_thread_lock);
        q = _queue;
    }
    if (q) {
        q->DrainFor(3000);
    }
}

void PostHogTelemetry::SetExtensionName(const std::string& name)
{
    std::lock_guard<std::mutex> t(_thread_lock);
    _extension_name = name;
}

std::string PostHogTelemetry::GetExtensionName()
{
    std::lock_guard<std::mutex> t(_thread_lock);
    return _extension_name;
}

void PostHogTelemetry::SetDuckDBVersion(const std::string& version)
{
    std::lock_guard<std::mutex> t(_thread_lock);
    _duckdb_version = version;
}

void PostHogTelemetry::SetDuckDBPlatform(const std::string& platform)
{
    std::lock_guard<std::mutex> t(_thread_lock);
    _duckdb_platform = platform;
}

std::string PostHogTelemetry::GetDuckDBVersion()
{
    std::lock_guard<std::mutex> t(_thread_lock);
    return _duckdb_version.empty() ? "unknown" : _duckdb_version;
}

std::string PostHogTelemetry::GetDuckDBPlatform()
{
    std::lock_guard<std::mutex> t(_thread_lock);
    return _duckdb_platform.empty() ? DetectPlatform() : _duckdb_platform;
}

std::string PostHogTelemetry::GetMacAddressSafe()
{
    try {
        return GetMacAddress();
    } catch (std::exception &e) {
        return "00:00:00:00:00:00";
    }
}

std::string PostHogTelemetry::Sha256Hex(const std::string& input)
{
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(input.data()),
           input.size(), digest);
    char hex[SHA256_DIGEST_LENGTH * 2 + 1];
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
        snprintf(hex + i * 2, 3, "%02x", digest[i]);
    return std::string(hex, SHA256_DIGEST_LENGTH * 2);
}

std::string PostHogTelemetry::GetDistinctId()
{
    // C++11 static local: initialized once per process, thread-safe
    static std::string cached = ComputeDistinctId();
    return cached;
}

std::string PostHogTelemetry::ComputeDistinctId()
{
    // Platform machine ID → SHA-256 (OS-native IDs are stable by design)
    auto machine_id = GetMachineId();
    if (!machine_id.empty()) {
        return Sha256Hex(machine_id);
    }
    // MAC address fallback → SHA-256
    return Sha256Hex(GetMacAddressSafe());
}

#ifdef __linux__

std::string PostHogTelemetry::GetMachineId()
{
    for (const char* path : {"/etc/machine-id", "/var/lib/dbus/machine-id"}) {
        std::ifstream f(path);
        std::string id;
        if (f >> id && !id.empty()) return id;
    }
    return "";
}

std::string PostHogTelemetry::GetMacAddress()
{
    auto device = FindFirstPhysicalDevice();
    if (device.empty()) {
        return "00:00:00:00:00:00";
    }

    std::ifstream file(FormatStr("/sys/class/net/%s/address", device.c_str()));

    std::string mac_address;
    if (file >> mac_address) {
        return mac_address;
    }

    throw std::runtime_error(FormatStr("Could not read mac address of device %s", device.c_str()));
}

bool PostHogTelemetry::IsPhysicalDevice(const std::string& device) {
    std::string path = FormatStr("/sys/class/net/%s/device/driver", device.c_str());
    return access(path.c_str(), F_OK) != -1;
}

std::string PostHogTelemetry::FindFirstPhysicalDevice()
{
    DIR* dir = opendir("/sys/class/net");
    if (!dir) {
        throw std::runtime_error("Could not open /sys/class/net");
    }

    std::vector<std::string> devices;
    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type == DT_LNK || entry->d_type == DT_DIR) {
            std::string device = entry->d_name;
            if (device != "." && device != "..") {
                devices.push_back(device);
            }
        }
    }
    closedir(dir);

    std::sort(devices.begin(), devices.end());

    for (const std::string& device : devices) {
        if (IsPhysicalDevice(device)) {
            return device;
        }
    }

    return "";
}

#elif _WIN32

std::string PostHogTelemetry::GetMachineId()
{
    HKEY hKey;
    if (RegOpenKeyExA(HKEY_LOCAL_MACHINE,
                      "SOFTWARE\\Microsoft\\Cryptography",
                      0, KEY_READ | KEY_WOW64_64KEY, &hKey) != ERROR_SUCCESS)
        return "";
    char buf[256]; DWORD size = sizeof(buf);
    LSTATUS res = RegQueryValueExA(hKey, "MachineGuid", nullptr,
                                   nullptr, (LPBYTE)buf, &size);
    RegCloseKey(hKey);
    return (res == ERROR_SUCCESS) ? std::string(buf) : "";
}

std::string PostHogTelemetry::GetMacAddress()
{
    ULONG out_buf_len = sizeof(IP_ADAPTER_INFO);
    std::vector<BYTE> buffer(out_buf_len);

    auto adapter_info = reinterpret_cast<PIP_ADAPTER_INFO>(buffer.data());
    if (GetAdaptersInfo(adapter_info, &out_buf_len) == ERROR_BUFFER_OVERFLOW) {
        buffer.resize(out_buf_len);
        adapter_info = reinterpret_cast<PIP_ADAPTER_INFO>(buffer.data());
    }

    DWORD ret = GetAdaptersInfo(adapter_info, &out_buf_len);
    if (ret != NO_ERROR) {
        return "";
    }

    std::vector<std::string> mac_addresses;
    PIP_ADAPTER_INFO adapter = adapter_info;
    while (adapter) {
        std::ostringstream str_buf;
        for (UINT i = 0; i < adapter->AddressLength; i++) {
            str_buf << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(adapter->Address[i]);
            if (i != (adapter->AddressLength - 1)) str_buf << '-';
        }
        mac_addresses.push_back(str_buf.str());
        adapter = adapter->Next;
    }

    return mac_addresses.empty() ? "" : mac_addresses.front();
}

#elif __APPLE__

std::string PostHogTelemetry::GetMachineId()
{
    io_registry_entry_t entry = IORegistryEntryFromPath(
        kIOMainPortDefault, "IOService:/");
    if (!entry) return "";
    CFStringRef uuid_ref = (CFStringRef)IORegistryEntryCreateCFProperty(
        entry, CFSTR("IOPlatformUUID"), kCFAllocatorDefault, 0);
    IOObjectRelease(entry);
    if (!uuid_ref) return "";
    char buf[64];
    bool ok = CFStringGetCString(uuid_ref, buf, sizeof(buf), kCFStringEncodingUTF8);
    CFRelease(uuid_ref);
    return ok ? std::string(buf) : "";
}

std::string PostHogTelemetry::GetMacAddress()
{
    struct ifaddrs *ifap, *ifa;
    struct sockaddr_dl *sdl = nullptr;
    char mac_address[18] = {0};

    if (getifaddrs(&ifap) != 0) {
        throw std::runtime_error("getifaddrs() failed!");
    }

    for (ifa = ifap; ifa; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr->sa_family == AF_LINK && strcmp(ifa->ifa_name, "en0") == 0) {
            sdl = (struct sockaddr_dl *)ifa->ifa_addr;
            break;
        }
    }

    if (sdl && sdl->sdl_alen == 6) {
        unsigned char *ptr = (unsigned char *)LLADDR(sdl);
        snprintf(mac_address, sizeof(mac_address), "%02x:%02x:%02x:%02x:%02x:%02x",
                 ptr[0], ptr[1], ptr[2], ptr[3], ptr[4], ptr[5]);
    } else {
        strncpy(mac_address, "00:00:00:00:00:00", sizeof(mac_address));
    }

    freeifaddrs(ifap);

    return std::string(mac_address);
}

#else

std::string PostHogTelemetry::GetMachineId() { return ""; }

std::string PostHogTelemetry::GetMacAddress()
{
    return "";
}

#endif

} // namespace duckdb
