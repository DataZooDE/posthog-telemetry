#define NOMINMAX

#include "telemetry.hpp"

#include <algorithm>
#include <cstdarg>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <string>
#include <vector>

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

std::string PostHogEvent::GetPropertiesJson() const
{
    std::string json = "{";
    bool first = true;
    for (auto &kv : properties)
    {
        if (!first) {
            json += ",";
        }
        json += FormatStr("\"%s\": \"%s\"", kv.first.c_str(), kv.second.c_str());
        first = false;
    }
    json += "}";
    return json;
}

std::string PostHogEvent::GetNowISO8601() const
{
    std::time_t t = std::time(nullptr);
    std::tm tm = *std::localtime(&t);
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%FT%TZ", &tm);
    return std::string(buffer);
}

// Free function for processing events (exposed for testing)
void PostHogProcess(const std::string api_key, const PostHogEvent &event)
{
    // Check if telemetry is disabled via environment variable
    const char* disable_telemetry = std::getenv("DATAZOO_DISABLE_TELEMETRY");
    if (disable_telemetry && (std::string(disable_telemetry) == "1" ||
                              std::string(disable_telemetry) == "true" ||
                              std::string(disable_telemetry) == "yes")) {
        return; // Skip telemetry
    }

    std::string payload = FormatStr(R"(
        {
            "api_key": "%s",
            "batch": [{
                "event": "%s",
                "distinct_id": "%s",
                "properties": %s,
                "timestamp": "%s"
            }]
        }
    )", api_key.c_str(), event.event_name.c_str(), event.distinct_id.c_str(),
        event.GetPropertiesJson().c_str(), event.GetNowISO8601().c_str());

    try {
        auto cli = duckdb_httplib_openssl::Client("https://eu.posthog.com");
        if (cli.is_valid() == false) {
            // Silently fail instead of throwing - telemetry should not break the application
            return;
        }
        auto url = "/batch/";
        auto res = cli.Post(url, payload, "application/json");
        if (res && res->status != 200) {
            // Silently fail instead of throwing - telemetry should not break the application
            return;
        }
        cli.stop();
    } catch (...) {
        // Silently fail instead of throwing - telemetry should not break the application
        return;
    }
}

// PostHogTelemetry Implementation --------------------------------------------------------

PostHogTelemetry::PostHogTelemetry()
    : _telemetry_enabled(true),
      _api_key("phc_t3wwRLtpyEmLHYaZCSszG0MqVr74J6wnCrj9D41zk2t"),
      _queue(nullptr)
{  }

PostHogTelemetry::~PostHogTelemetry()
{
    if (_queue) {
        _queue->Stop();
    }
}

PostHogTelemetry& PostHogTelemetry::Instance()
{
    static PostHogTelemetry instance;
    return instance;
}

void PostHogTelemetry::EnsureQueueInitialized()
{
    if (!_queue) {
        _queue = std::make_unique<TelemetryTaskQueue<PostHogEvent>>();
    }
}

void PostHogTelemetry::CaptureExtensionLoad(const std::string& extension_name,
                                            const std::string& extension_version)
{
    // Store extension name as default for CaptureFunctionExecution
    SetExtensionName(extension_name);

    if (!_telemetry_enabled) {
        return;
    }

    PostHogEvent event = {
        "extension_load",
        GetDistinctId(),
        {
            {"extension_name", extension_name},
            {"extension_version", extension_version},
            {"extension_platform", GetDuckDBPlatform()},
            {"duckdb_version", GetDuckDBVersion()}
        }
    };

    auto api_key = this->_api_key;
    EnsureQueueInitialized();
    _queue->EnqueueTask([api_key](auto event) { PostHogProcess(api_key, event); }, event);
}

// Overload 1: Explicit extension_name
void PostHogTelemetry::CaptureFunctionExecution(const std::string& function_name,
                                                const std::string& extension_name,
                                                const std::string& function_version)
{
    if (!_telemetry_enabled) {
        return;
    }

    PostHogEvent event = {
        "function_execution",
        GetDistinctId(),
        {
            {"function_name", function_name},
            {"function_version", function_version},
            {"extension_name", extension_name},
            {"extension_platform", GetDuckDBPlatform()},
            {"duckdb_version", GetDuckDBVersion()}
        }
    };
    auto api_key = this->_api_key;
    EnsureQueueInitialized();
    _queue->EnqueueTask([api_key](auto event) { PostHogProcess(api_key, event); }, event);
}

// Overload 2: Uses stored default extension_name
void PostHogTelemetry::CaptureFunctionExecution(const std::string& function_name,
                                                const std::string& function_version)
{
    CaptureFunctionExecution(function_name, GetExtensionName(), function_version);
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
