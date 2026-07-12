#define CATCH_CONFIG_RUNNER
#include <cstdlib>
#include <iostream>
#include "catch.hpp"
#include "telemetry.hpp"

int main(int argc, char *argv[]) {
    // Ensure tests never send data to PostHog
#ifdef _WIN32
    _putenv_s("DATAZOO_DISABLE_TELEMETRY", "1");
#else
    setenv("DATAZOO_DISABLE_TELEMETRY", "1", 1);
#endif

    // Disable automatic per-event/threshold flushing so batching and
    // aggregation are driven deterministically by explicit Flush()/Drain in the
    // tests. The dedicated auto-flush test re-enables it locally.
    duckdb::PostHogTelemetry::Instance().SetAutoFlushEnabledForTesting(false);
    // Make all function calls aggregate (no per-call prompt phase) so aggregation
    // tests can assert exact counts. The dedicated prompt test overrides this.
    duckdb::PostHogTelemetry::Instance().SetPromptFunctionCallsForTesting(0);

    std::cout << std::endl << "**** PostHog Telemetry Unit Tests ****" << std::endl << std::endl;

    return Catch::Session().run(argc, argv);
}
