#define CATCH_CONFIG_RUNNER
#include <cstdlib>
#include <iostream>
#include "catch.hpp"

int main(int argc, char *argv[]) {
    // Ensure tests never send data to PostHog
#ifdef _WIN32
    _putenv_s("DATAZOO_DISABLE_TELEMETRY", "1");
#else
    setenv("DATAZOO_DISABLE_TELEMETRY", "1", 1);
#endif

    std::cout << std::endl << "**** PostHog Telemetry Unit Tests ****" << std::endl << std::endl;

    return Catch::Session().run(argc, argv);
}
