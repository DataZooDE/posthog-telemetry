# Makefile for posthog-telemetry

.PHONY: all build test clean help

# Check if Ninja is available
NINJA := $(shell which ninja 2>/dev/null)
CMAKE_GENERATOR := $(if $(NINJA),-G Ninja,)

BUILD_DIR := build

ifeq ($(OS),Windows_NT)
    CMAKE := cmake
    RMDIR := rm -rf
    TEST_BIN := $(BUILD_DIR)/test/cpp/Release/posthog_telemetry_tests.exe
    CMAKE_GENERATOR := -G "Visual Studio 17 2022" -A x64
    CMAKE_EXTRA := -DCMAKE_TOOLCHAIN_FILE="$$VCPKG_INSTALLATION_ROOT/scripts/buildsystems/vcpkg.cmake"
    CMAKE_BUILD_EXTRA := --config Release
else ifeq ($(shell uname),Darwin)
    CMAKE := cmake
    RMDIR := rm -rf
    TEST_BIN := $(BUILD_DIR)/test/cpp/posthog_telemetry_tests
    OPENSSL_PREFIX := $(shell brew --prefix openssl@3 2>/dev/null)
    CMAKE_EXTRA := $(if $(OPENSSL_PREFIX),-DOPENSSL_ROOT_DIR=$(OPENSSL_PREFIX),)
    CMAKE_BUILD_EXTRA :=
else
    CMAKE := cmake
    RMDIR := rm -rf
    TEST_BIN := $(BUILD_DIR)/test/cpp/posthog_telemetry_tests
    CMAKE_EXTRA :=
    CMAKE_BUILD_EXTRA :=
endif

# Default target
all: test

help:
	@echo "Available targets:"
	@echo "  build   - Configure and build library + tests"
	@echo "  test    - Build and run unit tests"
	@echo "  clean   - Remove build artifacts"
	@echo "  help    - Show this help"

build: $(BUILD_DIR)/CMakeCache.txt
	@$(CMAKE) --build $(BUILD_DIR) $(CMAKE_BUILD_EXTRA)

$(BUILD_DIR)/CMakeCache.txt:
	@$(CMAKE) -B $(BUILD_DIR) \
		-DPOSTHOG_BUILD_TESTS=ON \
		-DCMAKE_BUILD_TYPE=Release \
		$(CMAKE_GENERATOR) \
		$(CMAKE_EXTRA)

test: build
	@echo "Running unit tests..."
	@$(TEST_BIN) --reporter console

clean:
	@echo "Cleaning build artifacts..."
	@$(RMDIR) $(BUILD_DIR)
