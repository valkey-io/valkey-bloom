#!/usr/bin/env sh

# Script to run format checks valkey-bloom module, build it and generate .so files, run unit and integration tests.

# Exit the script if any command fails
set -e

SCRIPT_DIR=$(pwd)
echo "Script Directory: $SCRIPT_DIR"

echo "Running cargo and clippy format checks..."
cargo fmt --check
cargo clippy --profile release --all-targets -- -D clippy::all


echo "Running unit tests..."
cargo test --features enable-system-alloc

# Ensure SERVER_VERSION environment variable is set
if [ -z "$SERVER_VERSION" ]; then
    echo "ERROR: SERVER_VERSION environment variable is not set. Defaulting to unstable."
    export SERVER_VERSION="unstable"
fi

if [ "$SERVER_VERSION" != "unstable" ] && [ "$SERVER_VERSION" != "8.0.0" ] ; then
  echo "ERROR: Unsupported version - $SERVER_VERSION"
  exit 1
fi

echo "Running cargo build release..."
if [ "$SERVER_VERSION" == "8.0.0" ] ; then
    RUSTFLAGS="-D warnings" cargo build --all --all-targets  --release --features valkey_8_0
else
    RUSTFLAGS="-D warnings" cargo build --all --all-targets  --release
fi


REPO_URL="https://github.com/valkey-io/valkey.git"
BINARY_PATH="tests/.build/binaries/$SERVER_VERSION/valkey-server"

if [ -f "$BINARY_PATH" ] && [ -x "$BINARY_PATH" ]; then
    echo "valkey-server binary '$BINARY_PATH' found."
else
    echo "valkey-server binary '$BINARY_PATH' not found."
    mkdir -p "tests/.build/binaries/$SERVER_VERSION"
    cd tests/.build
    rm -rf valkey
    git clone "$REPO_URL"
    cd valkey
    git checkout "$SERVER_VERSION"
    make distclean
    if [ ! -z "${ASAN_BUILD}" ]; then
        make -j SANITIZER=address
    else
        make -j
    fi
    cp src/valkey-server ../binaries/$SERVER_VERSION/
fi

REQUIREMENTS_FILE="requirements.txt"

# Check if pip is available
if command -v pip > /dev/null 2>&1; then
    echo "Using pip to install packages..."
    pip install -r "$SCRIPT_DIR/$REQUIREMENTS_FILE"
# Check if pip3 is available
elif command -v pip3 > /dev/null 2>&1; then
    echo "Using pip3 to install packages..."
    pip3 install -r "$SCRIPT_DIR/$REQUIREMENTS_FILE"
else
    echo "Error: Neither pip nor pip3 is available. Please install Python package installer."
    exit 1
fi

os_type=$(uname)
MODULE_EXT=".so"
if [[ "$os_type" == "Darwin" ]]; then
  MODULE_EXT=".dylib"
elif [[ "$os_type" == "Linux" ]]; then
  MODULE_EXT=".so"
elif [[ "$os_type" == "Windows" ]]; then
  MODULE_EXT=".dll"
else
  echo "Unsupported OS type: $os_type"
  exit 1
fi
export MODULE_PATH="$SCRIPT_DIR/target/release/libvalkey_bloom$MODULE_EXT"

echo "Running the integration tests..."
if [ ! -z "${ASAN_BUILD}" ]; then
    # TEST_PATTERN can be used to run specific tests or test patterns.
    if [[ -n "$TEST_PATTERN" ]]; then
        python3 -m pytest --capture=sys --cache-clear -v "$SCRIPT_DIR/tests/" -k $TEST_PATTERN 2>&1 | tee test_output.tmp
    else
        echo "TEST_PATTERN is not set. Running all integration tests."
        python3 -m pytest --capture=sys --cache-clear -v "$SCRIPT_DIR/tests/" 2>&1 | tee test_output.tmp
    fi

    # Check for memory leaks in the output
    if grep -q "LeakSanitizer: detected memory leaks" test_output.tmp; then
        RED='\033[0;31m'
        echo -e "${RED}Memory leaks detected in the following tests:"
        LEAKING_TESTS=$(grep -B 2 "LeakSanitizer: detected memory leaks" test_output.tmp | \
                        grep -v "LeakSanitizer" | \
                        grep ".*\.py::")
        
        LEAK_COUNT=$(echo "$LEAKING_TESTS" | wc -l)
        
        # Output each leaking test
        echo "$LEAKING_TESTS" | while read -r line; do
            echo "::error::Test with leak: $line"
        done
        
        echo -e "\n$LEAK_COUNT python integration tests have leaks detected in them"
        rm test_output.tmp
        exit 1
    fi


    rm test_output.tmp
else 
    # TEST_PATTERN can be used to run specific tests or test patterns.
    if [[ -n "$TEST_PATTERN" ]]; then
        python3 -m pytest --cache-clear -v "$SCRIPT_DIR/tests/" -k $TEST_PATTERN
    else
        echo "TEST_PATTERN is not set. Running all integration tests."
        python3 -m pytest --cache-clear -v "$SCRIPT_DIR/tests/"
    fi
fi

echo "Build, Format Checks, Unit tests, and Integration Tests succeeded"
