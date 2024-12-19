#!/usr/bin/env sh

# Script to run format checks valkey-bloom module, build it and generate .so files, run unit and integration tests.

# Exit the script if any command fails
set -e

SCRIPT_DIR=$(pwd)
echo "Script Directory: $SCRIPT_DIR"

echo "Running cargo and clippy format checks..."
cargo fmt --check
cargo clippy --profile release --all-targets -- -D clippy::all

echo "Running cargo build release..."
RUSTFLAGS="-D warnings" cargo build --all --all-targets  --release

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
    make -j
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

export MODULE_PATH="$SCRIPT_DIR/target/release/libvalkey_bloom.so"

echo "Running the integration tests..."
# TEST_PATTERN can be used to run specific tests or test patterns.
if [[ -n "$TEST_PATTERN" ]]; then
    python3 -m pytest --cache-clear -v "$SCRIPT_DIR/tests/" -k $TEST_PATTERN
else
    echo "TEST_PATTERN is not set. Running all integration tests."
    python3 -m pytest --cache-clear -v "$SCRIPT_DIR/tests/"
fi

echo "Build, Format Checks, Unit tests, and Integration Tests succeeded"
