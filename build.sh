#!/usr/bin/env sh

# Script to run format checks valkey-bloom module, build it and generate .so files, run unit and integration tests.

# Exit the script if any command fails
set -e

SCRIPT_DIR=$(pwd)
echo "Script Directory: $SCRIPT_DIR"

if [ "$1" = "clean" ]; then
  echo "Cleaning build artifacts"
  rm -rf target/
  rm -rf tests/build/
  rm -rf test-data/
  echo "Clean completed."
  exit 0
fi

# Ensure SERVER_VERSION environment variable is set
if [ -z "$SERVER_VERSION" ]; then
    echo "ERROR: SERVER_VERSION environment variable is not set. Defaulting to unstable."
    export SERVER_VERSION="unstable"
fi

REPO_URL="https://github.com/valkey-io/valkey.git"

MIN_SERVER_MAJOR="8"

# Validate SERVER_VERSION against the upstream valkey release branches when the
# remote is reachable. Capture git ls-remote explicitly (not inside a pipeline,
# whose trailing commands would mask its failure) and check its exit status.
# If the remote is unreachable (offline), warn and skip validation instead of
# aborting: a cached binary at $BINARY_PATH (or the later git clone) is the real
# source of truth for whether this version can actually be built.
if ! REMOTE_REFS=$(git ls-remote --heads "$REPO_URL" 2>/dev/null); then
  echo "WARNING: could not reach $REPO_URL to validate SERVER_VERSION; skipping version check" >&2
else
  RELEASE_VERSIONS=$(printf '%s\n' "$REMOTE_REFS" \
    | sed -E 's#.*refs/heads/##' \
    | grep -E '^[0-9]+\.[0-9]+$' \
    | awk -F. -v M="$MIN_SERVER_MAJOR" '$1>=M' \
    | sort -V)
  VALID_VERSIONS=$(printf 'unstable\n%s\n' "$RELEASE_VERSIONS")
  if ! printf '%s\n' "$VALID_VERSIONS" | grep -qxF "$SERVER_VERSION"; then
    echo "ERROR: Unsupported version - $SERVER_VERSION"
    echo "Valid versions are: $(printf '%s\n' "$VALID_VERSIONS" | sort -V | tr '\n' ' ')"
    exit 1
  fi
fi

echo "Running cargo and clippy format checks..."
cargo fmt --check
cargo clippy --profile release --all-targets -- -D clippy::all


echo "Running unit tests..."
cargo test --features enable-system-alloc

echo "Running cargo build release..."
if [ "$SERVER_VERSION" == "8.0" ] ; then
    RUSTFLAGS="-D warnings" cargo build --all --all-targets  --release --features valkey_8_0
else
    RUSTFLAGS="-D warnings" cargo build --all --all-targets  --release
fi


BINARY_PATH="tests/build/binaries/$SERVER_VERSION/valkey-server"
CACHED_VALKEY_PATH="tests/build/valkey"

# Optional: path to an externally built valkey-server binary. When set,
# we skip building Valkey from source and copy the provided binary into
# the integration test directory instead.
if [ -n "$VALKEY_SERVER_PATH" ]; then
    if [ ! -f "$VALKEY_SERVER_PATH" ] || [ ! -x "$VALKEY_SERVER_PATH" ]; then
        echo "ERROR: VALKEY_SERVER_PATH is set but file does not exist or is not executable: $VALKEY_SERVER_PATH"
        exit 1
    fi
    echo "Using external valkey-server binary: $VALKEY_SERVER_PATH"
    mkdir -p "tests/build/binaries/$SERVER_VERSION"
    cp "$VALKEY_SERVER_PATH" "$BINARY_PATH"
fi

if [ -f "$BINARY_PATH" ] && [ -x "$BINARY_PATH" ]; then
    echo "valkey-server binary '$BINARY_PATH' found."
else
    echo "valkey-server binary '$BINARY_PATH' not found."
    mkdir -p "tests/build/binaries/$SERVER_VERSION"
    rm -rf $CACHED_VALKEY_PATH
    cd tests/build
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
    cd $SCRIPT_DIR
    rm -rf $CACHED_VALKEY_PATH
fi


TEST_FRAMEWORK_REPO="https://github.com/valkey-io/valkey-test-framework"
TEST_FRAMEWORK_DIR="tests/build/valkeytestframework"

if [ -d "$TEST_FRAMEWORK_DIR" ]; then
    echo "valkeytestframework found."
else
    echo "Cloning valkey-test-framework..."
    git clone "$TEST_FRAMEWORK_REPO"
    mkdir -p "$TEST_FRAMEWORK_DIR"
    mv "valkey-test-framework/src"/* "$TEST_FRAMEWORK_DIR/"
    rm -rf valkey-test-framework
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
if [ "$os_type" = "Darwin" ]; then
  MODULE_EXT=".dylib"
elif [ "$os_type" = "Linux" ]; then
  MODULE_EXT=".so"
else
  echo "Unsupported OS type: $os_type"
  exit 1
fi
export MODULE_PATH="$SCRIPT_DIR/target/release/libvalkey_bloom$MODULE_EXT"

echo "Running the integration tests..."
if [ ! -z "${ASAN_BUILD}" ]; then
    # TEST_PATTERN can be used to run specific tests or test patterns.
    if [ -n "$TEST_PATTERN" ]; then
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
    if [ -n "$TEST_PATTERN" ]; then
        python3 -m pytest --cache-clear -v "$SCRIPT_DIR/tests/" -k $TEST_PATTERN
    else
        echo "TEST_PATTERN is not set. Running all integration tests."
        python3 -m pytest --cache-clear -v "$SCRIPT_DIR/tests/"
    fi
fi

echo "Build, Format Checks, Unit tests, and Integration Tests succeeded"
