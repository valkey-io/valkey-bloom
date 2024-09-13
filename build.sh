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
cargo build --all --all-targets  --release

# We are waiting on a new feature in the valkey-module-rs to be released which will allow unit testing of Valkey Rust Modules.
# echo "Running unit tests..."
# cargo test

# Ensure VERSION environment variable is set
if [ -z "$VERSION" ]; then
  echo "ERROR: VERSION environment variable is not set."
  exit 1
fi

if [ "$VERSION" != "unstable" ] && [ "$VERSION" != "7.2.6" ] && [ "$VERSION" != "7.2.5" ] ; then
  echo "ERROR: Unsupported version - $VERSION"
  exit 1
fi

REPO_URL="https://github.com/valkey-io/valkey.git"
BINARY_PATH="tests/.build/binaries/$VERSION/valkey-server"

if [ -f "$BINARY_PATH" ] && [ -x "$BINARY_PATH" ]; then
    echo "valkey-server binary '$BINARY_PATH' found."
else
    echo "valkey-server binary '$BINARY_PATH' not found."
    mkdir -p "tests/.build/binaries/$VERSION"
    cd tests/.build
    rm -rf valkey
    git clone "$REPO_URL"
    cd valkey
    git checkout "$VERSION"
    make
    cp src/valkey-server ../binaries/$VERSION/
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
python3 -m pytest --cache-clear -v "$SCRIPT_DIR/tests/"

echo "Build and Integration Tests succeeded"
