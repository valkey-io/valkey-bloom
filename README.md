# valkey-bloom

Valkey-Bloom (BSD-3-Clause) is a Rust Valkey-Module which brings a native and space efficient probabilistic Module data type to Valkey. With this, users can create filters (space-efficient probabilistic Module data type) to add elements, perform “check” operation to test whether an element exists, auto scale their filters, perform RDB Save and load operations, etc.

Valkey-Bloom is built using bloomfilter::Bloom (https://crates.io/crates/bloomfilter which has a BSD-2-Clause license).

It is compatible with the BloomFilter (BF.*) command APIs in Redis offerings.

## Supported commands
```
BF.EXISTS
BF.ADD
BF.MEXISTS
BF.MADD
BF.CARD
BF.RESERVE
BF.INFO
BF.INSERT
BF.LOAD
```

## Build instructions
```
curl https://sh.rustup.rs -sSf | sh
sudo yum install clang
git clone https://github.com/valkey-io/valkey-bloom.git
cd valkey-bloom
cargo build --all --all-targets  --release
valkey-server --loadmodule ./target/release/libvalkey_bloom.so
```

#### Local development script to build, run format checks, run unit / integration tests, and for cargo release:
```
# Builds the valkey-server (unstable) for integration testing.
SERVER_VERSION=unstable
./build.sh
# Same as above, but uses valkey-server (8.0.0) for integration testing.
SERVER_VERSION=8.0.0
./build.sh
```

## Load the Module
To test the module with a Valkey, you can load the module in the following ways:

#### Using valkey.conf:
```
1. Add the following to valkey.conf:
    loadmodule /path/to/libvalkey_bloom.so
2. Start valkey-server:
    valkey-server /path/to/valkey.conf
```

#### Starting Valkey with the `--loadmodule` option:
```text
valkey-server --loadmodule /path/to/libvalkey_bloom.so
```

#### Using the Valkey command `MODULE LOAD`:
```
1. Connect to a running Valkey instance using valkey-cli
2. Execute Valkey command:
    MODULE LOAD /path/to/libvalkey_bloom.so
```