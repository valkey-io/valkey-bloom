# valkey-bloom

Valkey-Bloom (BSD-3-Clause) is a Rust based Valkey-Module which brings a Bloom Filter (Module) data type into Valkey and supports verions >= 8.0. With this, users can create bloom filters (space efficient probabilistic data structures) to add elements, check whether elements exists, auto scale their filters, customize bloom filter properties, perform RDB Save and load operations, etc.

Valkey-Bloom is built using `bloomfilter::Bloom` (https://crates.io/crates/bloomfilter which has a BSD-2-Clause license).

It is API compatible with the bloom filter command syntax of the official Valkey client libraries including valkey-py, valkey-java, valkey-go (as well as the equivalent Redis libraries)

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
# Building for Valkey 8.1 and above:
cargo build --release
# Building for Valkey 8.0 specifically:
cargo build --release --features valkey_8_0
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
# Build with asan, you may need to remove the old valkey binary if you have used ./build.sh before. You can do this by deleting the `.build` folder in the `tests` folder 
ASAN_BUILD=true
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
## Feature Flags

* valkey_8_0: valkey-bloom is intended to be loaded on server versions >= Valkey 8.1 and by default it is built this way (unless this flag is provided). It is however compatible with Valkey version 8.0 if the user explicitly provides this feature flag in their cargo build command.
```
cargo build --release --features valkey_8_0
```

This can also be done by specifiyng SERVER_VERSION=8.0.0 and then running `./build.sh`