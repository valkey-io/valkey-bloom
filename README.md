# valkey-bloom

Valkey-Bloom (BSD-3-Clause) is a Rust Valkey-Module which brings a native and space efficient probabilistic Module data type to Valkey. With this, users can create filters (space-efficient probabilistic Module data type) to add elements, perform “check” operation to test whether an element exists, auto scale their filters, perform RDB Save and load operations, etc.

Valkey-Bloom is built using bloomfilter::Bloom (https://crates.io/crates/bloomfilter which has a BSD-2-Clause license).

It is compatible with the BloomFilter (BF.*) command APIs of the ReBloom Module from Redis Ltd.

The following commands are supported.
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
BF.DUMP
```

Build instructions for Linux.
```
curl https://sh.rustup.rs -sSf | sh
sudo yum install clang
git clone https://github.com/KarthikSubbarao/valkey-bloom.git
cd valkey-bloom
cargo build --all --all-targets  --release
valkey-server --loadmodule ./target/release/libvalkey_bloom.so
```

Local development script to build, run format checks, run unit / integration tests, and for cargo release:
```
# Builds the valkey-server (unstable) for integration testing.
SERVER_VERSION=unstable
./build.sh
# Builds the valkey-server (8.0.0) for integration testing.
SERVER_VERSION=8.0.0
./build.sh
```

Client Usage
```
<redacted> % ./valkey-cli 
127.0.0.1:6379> module list
1) 1) "name"
   2) "bloom"
   3) "ver"
   4) (integer) 1
   5) "path"
   6) "./target/release/libvalkey_bloom.so"
   7) "args"
   8) (empty array)
127.0.0.1:6379> bf.add key item
(integer) 1
127.0.0.1:6379> bf.exists key item
(integer) 1
127.0.0.1:6379> bf.exists key item2
(integer) 0
127.0.0.1:6379> bf.card key
(integer) 1
127.0.0.1:6379> bf.reserve key 0.01 10000
(error) ERR item exists
127.0.0.1:6379> bf.reserve key1 0.01 10000
OK
127.0.0.1:6379> bf.card key1
(integer) 0
127.0.0.1:6379> bf.add key1 item
(integer) 1
127.0.0.1:6379> bf.card key1
(integer) 1
```

```
127.0.0.1:6379> bf.reserve key1 0.01 10000
OK
127.0.0.1:6379> bf.info key3
(empty array)
127.0.0.1:6379> bf.info key1
 1) Capacity
 2) (integer) 10000
 3) Size
 4) (integer) 12198
 5) Number of filters
 6) (integer) 1
 7) Number of items inserted
 8) (integer) 0
 9) Expansion rate
10) (integer) 2
```

RDB Load, Save and flushall validation
```
127.0.0.1:6379> info keyspace
# Keyspace
127.0.0.1:6379> bf.add key item
(integer) 1
127.0.0.1:6379> info keyspace
# Keyspace
db0:keys=1,expires=0,avg_ttl=0
127.0.0.1:6379> flushall
OK
127.0.0.1:6379> info keyspace
# Keyspace
127.0.0.1:6379> bf.add key item
(integer) 1
127.0.0.1:6379> bgsave
Background saving started
127.0.0.1:6379> shutdown
not connected> info keyspace // Started up
# Keyspace
db0:keys=1,expires=0,avg_ttl=0
127.0.0.1:6379> keys *
1) "key"
127.0.0.1:6379> bf.exists key item
(integer) 1
```
