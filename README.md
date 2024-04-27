# valkey-bloom

Tested on Linux so far.

```
curl https://sh.rustup.rs -sSf | sh
sudo yum install clang
git clone https://github.com/KarthikSubbarao/valkey-bloom.git
cd valkey-bloom
cargo build --all --all-targets  --release
find . -name "libvalkey_bloom.so"  
valkey-server --loadmodule ./target/release/libvalkey_bloom.so
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

```
16084:M 27 Apr 2024 02:23:15.759 * Legacy Redis Module ./target/debug/libvalkey_bloom.so found
16084:M 27 Apr 2024 02:23:15.760 * <bloom> Created new data type 'bloomtype'
16084:M 27 Apr 2024 02:23:15.760 * Module 'bloom' loaded from ./target/debug/libvalkey_bloom.so
16084:M 27 Apr 2024 02:23:15.760 * Server initialized
16084:M 27 Apr 2024 02:23:15.760 * Loading RDB produced by valkey version 255.255.255
16084:M 27 Apr 2024 02:23:15.760 * RDB age 5 seconds
16084:M 27 Apr 2024 02:23:15.760 * RDB memory usage when created 1.17 Mb
16084:M 27 Apr 2024 02:23:15.760 * <module> NOOP for now
16084:M 27 Apr 2024 02:23:15.763 * Done loading RDB, keys loaded: 1, keys expired: 0.
16084:M 27 Apr 2024 02:23:15.763 * DB loaded from disk: 0.003 seconds
```
