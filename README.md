# valkey-bloom

Tested on Linux so far.

```
curl https://sh.rustup.rs -sSf | sh
sudo yum install clang
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
```