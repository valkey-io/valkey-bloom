# valkey-bloom

Tested on Linux so far.

```
curl https://sh.rustup.rs -sSf | sh
sudo yum install clang
cargo build --all --all-targets  --release
find . -name "libvalkey_bloom.so"  
valkey-server --loadmodule ./target/release/libvalkey_bloom.so
```