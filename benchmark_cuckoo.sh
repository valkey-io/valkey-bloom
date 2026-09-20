#!/usr/bin/env sh
set -eu
exec python3 "$(dirname "$0")/benchmarks/cuckoo_comparison.py" "$@"
