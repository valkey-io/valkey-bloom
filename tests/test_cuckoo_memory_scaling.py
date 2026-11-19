"""
Memory scaling analysis for the Cuckoo Filter.

Measures CF.INFO Size under varying parameters and prints a Markdown table
suitable for pasting into CUCKOO_IMPLEMENTATION_STATUS.md.

Run with:
  SERVER_VERSION=unstable MODULE_PATH=<path>/libvalkey_bloom.dylib \
    python3 -m pytest tests/test_cuckoo_memory_scaling.py -v -s
"""
import pytest
from valkey_bloom_test_case import ValkeyBloomTestCaseBase


def _cf_info_size(client, key):
    info = client.execute_command("CF.INFO", key)
    # CF.INFO returns [field, value, ...]. "Size" is at index 1.
    for i in range(0, len(info) - 1, 2):
        if info[i] in (b"Size", "Size"):
            return info[i + 1]
    raise ValueError(f"Size not found in CF.INFO output: {info}")


class TestCuckooMemoryScaling(ValkeyBloomTestCaseBase):

    def test_memory_by_capacity(self):
        """Memory usage vs capacity (bucket_size=4, expansion=1)."""
        client = self.server.get_new_client()
        capacities = [100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000]
        bucket_size = 4
        expansion = 1

        rows = []
        for cap in capacities:
            client.execute_command("DEL", "cf_bench")
            client.execute_command(
                "CF.RESERVE", "cf_bench", cap,
                "BUCKETSIZE", bucket_size,
                "EXPANSION", expansion,
            )
            size = _cf_info_size(client, "cf_bench")
            rows.append((cap, bucket_size, expansion, size))

        _print_table(
            "Memory vs Capacity (bucket_size=4, expansion=1)",
            ["Capacity", "Bucket Size", "Expansion", "Size (bytes)"],
            rows,
        )
        client.execute_command("DEL", "cf_bench")

    def test_memory_by_bucket_size(self):
        """Memory usage vs bucket size (capacity=10000, expansion=1)."""
        client = self.server.get_new_client()
        capacity = 10_000
        bucket_sizes = [1, 2, 4, 8, 16, 32]
        expansion = 1

        rows = []
        for bs in bucket_sizes:
            client.execute_command("DEL", "cf_bench")
            client.execute_command(
                "CF.RESERVE", "cf_bench", capacity,
                "BUCKETSIZE", bs,
                "EXPANSION", expansion,
            )
            size = _cf_info_size(client, "cf_bench")
            rows.append((capacity, bs, expansion, size))

        _print_table(
            "Memory vs Bucket Size (capacity=10000, expansion=1)",
            ["Capacity", "Bucket Size", "Expansion", "Size (bytes)"],
            rows,
        )
        client.execute_command("DEL", "cf_bench")

    def test_memory_by_expansion(self):
        """Memory usage after filling to capacity with different expansion rates."""
        client = self.server.get_new_client()
        capacity = 1_000
        bucket_size = 4
        expansions = [0, 1, 2, 4]  # 0 = non-scaling

        rows = []
        for exp in expansions:
            client.execute_command("DEL", "cf_bench")
            client.execute_command(
                "CF.RESERVE", "cf_bench", capacity,
                "BUCKETSIZE", bucket_size,
                "EXPANSION", exp if exp > 0 else 1,
            )
            # Fill to capacity to trigger scaling (where applicable)
            for i in range(capacity):
                client.execute_command("CF.ADD", "cf_bench", f"item{i}")

            info = client.execute_command("CF.INFO", "cf_bench")
            info_dict = {info[i]: info[i + 1] for i in range(0, len(info) - 1, 2)}
            size_key = b"Size" if b"Size" in info_dict else "Size"
            filters_key = b"Number of filters" if b"Number of filters" in info_dict else "Number of filters"
            size = info_dict.get(size_key, "N/A")
            num_filters = info_dict.get(filters_key, "N/A")
            rows.append((capacity, bucket_size, exp, num_filters, size))

        _print_table(
            "Memory after filling to capacity with different expansion rates (capacity=1000, bucket_size=4)",
            ["Initial Capacity", "Bucket Size", "Expansion", "Num Filters", "Size (bytes)"],
            rows,
        )
        client.execute_command("DEL", "cf_bench")

    def test_memory_grid(self):
        """Full parameter grid: capacity × bucket_size (empty filter, expansion=1)."""
        client = self.server.get_new_client()
        capacities  = [1_000, 10_000, 100_000, 1_000_000]
        bucket_sizes = [1, 2, 4, 8]
        expansion = 1

        rows = []
        for cap in capacities:
            for bs in bucket_sizes:
                client.execute_command("DEL", "cf_bench")
                client.execute_command(
                    "CF.RESERVE", "cf_bench", cap,
                    "BUCKETSIZE", bs,
                    "EXPANSION", expansion,
                )
                size = _cf_info_size(client, "cf_bench")
                rows.append((cap, bs, size))

        _print_table(
            "Memory Grid: capacity × bucket_size (empty filter, expansion=1)",
            ["Capacity", "Bucket Size", "Size (bytes)"],
            rows,
        )
        client.execute_command("DEL", "cf_bench")


def _print_table(title, headers, rows):
    col_widths = [len(h) for h in headers]
    str_rows = [[str(v) for v in row] for row in rows]
    for row in str_rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    sep  = "| " + " | ".join("-" * w for w in col_widths) + " |"
    head = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"

    print(f"\n### {title}\n")
    print(head)
    print(sep)
    for row in str_rows:
        print("| " + " | ".join(cell.ljust(col_widths[i]) for i, cell in enumerate(row)) + " |")
    print()
