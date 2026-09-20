#!/usr/bin/env python3
"""Compare dedicated Valkey-Bloom and RedisBloom servers using the same input.

Only keys prefixed with cuckoo-bench: are created/deleted. Latencies include
client/network round trips; throughput includes Python and pipeline overhead.
"""
import argparse
import json
import math
import platform
import statistics
import time
from pathlib import Path

import valkey

CONFIGS = [(2, 20, 1), (4, 500, 2), (8, 1000, 4), (4, 500, 0)]


def positive_int(value):
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError('must be a positive integer') from exc
    if number <= 0:
        raise argparse.ArgumentTypeError('must be a positive integer')
    return number


def percentile(values, fraction):
    return sorted(values)[min(len(values) - 1, math.ceil(len(values) * fraction) - 1)]


def measure(client, label, count, config, repeat, batch_size):
    bucket, kicks, expansion = config
    capacity = count if expansion == 0 else max(1, count // 4)
    key = f'cuckoo-bench:{count}:{bucket}:{kicks}:{expansion}'
    client.delete(key)
    client.execute_command('CF.RESERVE', key, capacity, 'BUCKETSIZE', bucket,
                           'MAXITERATIONS', kicks, 'EXPANSION', expansion)
    latencies = []
    errors = 0
    accepted = []
    started = time.perf_counter()
    for start in range(0, count, batch_size):
        stop = min(start + batch_size, count)
        with client.pipeline(transaction=False) as pipe:
            for item in range(start, stop - 1):
                pipe.execute_command('CF.ADD', key, f'item:{item}')
            results = pipe.execute(raise_on_error=False)
        accepted.extend(not isinstance(result, valkey.ResponseError) for result in results)
        errors += sum(isinstance(result, valkey.ResponseError) for result in results)
        before = time.perf_counter_ns()
        try:
            client.execute_command('CF.ADD', key, f'item:{stop - 1}')
            accepted.append(True)
        except valkey.ResponseError:
            accepted.append(False)
            errors += 1
        latencies.append((time.perf_counter_ns() - before) / 1000)
    add_seconds = time.perf_counter() - started
    memory = client.memory_usage(key)
    info = client.execute_command('CF.INFO', key)
    info = {k.decode(): v for k, v in zip(info[::2], info[1::2])}
    lookup_latencies = []
    hits = 0
    successful_lookup_misses = 0
    started = time.perf_counter()
    for start in range(0, count, batch_size):
        stop = min(start + batch_size, count)
        with client.pipeline(transaction=False) as pipe:
            for item in range(start, stop - 1):
                pipe.execute_command('CF.EXISTS', key, f'item:{item}')
            replies = pipe.execute()
            hits += sum(replies)
            successful_lookup_misses += sum(ok and not hit for ok, hit in zip(accepted[start:stop - 1], replies))
        before = time.perf_counter_ns()
        hit = client.execute_command('CF.EXISTS', key, f'item:{stop - 1}')
        hits += hit
        successful_lookup_misses += accepted[stop - 1] and not hit
        lookup_latencies.append((time.perf_counter_ns() - before) / 1000)
    lookup_seconds = time.perf_counter() - started
    stored = info['Number of items inserted']
    assert stored == sum(accepted), (label, stored, sum(accepted))
    assert successful_lookup_misses == 0, (label, successful_lookup_misses)
    result = dict(successful_inserts=sum(accepted), stored_fingerprints=stored,
                  successful_lookup_misses=successful_lookup_misses, implementation=label, items=count, capacity=capacity,
                  bucket_size=bucket, max_iterations=kicks, expansion=expansion,
                  repeat=repeat, memory_bytes=memory, insert_errors=errors, lookup_hits=hits,
                  add_ops_per_second=count / add_seconds,
                  exists_ops_per_second=count / lookup_seconds,
                  add_p50_us=statistics.median(latencies), add_p95_us=percentile(latencies, .95),
                  exists_p50_us=statistics.median(lookup_latencies), exists_p95_us=percentile(lookup_latencies, .95),
                  latency_samples=len(latencies), info=info)
    client.delete(key)
    return result


def markdown(rows):
    lines = ['| Items | Bucket | Kicks | Expansion | Valkey bytes | ReBloom bytes | Valkey ADD p50 µs | ReBloom ADD p50 µs | Valkey EXISTS p50 µs | ReBloom EXISTS p50 µs |',
             '| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    combinations = sorted({(r['items'], r['bucket_size'], r['max_iterations'], r['expansion']) for r in rows})
    for combo in combinations:
        values = []
        for metric in ['memory_bytes', 'add_p50_us', 'exists_p50_us']:
            for label in ['Valkey', 'ReBloom']:
                samples = [r[metric] for r in rows if r['implementation'] == label and
                           (r['items'], r['bucket_size'], r['max_iterations'], r['expansion']) == combo]
                value = statistics.median(samples)
                values.append(f'{value:,.0f}' if metric == 'memory_bytes' else f'{value:.1f}')
        lines.append('| ' + ' | '.join([str(value) for value in combo] + values) + ' |')
    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--valkey-url', default='redis://127.0.0.1:6380')
    parser.add_argument('--rebloom-url', default='redis://127.0.0.1:6390')
    parser.add_argument('--sizes', type=positive_int, nargs='+', default=[10_000, 100_000, 1_000_000])
    parser.add_argument('--repeats', type=positive_int, default=3)
    parser.add_argument('--batch-size', type=positive_int, default=512)
    parser.add_argument('--output', type=Path, default=Path('benchmarks/cuckoo-results.json'))
    args = parser.parse_args()
    clients = {'Valkey': valkey.Valkey.from_url(args.valkey_url),
               'ReBloom': valkey.Valkey.from_url(args.rebloom_url)}
    result = {'machine': platform.platform(), 'batch_size': args.batch_size,
              'servers': {name: {'server': client.info('server'), 'modules': client.info('modules')}
                          for name, client in clients.items()}, 'rows': []}
    # INFO may contain byte values in some client/server versions.
    for repeat in range(args.repeats):
        for count in args.sizes:
            for config in CONFIGS:
                order = list(clients) if repeat % 2 == 0 else list(reversed(clients))
                for label in order:
                    row = measure(clients[label], label, count, config, repeat, args.batch_size)
                    result['rows'].append(row)
                    args.output.write_text(json.dumps(result, indent=2, default=str) + '\n')
                    print(f'{label}: n={count} config={config} repeat={repeat} '
                          f'bytes={row["memory_bytes"]} errors={row["insert_errors"]}', flush=True)
    args.output.with_suffix('.md').write_text(markdown(result['rows']))


if __name__ == '__main__':
    main()
