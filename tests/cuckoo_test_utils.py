"""Read actual CF.LOAD snapshots emitted by a command-format AOF rewrite."""

from pathlib import Path

import pytest
from valkey_bloom_test_case import ValkeyBloomTestCaseBase

from valkeytestframework.util.waiters import wait_for_equal


class CuckooTestCase(ValkeyBloomTestCaseBase):
    @pytest.fixture(autouse=True)
    def use_random_seed_fixture(self):
        # Cuckoo has a fixed RNG seed; Bloom's seed setting is irrelevant here.
        self.use_random_seed = 'no'


def rewrite_cuckoo_aof(client, server):
    client.config_set('aof-use-rdb-preamble', 'no')
    client.config_set('appendonly', 'yes')
    wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0)
    client.execute_command('BGREWRITEAOF')
    wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0)
    assert client.info('persistence')['aof_last_bgrewrite_status'] == 'ok'

    directory = Path(server.cwd) / server.args['appenddirname']
    manifests = list(directory.glob('*.manifest'))
    assert len(manifests) == 1
    base_files = [line.split()[1] for line in manifests[0].read_text().splitlines()
                  if line.endswith('type b')]
    assert len(base_files) == 1
    snapshots = {}
    with (directory / base_files[0]).open('rb') as aof:
        while header := aof.readline():
            assert header.startswith(b'*'), header
            args = []
            for _ in range(int(header[1:])):
                length = aof.readline()
                assert length.startswith(b'$'), length
                args.append(aof.read(int(length[1:])))
                assert aof.read(2) == b'\r\n'
            if args[0].upper() == b'CF.LOAD':
                assert len(args) == 3
                assert args[1] not in snapshots, f'Duplicate CF.LOAD for {args[1]!r}'
                snapshots[args[1]] = args[2]
    assert snapshots, 'Rewritten AOF contains no CF.LOAD commands'
    return snapshots
