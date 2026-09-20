import os
import sys

import pytest
from valkey import ResponseError

from cuckoo_test_utils import CuckooTestCase
from valkeytestframework.util.waiters import wait_for_equal


@pytest.mark.skip_for_asan(reason='Active defrag requires the Valkey jemalloc build')
class TestCuckooDefrag(CuckooTestCase):
    @pytest.fixture(autouse=True)
    def configure_defrag(self, setup_test):
        client = self.server.get_new_client()
        # Verify support after server startup. Keep it off until fragmentation and
        # baseline metrics are ready, so the measured callbacks see our objects.
        try:
            client.config_set('activedefrag', 'yes')
        except ResponseError as error:
            # Linux CI must provide the supported jemalloc build. Locally, skip
            # only the known lack of server support, not arbitrary CONFIG errors.
            unsupported = 'requires a' in str(error) and 'Jemalloc' in str(error)
            if unsupported and not (sys.platform == 'linux' and os.getenv('CI')):
                pytest.skip(f'Active defrag is unavailable in this server build: {error}')
            raise
        assert client.config_get('activedefrag')['activedefrag'] == 'yes'
        client.config_set('activedefrag', 'no')
        client.config_set('active-defrag-ignore-bytes', 1)
        client.config_set('active-defrag-threshold-lower', 1)
        client.config_set('active-defrag-cycle-min', 50)
        client.config_set('active-defrag-cycle-max', 75)

    @pytest.mark.parametrize('capacity', [200, 8], ids=['single', 'scaled'])
    @pytest.mark.parametrize('delete_items', [False, True], ids=['intact', 'deleted'])
    def test_defrag_visits_buckets_and_preserves_data(self, capacity, delete_items):
        client = self.server.get_new_client()
        items = [f'item:{j}' for j in range(20)]
        with client.pipeline(transaction=False) as pipe:
            for i in range(2000):
                key = f'buckets:{i}'
                pipe.execute_command('CF.RESERVE', key, capacity, 'EXPANSION', 2)
                pipe.execute_command('CF.INSERT', key, 'ITEMS', *items, 'duplicate', 'duplicate')
                if delete_items:
                    for item in items[::2]:
                        pipe.execute_command('CF.DEL', key, item)
            pipe.execute()
        client.delete(*[f'buckets:{i}' for i in range(0, 2000, 2)])
        keys = [f'buckets:{i}' for i in range(1, 2000, 2)]
        before = client.execute_command('DEBUG', 'DIGEST-VALUE', *keys)
        info_before = client.execute_command('CF.INFO', keys[0])
        filters = client.execute_command('CF.INFO', keys[0], 'Number of filters')
        assert (filters > 1) == (capacity == 8)
        with client.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.execute_command('CF.MEXISTS', key, *items)
                pipe.execute_command('CF.COUNT', key, 'duplicate')
            membership_before = pipe.execute()
        baseline = client.info('modules')
        client.config_set('activedefrag', 'yes')
        wait_for_equal(
            lambda: client.info('modules')['bf_cuckoo_defrag_bucket_attempts']
            > baseline['bf_cuckoo_defrag_bucket_attempts'],
            True,
        )
        client.config_set('activedefrag', 'no')
        after = client.info('modules')
        attempts = sum(after['bf_cuckoo_defrag_' + name] - baseline['bf_cuckoo_defrag_' + name]
                       for name in ['hits', 'misses'])
        # A callback may yield between subfilters; a whole visit need not finish.
        assert attempts > 0
        assert after['bf_cuckoo_defrag_bucket_attempts'] > baseline['bf_cuckoo_defrag_bucket_attempts']
        assert client.execute_command('DEBUG', 'DIGEST-VALUE', *keys) == before
        # Defrag may shrink the pointer vector. All logical CF.INFO fields persist.
        info_after = client.execute_command('CF.INFO', keys[0])
        assert info_after[2:] == info_before[2:]
        with client.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.execute_command('CF.MEXISTS', key, *items)
                pipe.execute_command('CF.COUNT', key, 'duplicate')
            assert pipe.execute() == membership_before
        for key in keys[:10]:
            assert client.execute_command('CF.ADD', key, 'after-defrag') == 1
            assert client.execute_command('CF.EXISTS', key, 'after-defrag') == 1
            assert client.execute_command('CF.DEL', key, 'after-defrag') == 1
