import os
import pytest
from valkey import ResponseError
from valkeytestframework.valkey_test_case import ReplicationTestCase
from cuckoo_test_utils import rewrite_cuckoo_aof
from valkeytestframework.util.waiters import wait_for_equal

class TestCuckooReplication(ReplicationTestCase):

    use_random_seed = 'no'

    def waitForReplicaToSyncUp(self, server):
        super().waitForReplicaToSyncUp(server)
        # The framework only waits for the link to be up. The replica may still
        # be applying commands, especially on ASAN builds; wait for our writes.
        offset = self.client.info('replication')['master_repl_offset']
        wait_for_equal(
            lambda: server.client.info('replication')['slave_repl_offset'] >= offset,
            True,
            timeout=10,
        )

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        self.args = {
            "enable-debug-command": "yes",
            'loadmodule': os.getenv('MODULE_PATH'),
            'bf.bloom-use-random-seed': self.use_random_seed,
        }
        server_path = f"{os.path.dirname(os.path.realpath(__file__))}/build/binaries/{os.environ['SERVER_VERSION']}/valkey-server"
        self.server, self.client = self.create_server(
            testdir=self.testdir,
            server_path=server_path,
            args=self.args,
        )

    @pytest.fixture(autouse=True)
    def use_random_seed_fixture(self):
        self.use_random_seed = 'no'

    def test_cf_add_replication(self):
        """Test that CF.ADD replicates to replica"""
        self.setup_replication(num_replicas=1)

        result = self.client.execute_command('CF.ADD', 'replTest', 'item1')
        assert result == 1

        self.waitForReplicaToSyncUp(self.replicas[0])

        exists = self.replicas[0].client.execute_command('CF.EXISTS', 'replTest', 'item1')
        assert exists == 1

    @pytest.mark.parametrize('command', ['CF.ADD', 'CF.ADDNX', 'CF.INSERT', 'CF.INSERTNX'])
    @pytest.mark.parametrize('reserve', [False, True])
    def test_insertion_keyspace_event_parity(self, command, reserve):
        """Replicas publish the same insertion event, including repeated NX calls."""
        self.setup_replication(num_replicas=1)
        replica = self.replicas[0].client
        key = 'eventParity'
        if reserve:
            self.client.execute_command('CF.RESERVE', key, 64)
            self.waitForReplicaToSyncUp(self.replicas[0])
        for client in (self.client, replica):
            client.config_set('notify-keyspace-events', 'AKEm')

        channels = [b'__keyevent@0__:cuckoo.add', b'__keyevent@0__:cuckoo.insert']
        multi = command.startswith('CF.INSERT')
        args = (command, key, 'ITEMS', 'item') if multi else (command, key, 'item')

        def read_events(pubsub):
            # The pong fences notifications after the completed replicated write.
            pubsub.ping(b'events-complete')
            events = []
            while True:
                message = pubsub.get_message(timeout=5)
                assert message is not None
                if message['type'] == 'pong':
                    assert message['data'] == b'events-complete'
                    return events
                assert message['type'] == 'message'
                events.append((message['channel'], message['data']))

        with self.client.pubsub() as primary_events, replica.pubsub() as replica_events:
            for pubsub in (primary_events, replica_events):
                pubsub.subscribe(*channels)
                for _ in channels:
                    assert pubsub.get_message(timeout=5)['type'] == 'subscribe'
            for repeated in (False, True):
                added = int(not (repeated and command.endswith('NX')))
                assert self.client.execute_command(*args) == ([added] if multi else added)
                self.waitForReplicaToSyncUp(self.replicas[0])
                expected = [(channels[int(multi)], key.encode())] if added else []
                assert read_events(primary_events) == expected
                assert read_events(replica_events) == expected
                assert self.client.dump(key) == replica.dump(key)

    def test_cf_del_replication(self):
        """Test that CF.DEL replicates to replica"""
        self.setup_replication(num_replicas=1)

        self.client.execute_command('CF.ADD', 'delRepl', 'item1')
        self.waitForReplicaToSyncUp(self.replicas[0])
        self.client.execute_command('CF.DEL', 'delRepl', 'item1')
        self.waitForReplicaToSyncUp(self.replicas[0])

        exists = self.replicas[0].client.execute_command('CF.EXISTS', 'delRepl', 'item1')
        assert exists == 0

    def test_cf_reserve_replication(self):
        """Test that CF.RESERVE replicates to replica"""
        self.setup_replication(num_replicas=1)

        self.client.execute_command('CF.RESERVE', 'resRepl', 1000, 'BUCKETSIZE', 4)
        self.waitForReplicaToSyncUp(self.replicas[0])

        info = self.replicas[0].client.execute_command('CF.INFO', 'resRepl')
        info_dict = dict(zip(info[::2], info[1::2]))
        assert info_dict[b'Bucket size'] == 4

    def test_cf_insert_replication(self):
        """Test that CF.INSERT replicates to replica"""
        self.setup_replication(num_replicas=1)

        self.client.execute_command('CF.INSERT', 'insRepl', 'ITEMS', 'val1', 'val2', 'val3')
        self.waitForReplicaToSyncUp(self.replicas[0])

        assert self.replicas[0].client.execute_command('CF.EXISTS', 'insRepl', 'val1') == 1
        assert self.replicas[0].client.execute_command('CF.EXISTS', 'insRepl', 'val2') == 1
        assert self.replicas[0].client.execute_command('CF.EXISTS', 'insRepl', 'val3') == 1

    def test_occurrence_count_replication(self):
        """Test that membership estimates replicate correctly"""
        self.setup_replication(num_replicas=1)

        self.client.execute_command('CF.ADD', 'countRepl', 'item1')
        self.client.execute_command('CF.ADD', 'countRepl', 'item1')
        self.client.execute_command('CF.ADD', 'countRepl', 'item1')
        self.waitForReplicaToSyncUp(self.replicas[0])

        count = self.replicas[0].client.execute_command('CF.COUNT', 'countRepl', 'item1')
        assert count == 3

    def test_scaling_filter_replication(self):
        """Test that filter scaling replicates correctly"""
        self.setup_replication(num_replicas=1)

        self.client.execute_command('CF.RESERVE', 'scaleRepl', 10, 'EXPANSION', 2)
        for i in range(30):
            self.client.execute_command('CF.ADD', 'scaleRepl', f'item{i}')
        self.waitForReplicaToSyncUp(self.replicas[0])

        primary_info = self.client.execute_command('CF.INFO', 'scaleRepl')
        replica_info = self.replicas[0].client.execute_command('CF.INFO', 'scaleRepl')
        assert primary_info == replica_info

        for i in range(30):
            exists = self.replicas[0].client.execute_command('CF.EXISTS', 'scaleRepl', f'item{i}')
            assert exists == 1

    def test_multiple_operations_replication(self):
        """Test complex sequence of operations replicates correctly"""
        self.setup_replication(num_replicas=1)

        self.client.execute_command('CF.RESERVE', 'multiRepl', 1000)
        self.client.execute_command('CF.ADD', 'multiRepl', 'keep1')
        self.client.execute_command('CF.ADD', 'multiRepl', 'keep2')
        self.client.execute_command('CF.ADD', 'multiRepl', 'remove1')
        self.client.execute_command('CF.DEL', 'multiRepl', 'remove1')
        self.client.execute_command('CF.INSERT', 'multiRepl', 'ITEMS', 'ins1', 'ins2')
        self.waitForReplicaToSyncUp(self.replicas[0])

        assert self.replicas[0].client.execute_command('CF.EXISTS', 'multiRepl', 'keep1') == 1
        assert self.replicas[0].client.execute_command('CF.EXISTS', 'multiRepl', 'keep2') == 1
        assert self.replicas[0].client.execute_command('CF.EXISTS', 'multiRepl', 'remove1') == 0
        assert self.replicas[0].client.execute_command('CF.EXISTS', 'multiRepl', 'ins1') == 1
        assert self.replicas[0].client.execute_command('CF.EXISTS', 'multiRepl', 'ins2') == 1

    def test_replica_readonly(self):
        """Test that replica refuses write operations"""
        self.setup_replication(num_replicas=1)

        try:
            self.replicas[0].client.execute_command('CF.ADD', 'readonlyTest', 'item1')
            assert False, "Expected READONLY error"
        except ResponseError as e:
            assert 'READONLY' in str(e) or 'replica' in str(e).lower()

    def test_bulk_operations_replication(self):
        """Test that bulk operations replicate correctly"""
        self.setup_replication(num_replicas=1)

        items = [f'bulk{i}' for i in range(100)]
        self.client.execute_command('CF.INSERT', 'bulkRepl', 'ITEMS', *items)
        self.waitForReplicaToSyncUp(self.replicas[0])

        results = self.replicas[0].client.execute_command('CF.MEXISTS', 'bulkRepl', *items)
        assert all(r == 1 for r in results)

    def test_cf_load_replication(self):
        """Test that CF.LOAD replicates correctly"""
        self.setup_replication(num_replicas=1)

        self.client.execute_command('CF.RESERVE', 'loadTest', 100)
        self.client.execute_command('CF.ADD', 'loadTest', 'item1')
        self.waitForReplicaToSyncUp(self.replicas[0])

        snapshot = rewrite_cuckoo_aof(self.client, self.server)[b'loadTest']
        replica = self.replicas[0].client
        assert replica.exists('loaded') == 0
        assert self.client.execute_command('CF.LOAD', 'loaded', snapshot) == b'OK'
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert replica.execute_command('CF.EXISTS', 'loaded', 'item1') == 1
        assert self.client.dump('loadTest') == self.client.dump('loaded') == replica.dump('loaded')
        assert self.client.execute_command('DEBUG', 'DIGEST-VALUE', 'loaded') == replica.execute_command('DEBUG', 'DIGEST-VALUE', 'loaded')

    def test_replication_after_reconnect(self):
        """Test replication resumes after connection loss"""
        self.setup_replication(num_replicas=1)

        self.client.execute_command('CF.ADD', 'reconnTest', 'item1')
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.replicas[0].client.execute_command('CF.EXISTS', 'reconnTest', 'item1') == 1

        # Break replication
        self.replicas[0].client.execute_command('REPLICAOF', 'NO', 'ONE')

        # Add more data on primary (won't replicate immediately)
        self.client.execute_command('CF.ADD', 'reconnTest', 'item2')

        # Reconnect replication and wait for sync
        self.replicas[0].client.execute_command('REPLICAOF', self.server.bind_ip, self.server.port)
        self.waitForReplicaToSyncUp(self.replicas[0])

        exists = self.replicas[0].client.execute_command('CF.EXISTS', 'reconnTest', 'item2')
        assert exists == 1

    @pytest.mark.parametrize('command', ['CF.ADD', 'CF.ADDNX', 'CF.INSERT', 'CF.INSERTNX', 'CF.RESERVE'])
    def test_creation_replicates_all_properties(self, command):
        self.setup_replication(num_replicas=1)
        replica = self.replicas[0].client
        for suffix, primary, secondary in [
            ('capacity', 32, 128), ('bucket-size', 2, 8),
            ('max-kicks', 20, 100), ('expansion', 2, 4),
        ]:
            self.client.config_set('bf.cuckoo-' + suffix, primary)
            replica.config_set('bf.cuckoo-' + suffix, secondary)
        if command == 'CF.RESERVE':
            self.client.execute_command(command, 'properties', 32)
        elif command in ('CF.INSERT', 'CF.INSERTNX'):
            self.client.execute_command(command, 'properties', 'ITEMS', 'first')
        else:
            self.client.execute_command(command, 'properties', 'first')
        for i in range(150):
            self.client.execute_command('CF.ADD', 'properties', f'value-{i}')
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.client.dump('properties') == replica.dump('properties')
        assert self.client.execute_command('DEBUG', 'DIGEST-VALUE', 'properties') == replica.execute_command('DEBUG', 'DIGEST-VALUE', 'properties')

    def test_partial_insert_replication_with_different_memory_limits(self):
        self.setup_replication(num_replicas=1)
        replica = self.replicas[0].client
        self.client.execute_command('CF.RESERVE', 'limited', 4, 'EXPANSION', 1)
        info = self.client.execute_command('CF.INFO', 'limited')
        size = dict(zip(info[::2], info[1::2]))[b'Size']
        self.client.config_set('bf.cuckoo-memory-usage-limit', size)
        response = self.client.execute_command('CF.INSERT', 'limited', 'ITEMS', *[f'value-{i}' for i in range(20)])
        assert any(isinstance(value, ResponseError) for value in response)
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.client.dump('limited') == replica.dump('limited')

    def test_full_sync_preserves_rng_for_future_insertions(self):
        self.client.execute_command('CF.RESERVE', 'snapshot', 128, 'BUCKETSIZE', 2, 'EXPANSION', 2)
        for i in range(400):
            self.client.execute_command('CF.ADD', 'snapshot', f'value-{i}')
        for i in range(20):
            assert self.client.execute_command('CF.DEL', 'snapshot', f'value-{i}') == 1
        self.setup_replication(num_replicas=1)
        replica = self.replicas[0].client
        assert replica.execute_command('CF.INFO', 'snapshot', 'Number of items deleted') == 20
        assert self.client.dump('snapshot') == replica.dump('snapshot')
        for i in range(400, 1200):
            self.client.execute_command('CF.ADD', 'snapshot', f'value-{i}')
        for i in range(20, 40):
            assert self.client.execute_command('CF.DEL', 'snapshot', f'value-{i}') == 1
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.client.dump('snapshot') == replica.dump('snapshot')
        assert replica.execute_command('CF.INFO', 'snapshot', 'Number of items deleted') == 40

    def test_nx_skipped_items_are_not_replayed_as_adds(self):
        self.setup_replication(num_replicas=1)
        c = self.client
        c.execute_command('CF.ADD', 'nx', 'old')
        c.execute_command('CF.ADD', 'nx', 'old')
        self.waitForReplicaToSyncUp(self.replicas[0])
        offset = c.info('replication')['master_repl_offset']
        assert c.execute_command('CF.ADDNX', 'nx', 'old') == 0
        assert c.info('replication')['master_repl_offset'] == offset
        assert c.execute_command('CF.INSERTNX', 'nx', 'ITEMS', 'old', 'new', 'old', 'new', 'last') == [0, 1, 0, 0, 1]
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert c.dump('nx') == self.replicas[0].client.dump('nx')
        offset = c.info('replication')['master_repl_offset']
        assert c.execute_command('CF.ADD', 'nx', 'old') == 1
        assert c.info('replication')['master_repl_offset'] > offset
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert c.dump('nx') == self.replicas[0].client.dump('nx')

    def test_full_sync_ignores_replica_memory_limit(self):
        self.client.execute_command('CF.RESERVE', 'large', 1000000, 'BUCKETSIZE', 5)
        self.client.execute_command('CF.ADD', 'large', 'saved')
        self.args['bf.cuckoo-memory-usage-limit'] = '1024'
        self.setup_replication(num_replicas=1)
        replica = self.replicas[0].client
        assert replica.dump('large') == self.client.dump('large')
        self.client.execute_command('CF.ADD', 'large', 'next')
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert replica.dump('large') == self.client.dump('large')

    def test_replicated_restore_ignores_replica_memory_limit(self):
        self.setup_replication(num_replicas=1)
        replica = self.replicas[0].client
        replica.config_set('bf.cuckoo-memory-usage-limit', 1024)
        self.client.execute_command('CF.RESERVE', 'source', 1000000, 'BUCKETSIZE', 5)
        self.client.execute_command('CF.ADD', 'source', 'saved')
        dump = self.client.dump('source')
        self.client.delete('source')
        self.client.restore('restored', 0, dump)
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert replica.dump('restored') == dump
        assert replica.execute_command('CF.EXISTS', 'restored', 'saved') == 1

    def test_failed_eviction_then_raised_limit_replicates_identically(self):
        self.setup_replication(num_replicas=1)
        c = self.client
        c.execute_command('CF.RESERVE', 'cached', 64, 'BUCKETSIZE', 4, 'MAXITERATIONS', 1, 'EXPANSION', 2)
        size = dict(zip(*(iter(c.execute_command('CF.INFO', 'cached')),)*2))[b'Size']
        c.config_set('bf.cuckoo-memory-usage-limit', size)
        for i in range(1000):
            try:
                c.execute_command('CF.ADD', 'cached', f'item:{i}')
            except ResponseError:
                break
        else:
            assert False, 'Expected an eviction failure'
        c.config_set('bf.cuckoo-memory-usage-limit', 256 * 1024 * 1024)
        for j in range(i + 1, i + 200):
            c.execute_command('CF.ADD', 'cached', f'item:{j}')
            self.waitForReplicaToSyncUp(self.replicas[0])
            assert c.dump('cached') == self.replicas[0].client.dump('cached')

    def test_cached_failure_with_reusable_older_filter(self):
        self.setup_replication(num_replicas=1)
        c = self.client
        c.execute_command('CF.RESERVE', 'cached-old', 64, 'BUCKETSIZE', 4,
                          'MAXITERATIONS', 1, 'EXPANSION', 2)
        for item in range(80):
            c.execute_command('CF.ADD', 'cached-old', item.to_bytes(8, 'little'))
        info = c.execute_command('CF.INFO', 'cached-old')
        size = dict(zip(info[::2], info[1::2]))[b'Size']
        c.config_set('bf.cuckoo-memory-usage-limit', size)
        for item in range(80, 1000):
            try:
                c.execute_command('CF.ADD', 'cached-old', item.to_bytes(8, 'little'))
            except ResponseError:
                break
        else:
            assert False, 'Expected an eviction failure'
        for item in range(32):
            c.execute_command('CF.DEL', 'cached-old', item.to_bytes(8, 'little'))
        for item in range(1000, 2000):
            try:
                c.execute_command('CF.ADD', 'cached-old', item.to_bytes(8, 'little'))
            except ResponseError:
                pass
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert c.dump('cached-old') == self.replicas[0].client.dump('cached-old')
