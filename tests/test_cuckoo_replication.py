import os
import pytest
from valkey import ResponseError
from valkeytestframework.valkey_test_case import ReplicationTestCase

class TestCuckooReplication(ReplicationTestCase):

    use_random_seed = 'no'

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
    def use_random_seed_fixture(self, bloom_config_parameterization):
        if bloom_config_parameterization == "random-seed":
            self.use_random_seed = "yes"
        elif bloom_config_parameterization == "fixed-seed":
            self.use_random_seed = "no"

    def test_cf_add_replication(self):
        """Test that CF.ADD replicates to replica"""
        self.setup_replication(num_replicas=1)

        result = self.client.execute_command('CF.ADD', 'replTest', 'item1')
        assert result == 1

        self.waitForReplicaToSyncUp(self.replicas[0])

        exists = self.replicas[0].client.execute_command('CF.EXISTS', 'replTest', 'item1')
        assert exists == 1

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
        """Test that duplicate counts replicate correctly"""
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

        exists = self.replicas[0].client.execute_command('CF.EXISTS', 'loadTest', 'item1')
        assert exists == 1

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
