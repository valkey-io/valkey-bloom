import time
from valkey import ResponseError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkey_test_case import ValkeyServerHandle
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *

class TestCuckooReplication(ValkeyBloomTestCaseBase):

    def setUp(self):
        super().setUp()
        # Create replica server
        self.replica_port = resource_port_tracker.get_port(self.server.port + 1000)
        self.replica = ValkeyServerHandle(
            port=self.replica_port,
            module_path=self.module_path,
            server_args=[]
        )
        self.replica.start()
        wait_for_equal(lambda: self.replica.is_alive(), True)

        # Configure as replica
        replica_client = self.replica.get_new_client()
        replica_client.execute_command('REPLICAOF', 'localhost', self.server.port)
        time.sleep(1)  # Wait for replication to establish

    def tearDown(self):
        if hasattr(self, 'replica'):
            self.replica.stop()
        super().tearDown()

    def test_cf_add_replication(self):
        """Test that CF.ADD replicates to replica"""
        primary_client = self.server.get_new_client()
        replica_client = self.replica.get_new_client()

        # Add item on primary
        result = primary_client.execute_command('CF.ADD', 'replTest', 'item1')
        assert result == 1

        # Wait for replication
        time.sleep(0.5)
        wait_for_equal(
            lambda: replica_client.execute_command('CF.EXISTS', 'replTest', 'item1'),
            1,
            timeout=5
        )

        # Verify on replica
        exists = replica_client.execute_command('CF.EXISTS', 'replTest', 'item1')
        assert exists == 1

    def test_cf_del_replication(self):
        """Test that CF.DEL replicates to replica"""
        primary_client = self.server.get_new_client()
        replica_client = self.replica.get_new_client()

        # Add and delete on primary
        primary_client.execute_command('CF.ADD', 'delRepl', 'item1')
        time.sleep(0.5)
        primary_client.execute_command('CF.DEL', 'delRepl', 'item1')

        # Wait for replication
        time.sleep(0.5)

        # Verify deletion replicated
        exists = replica_client.execute_command('CF.EXISTS', 'delRepl', 'item1')
        assert exists == 0

    def test_cf_reserve_replication(self):
        """Test that CF.RESERVE replicates to replica"""
        primary_client = self.server.get_new_client()
        replica_client = self.replica.get_new_client()

        # Reserve on primary
        primary_client.execute_command('CF.RESERVE', 'resRepl', 1000, 'BUCKETSIZE', 4)
        time.sleep(0.5)

        # Verify filter exists on replica
        info = replica_client.execute_command('CF.INFO', 'resRepl')
        info_dict = dict(zip(info[::2], info[1::2]))
        assert info_dict[b'Bucket size'] == 4

    def test_cf_insert_replication(self):
        """Test that CF.INSERT replicates to replica"""
        primary_client = self.server.get_new_client()
        replica_client = self.replica.get_new_client()

        # Insert on primary
        primary_client.execute_command('CF.INSERT', 'insRepl', 'ITEMS', 'val1', 'val2', 'val3')
        time.sleep(0.5)

        # Verify all items replicated
        exists1 = replica_client.execute_command('CF.EXISTS', 'insRepl', 'val1')
        exists2 = replica_client.execute_command('CF.EXISTS', 'insRepl', 'val2')
        exists3 = replica_client.execute_command('CF.EXISTS', 'insRepl', 'val3')
        assert exists1 == 1
        assert exists2 == 1
        assert exists3 == 1

    def test_occurrence_count_replication(self):
        """Test that duplicate counts replicate correctly"""
        primary_client = self.server.get_new_client()
        replica_client = self.replica.get_new_client()

        # Add duplicates on primary
        primary_client.execute_command('CF.ADD', 'countRepl', 'item1')
        primary_client.execute_command('CF.ADD', 'countRepl', 'item1')
        primary_client.execute_command('CF.ADD', 'countRepl', 'item1')
        time.sleep(0.5)

        # Verify count on replica
        count = replica_client.execute_command('CF.COUNT', 'countRepl', 'item1')
        assert count == 3

    def test_scaling_filter_replication(self):
        """Test that filter scaling replicates correctly"""
        primary_client = self.server.get_new_client()
        replica_client = self.replica.get_new_client()

        # Create small filter that will scale
        primary_client.execute_command('CF.RESERVE', 'scaleRepl', 10, 'EXPANSION', 2)

        # Add items to trigger scaling
        for i in range(30):
            primary_client.execute_command('CF.ADD', 'scaleRepl', f'item{i}')

        time.sleep(1)

        # Verify scaling replicated
        primary_info = primary_client.execute_command('CF.INFO', 'scaleRepl')
        replica_info = replica_client.execute_command('CF.INFO', 'scaleRepl')
        assert primary_info == replica_info

        # Verify all items exist on replica
        for i in range(30):
            exists = replica_client.execute_command('CF.EXISTS', 'scaleRepl', f'item{i}')
            assert exists == 1

    def test_multiple_operations_replication(self):
        """Test complex sequence of operations replicates correctly"""
        primary_client = self.server.get_new_client()
        replica_client = self.replica.get_new_client()

        # Perform multiple operations
        primary_client.execute_command('CF.RESERVE', 'multiRepl', 1000)
        primary_client.execute_command('CF.ADD', 'multiRepl', 'keep1')
        primary_client.execute_command('CF.ADD', 'multiRepl', 'keep2')
        primary_client.execute_command('CF.ADD', 'multiRepl', 'remove1')
        primary_client.execute_command('CF.DEL', 'multiRepl', 'remove1')
        primary_client.execute_command('CF.INSERT', 'multiRepl', 'ITEMS', 'ins1', 'ins2')

        time.sleep(1)

        # Verify final state on replica
        assert replica_client.execute_command('CF.EXISTS', 'multiRepl', 'keep1') == 1
        assert replica_client.execute_command('CF.EXISTS', 'multiRepl', 'keep2') == 1
        assert replica_client.execute_command('CF.EXISTS', 'multiRepl', 'remove1') == 0
        assert replica_client.execute_command('CF.EXISTS', 'multiRepl', 'ins1') == 1
        assert replica_client.execute_command('CF.EXISTS', 'multiRepl', 'ins2') == 1

    def test_replica_readonly(self):
        """Test that replica refuses write operations"""
        replica_client = self.replica.get_new_client()

        # Attempt write on replica should fail
        try:
            replica_client.execute_command('CF.ADD', 'readonlyTest', 'item1')
            assert False, "Expected READONLY error"
        except ResponseError as e:
            assert 'READONLY' in str(e) or 'replica' in str(e).lower()

    def test_replication_after_reconnect(self):
        """Test replication resumes after connection loss"""
        primary_client = self.server.get_new_client()
        replica_client = self.replica.get_new_client()

        # Add data
        primary_client.execute_command('CF.ADD', 'reconnTest', 'item1')
        time.sleep(0.5)

        # Break replication
        replica_client.execute_command('REPLICAOF', 'NO', 'ONE')
        time.sleep(0.5)

        # Add more data on primary (won't replicate)
        primary_client.execute_command('CF.ADD', 'reconnTest', 'item2')
        time.sleep(0.5)

        # Verify item2 not on replica yet
        exists = replica_client.execute_command('CF.EXISTS', 'reconnTest', 'item2')
        assert exists == 0

        # Reconnect replication
        replica_client.execute_command('REPLICAOF', 'localhost', self.server.port)
        time.sleep(2)

        # Verify full sync occurred
        exists = replica_client.execute_command('CF.EXISTS', 'reconnTest', 'item2')
        assert exists == 1

    def test_bulk_operations_replication(self):
        """Test that bulk operations replicate correctly"""
        primary_client = self.server.get_new_client()
        replica_client = self.replica.get_new_client()

        # Perform bulk inserts
        items = [f'bulk{i}' for i in range(100)]
        primary_client.execute_command('CF.INSERT', 'bulkRepl', 'ITEMS', *items)
        time.sleep(1)

        # Verify all items on replica
        results = replica_client.execute_command('CF.MEXISTS', 'bulkRepl', *items)
        assert all(r == 1 for r in results)

    def test_cf_load_replication(self):
        """Test that CF.LOAD replicates correctly"""
        primary_client = self.server.get_new_client()
        replica_client = self.replica.get_new_client()

        # Create and dump filter on primary
        primary_client.execute_command('CF.RESERVE', 'loadTest', 100)
        primary_client.execute_command('CF.ADD', 'loadTest', 'item1')

        # Get serialized data
        iterator = 0
        chunks = []
        while True:
            result = primary_client.execute_command('CF.SCANDUMP', 'loadTest', iterator)
            iterator = result[0]
            if iterator == 0:
                break
            chunks.append(result[1])

        # Load on primary (will replicate)
        primary_client.execute_command('DEL', 'loadedFilter')
        iterator = 0
        for chunk in chunks:
            iterator = primary_client.execute_command('CF.LOADCHUNK', 'loadedFilter', iterator, chunk)

        time.sleep(1)

        # Verify replicated to replica
        exists = replica_client.execute_command('CF.EXISTS', 'loadedFilter', 'item1')
        assert exists == 1
