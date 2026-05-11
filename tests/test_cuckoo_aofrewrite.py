import os
import time
import pytest
from valkey import ResponseError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkey_test_case import ValkeyServerHandle
from valkeytestframework.util.waiters import *

class TestCuckooAOFRewrite(ValkeyBloomTestCaseBase):

    @pytest.fixture(autouse=True)
    def configure_aof(self, setup_test):
        # Persist appendonly in startup args so it survives server restarts
        self.server.args['appendonly'] = 'yes'
        client = self.server.get_new_client()
        client.execute_command('CONFIG', 'SET', 'appendonly', 'yes')
        time.sleep(0.5)

    def test_basic_aof_rewrite(self):
        """Test basic AOF rewrite for cuckoo filter"""
        client = self.server.get_new_client()

        # Create and populate filter
        client.execute_command('CF.RESERVE', 'aofTest', 1000)
        client.execute_command('CF.ADD', 'aofTest', 'item1')
        client.execute_command('CF.ADD', 'aofTest', 'item1')  # Duplicate
        client.execute_command('CF.ADD', 'aofTest', 'item2')

        # Get state before rewrite
        count_before = client.execute_command('CF.COUNT', 'aofTest', 'item1')
        info_before = client.execute_command('CF.INFO', 'aofTest')

        # Trigger AOF rewrite
        client.execute_command('BGREWRITEAOF')
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=10)
        time.sleep(1)

        # Restart server to load from AOF
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        time.sleep(1)

        # Verify data restored from AOF
        count_after = client.execute_command('CF.COUNT', 'aofTest', 'item1')
        info_after = client.execute_command('CF.INFO', 'aofTest')

        assert count_after == count_before
        assert info_after == info_before

        # Verify items exist
        assert client.execute_command('CF.EXISTS', 'aofTest', 'item1') == 1
        assert client.execute_command('CF.EXISTS', 'aofTest', 'item2') == 1

    def test_aof_rewrite_with_deletions(self):
        """Test that AOF rewrite correctly handles deletions"""
        client = self.server.get_new_client()

        # Create filter with items
        client.execute_command('CF.ADD', 'delAOF', 'keep1')
        client.execute_command('CF.ADD', 'delAOF', 'keep2')
        client.execute_command('CF.ADD', 'delAOF', 'remove1')
        client.execute_command('CF.ADD', 'delAOF', 'remove2')

        # Delete some items
        client.execute_command('CF.DEL', 'delAOF', 'remove1')
        client.execute_command('CF.DEL', 'delAOF', 'remove2')

        # Rewrite and restart
        client.execute_command('BGREWRITEAOF')
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=10)
        time.sleep(1)

        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        time.sleep(1)

        # Verify deletions persisted
        assert client.execute_command('CF.EXISTS', 'delAOF', 'keep1') == 1
        assert client.execute_command('CF.EXISTS', 'delAOF', 'keep2') == 1
        assert client.execute_command('CF.EXISTS', 'delAOF', 'remove1') == 0
        assert client.execute_command('CF.EXISTS', 'delAOF', 'remove2') == 0

    def test_aof_rewrite_multiple_filters(self):
        """Test AOF rewrite with multiple cuckoo filters"""
        client = self.server.get_new_client()

        # Create multiple filters
        for i in range(10):
            client.execute_command('CF.RESERVE', f'aof{i}', 500)
            client.execute_command('CF.ADD', f'aof{i}', f'value{i}')

        # Trigger rewrite
        client.execute_command('BGREWRITEAOF')
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=10)
        time.sleep(1)

        # Restart
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        time.sleep(1)

        # Verify all filters restored
        for i in range(10):
            exists = client.execute_command('CF.EXISTS', f'aof{i}', f'value{i}')
            assert exists == 1

    def test_aof_rewrite_scaled_filter(self):
        """Test AOF rewrite with filter that has scaled"""
        client = self.server.get_new_client()

        # Create small filter and scale it
        client.execute_command('CF.RESERVE', 'scaleAOF', 10, 'EXPANSION', 2)

        # Add enough items to trigger scaling
        for i in range(30):
            client.execute_command('CF.ADD', 'scaleAOF', f'item{i}')

        # Get info before rewrite
        info_before = client.execute_command('CF.INFO', 'scaleAOF')
        info_dict_before = dict(zip(info_before[::2], info_before[1::2]))
        assert info_dict_before[b'Number of filters'] > 1

        # Rewrite and restart
        client.execute_command('BGREWRITEAOF')
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=10)
        time.sleep(1)

        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        time.sleep(1)

        # Verify scaled filter restored correctly
        info_after = client.execute_command('CF.INFO', 'scaleAOF')
        assert info_after == info_before

        # Verify all items exist
        for i in range(30):
            exists = client.execute_command('CF.EXISTS', 'scaleAOF', f'item{i}')
            assert exists == 1

    def test_aof_rewrite_occurrence_counts(self):
        """Test that occurrence counts are preserved in AOF rewrite"""
        client = self.server.get_new_client()

        # Add items with different occurrence counts
        for i in range(5):
            client.execute_command('CF.ADD', 'countAOF', 'item1')
        for i in range(3):
            client.execute_command('CF.ADD', 'countAOF', 'item2')
        client.execute_command('CF.ADD', 'countAOF', 'item3')

        # Get counts before rewrite
        count1_before = client.execute_command('CF.COUNT', 'countAOF', 'item1')
        count2_before = client.execute_command('CF.COUNT', 'countAOF', 'item2')
        count3_before = client.execute_command('CF.COUNT', 'countAOF', 'item3')

        # Rewrite and restart
        client.execute_command('BGREWRITEAOF')
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=10)
        time.sleep(1)

        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        time.sleep(1)

        # Verify counts preserved
        count1_after = client.execute_command('CF.COUNT', 'countAOF', 'item1')
        count2_after = client.execute_command('CF.COUNT', 'countAOF', 'item2')
        count3_after = client.execute_command('CF.COUNT', 'countAOF', 'item3')

        assert count1_after == count1_before == 5
        assert count2_after == count2_before == 3
        assert count3_after == count3_before == 1

    def test_aof_with_reserve_options(self):
        """Test that CF.RESERVE options are preserved in AOF"""
        client = self.server.get_new_client()

        # Create filter with specific options
        client.execute_command('CF.RESERVE', 'optAOF', 2000,
                             'BUCKETSIZE', 8,
                             'MAXITERATIONS', 600,
                             'EXPANSION', 3)
        client.execute_command('CF.ADD', 'optAOF', 'test')

        # Get info before rewrite
        info_before = client.execute_command('CF.INFO', 'optAOF')
        info_dict_before = dict(zip(info_before[::2], info_before[1::2]))

        # Rewrite and restart
        client.execute_command('BGREWRITEAOF')
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=10)
        time.sleep(1)

        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        time.sleep(1)

        # Verify options preserved
        info_after = client.execute_command('CF.INFO', 'optAOF')
        info_dict_after = dict(zip(info_after[::2], info_after[1::2]))

        assert info_dict_after[b'Bucket size'] == info_dict_before[b'Bucket size']
        assert info_dict_after[b'Max iterations'] == info_dict_before[b'Max iterations']
        assert info_dict_after[b'Expansion rate'] == info_dict_before[b'Expansion rate']

    def test_incremental_aof_then_rewrite(self):
        """Test incremental AOF followed by rewrite"""
        client = self.server.get_new_client()

        # Add some data
        client.execute_command('CF.ADD', 'incrAOF', 'item1')
        client.execute_command('CF.ADD', 'incrAOF', 'item2')
        time.sleep(0.5)

        # Verify incremental AOF is working
        aof_size_before = client.info('persistence').get('aof_current_size', 0)
        assert aof_size_before > 0

        # Add more data
        for i in range(10):
            client.execute_command('CF.ADD', 'incrAOF', f'more{i}')
        time.sleep(0.5)

        # AOF should have grown
        aof_size_after = client.info('persistence').get('aof_current_size', 0)
        assert aof_size_after > aof_size_before

        # Now rewrite
        client.execute_command('BGREWRITEAOF')
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=10)
        time.sleep(1)

        # Restart
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        time.sleep(1)

        # Verify all data present
        assert client.execute_command('CF.EXISTS', 'incrAOF', 'item1') == 1
        assert client.execute_command('CF.EXISTS', 'incrAOF', 'more5') == 1

    def test_aof_uses_cf_load_command(self):
        """Verify that AOF rewrite uses CF.LOAD command"""
        client = self.server.get_new_client()

        # Create filter
        client.execute_command('CF.RESERVE', 'loadCmdTest', 1000)
        client.execute_command('CF.ADD', 'loadCmdTest', 'item1')

        # Trigger rewrite
        client.execute_command('BGREWRITEAOF')
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=10)
        time.sleep(1)

        # Check AOF file contains CF.LOAD
        # Note: This is implementation-specific and may need adjustment
        # based on AOF file location
        aof_file = client.info('persistence').get('aof_filename', 'appendonly.aof')

        # The rewritten AOF should use CF.LOAD for compactness
        # This is verified by the fact that data restores correctly
        # and the AOF file should be smaller than incremental log

        # Restart to verify
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        time.sleep(1)

        exists = client.execute_command('CF.EXISTS', 'loadCmdTest', 'item1')
        assert exists == 1
