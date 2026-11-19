import os
from valkey import ResponseError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkey_test_case import ValkeyServerHandle
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *

class TestCuckooSaveRestore(ValkeyBloomTestCaseBase):

    def test_basic_save_and_restore(self):
        """Test basic RDB save and restore of cuckoo filter"""
        client = self.server.get_new_client()

        # Create and populate cuckoo filter
        client.execute_command('CF.RESERVE', 'testSave', 1000)
        cf_add_result_1 = client.execute_command('CF.ADD', 'testSave', 'item1')
        assert cf_add_result_1 == 1
        cf_add_result_2 = client.execute_command('CF.ADD', 'testSave', 'item1')
        assert cf_add_result_2 == 1  # Duplicate allowed
        cf_add_result_3 = client.execute_command('CF.ADD', 'testSave', 'item2')
        assert cf_add_result_3 == 1

        # Verify data before save
        cf_exists_result_1 = client.execute_command('CF.EXISTS', 'testSave', 'item1')
        assert cf_exists_result_1 == 1
        cf_count_result_1 = client.execute_command('CF.COUNT', 'testSave', 'item1')
        assert cf_count_result_1 == 2  # Added twice
        cf_info_result_1 = client.execute_command('CF.INFO', 'testSave')
        assert len(cf_info_result_1) != 0

        curr_item_count_1 = self.server.num_keys(client=client)

        # Get digests for comparison
        server_digest = client.execute_command('DEBUG', 'DIGEST')
        assert server_digest is not None
        object_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'testSave')

        # Save RDB and restart server
        client.execute_command('BGSAVE')
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)

        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        # Verify digests match
        restored_server_digest = client.execute_command('DEBUG', 'DIGEST')
        restored_object_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'testSave')
        assert restored_server_digest == server_digest
        assert restored_object_digest == object_digest

        self.server.verify_string_in_logfile("Loading RDB produced by Valkey")
        self.server.verify_string_in_logfile("Done loading RDB, keys loaded: 1, keys expired: 0")

        # Verify restored data
        curr_item_count_2 = self.server.num_keys(client=client)
        assert curr_item_count_2 == curr_item_count_1

        cf_exists_result_2 = client.execute_command('CF.EXISTS', 'testSave', 'item1')
        assert cf_exists_result_2 == 1

        cf_count_result_2 = client.execute_command('CF.COUNT', 'testSave', 'item1')
        assert cf_count_result_2 == 2  # Count preserved

        cf_info_result_2 = client.execute_command('CF.INFO', 'testSave')
        assert cf_info_result_2 == cf_info_result_1

    def test_save_many_filters(self):
        """Test saving and restoring many cuckoo filters"""
        client = self.server.get_new_client()
        count = 100

        # Create many filters
        for i in range(0, count):
            name = f"cf{i}key"
            client.execute_command('CF.RESERVE', name, 500)
            client.execute_command('CF.ADD', name, f'value{i}')
            client.execute_command('CF.ADD', name, f'value{i}')  # Duplicate

        # Save and restart
        client.execute_command('BGSAVE')
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)

        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        # Verify all filters restored
        curr_item_count = self.server.num_keys(client=client)
        assert curr_item_count == count

        # Spot check some filters
        for i in [0, count//2, count-1]:
            name = f"cf{i}key"
            exists = client.execute_command('CF.EXISTS', name, f'value{i}')
            assert exists == 1
            count_val = client.execute_command('CF.COUNT', name, f'value{i}')
            assert count_val == 2

    def test_save_with_deletions(self):
        """Test that deletions are preserved across save/restore"""
        client = self.server.get_new_client()

        # Create filter with items
        client.execute_command('CF.RESERVE', 'delTest', 1000)
        client.execute_command('CF.ADD', 'delTest', 'keep1')
        client.execute_command('CF.ADD', 'delTest', 'keep2')
        client.execute_command('CF.ADD', 'delTest', 'remove1')
        client.execute_command('CF.ADD', 'delTest', 'remove2')

        # Delete some items
        client.execute_command('CF.DEL', 'delTest', 'remove1')
        client.execute_command('CF.DEL', 'delTest', 'remove2')

        # Verify before save
        assert client.execute_command('CF.EXISTS', 'delTest', 'keep1') == 1
        assert client.execute_command('CF.EXISTS', 'delTest', 'remove1') == 0

        # Save and restart
        client.execute_command('BGSAVE')
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)

        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        # Verify deletions persisted
        assert client.execute_command('CF.EXISTS', 'delTest', 'keep1') == 1
        assert client.execute_command('CF.EXISTS', 'delTest', 'keep2') == 1
        assert client.execute_command('CF.EXISTS', 'delTest', 'remove1') == 0
        assert client.execute_command('CF.EXISTS', 'delTest', 'remove2') == 0

    def test_save_scaled_filter(self):
        """Test saving and restoring a filter that has scaled"""
        client = self.server.get_new_client()

        # Create small filter that will scale
        client.execute_command('CF.RESERVE', 'scaleTest', 10, 'EXPANSION', 2)

        # Add enough items to trigger scaling
        for i in range(30):
            client.execute_command('CF.ADD', 'scaleTest', f'item{i}')

        # Get info before save
        info_before = client.execute_command('CF.INFO', 'scaleTest')
        info_dict_before = dict(zip(info_before[::2], info_before[1::2]))

        # Should have scaled
        assert info_dict_before[b'Number of filters'] > 1

        # Save and restart
        client.execute_command('BGSAVE')
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)

        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        # Verify scaled filter restored correctly
        info_after = client.execute_command('CF.INFO', 'scaleTest')
        assert info_after == info_before

        # Verify all items still exist
        for i in range(30):
            exists = client.execute_command('CF.EXISTS', 'scaleTest', f'item{i}')
            assert exists == 1

    def test_save_with_occurrence_counts(self):
        """Test that occurrence counts (duplicates) are preserved"""
        client = self.server.get_new_client()

        client.execute_command('CF.RESERVE', 'countTest', 1000)

        # Add items with different occurrence counts
        for i in range(5):  # item0 added 5 times
            client.execute_command('CF.ADD', 'countTest', 'item0')
        for i in range(3):  # item1 added 3 times
            client.execute_command('CF.ADD', 'countTest', 'item1')
        client.execute_command('CF.ADD', 'countTest', 'item2')  # item2 added 1 time

        # Verify counts before save
        count0_before = client.execute_command('CF.COUNT', 'countTest', 'item0')
        count1_before = client.execute_command('CF.COUNT', 'countTest', 'item1')
        count2_before = client.execute_command('CF.COUNT', 'countTest', 'item2')
        assert count0_before == 5
        assert count1_before == 3
        assert count2_before == 1

        # Save and restart
        client.execute_command('BGSAVE')
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)

        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        # Verify counts preserved
        count0_after = client.execute_command('CF.COUNT', 'countTest', 'item0')
        count1_after = client.execute_command('CF.COUNT', 'countTest', 'item1')
        count2_after = client.execute_command('CF.COUNT', 'countTest', 'item2')
        assert count0_after == 5
        assert count1_after == 3
        assert count2_after == 1
