import time
from valkeytests.valkey_test_case import ValkeyAction
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytests.conftest import resource_port_tracker
from util.waiters import *
import pytest

@pytest.mark.skip_for_asan(reason="These tests are skipped due to not being able to set activedefrag to yes when valkey server is an ASAN build")
class TestBloomDefrag(ValkeyBloomTestCaseBase):
    
    @pytest.mark.parametrize("initial_capacity", [1, 200])
    def test_bloom_defrag(self, initial_capacity):
        # Set defragmentation thresholds
        self.client.config_set('activedefrag', 'no')
        self.client.config_set('active-defrag-ignore-bytes', '1')
        self.client.config_set('active-defrag-threshold-lower', '2')
        
        # Set a lower maxmemory
        max_memory = 20 * 1024 * 1024 
        self.client.config_set('maxmemory', str(max_memory))

        # Initial stats
        stats = self.parse_valkey_info("STATS")
        initial_defrag_hits = int(stats.get('active_defrag_hits', 0))
        initial_defrag_misses = int(stats.get('active_defrag_misses', 0))


        # Create list of key names we will operate with
        scale_names = [f'scale_{i}' for i in range(1, 2000)]
        # A list of the number of items we inserted into each Bloom object
        num_items_inserted_per_object = []

        # Insert data
        for index, scale in enumerate(scale_names):
            self.client.execute_command(f'bf.reserve {scale} 0.001 {initial_capacity} EXPANSION 2')
            # The new_add_operation_idx means all numbers from 1 to it should all return 1 when called with bf.exists
            _, new_add_operation_idx = self.add_items_till_capacity(self.client, scale, 100,  1, "")
            # We delete every other object so only need to keep the ones with a odd index
            if index % 2 == 1:
                num_items_inserted_per_object.append(new_add_operation_idx)

        # Delete every other item to create fragmentation
        for scale in scale_names[::2]:
            self.client.execute_command(f'DEL {scale}')
        # Add a wait due to lazy delete where if we call info to early we wont get the correct memory info
        time.sleep(5)

        # Get the memory info before we start actively defragging
        memory_info_non_defragged = self.parse_valkey_info("MEMORY")

        # Enable defragmentation and defrag items.
        self.client.config_set('activedefrag', 'yes')
        # Wait for 5 seconds of defragging. Returned time is in milliseconds. 
        wait_for_equal(lambda: int(self.parse_valkey_info("STATS").get('total_active_defrag_time')) > 5000, True)

        # Get the memory info after we have defragged
        memory_info_after_defrag = self.parse_valkey_info("MEMORY")

        first_defrag_stats = self.parse_valkey_info("STATS")
        first_defrag_hits = int(first_defrag_stats.get('active_defrag_hits', 0))
        first_defrag_misses = int(first_defrag_stats.get('active_defrag_misses', 0))

        # Assertion we got hits and misses when defragging
        assert first_defrag_hits > initial_defrag_hits and first_defrag_misses > initial_defrag_misses
        assert float(memory_info_after_defrag.get('allocator_frag_ratio', 0)) < float(memory_info_non_defragged.get('allocator_frag_ratio', 0))
        # Check that items we added still exist in the respective bloom objects
        self.check_values_present(scale_names, num_items_inserted_per_object)
        info_results = self.client.info_section("bf")
        assert info_results.info['bf_bloom_defrag_hits'] + info_results.info['bf_bloom_defrag_misses'] > 0
        self.client.bgsave()
        self.server.wait_for_save_done()

        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        # Set config as we had before saving and restarting
        self.client.config_set('activedefrag', 'yes')
        self.client.config_set('active-defrag-ignore-bytes', '1')
        self.client.config_set('active-defrag-threshold-lower', '2')
        self.client.config_set('maxmemory', str(max_memory))

        # Wait for 5 seconds of defragging. Returned time is in milliseconds. 
        wait_for_equal(lambda: int(self.parse_valkey_info("STATS").get('total_active_defrag_time')) > 5000, True)

        final_stats = self.parse_valkey_info("STATS")
        final_defrag_hits = int(final_stats.get('active_defrag_hits', 0))
        final_defrag_misses = int(final_stats.get('active_defrag_misses', 0))
        assert  final_defrag_hits > initial_defrag_hits or final_defrag_misses > initial_defrag_misses, "No defragmentation occurred after RDB load"
        # Check that items we added still exist in the respective bloom objects
        self.check_values_present(scale_names, num_items_inserted_per_object)
        info_results = self.client.info_section("bf")
        assert info_results.info['bf_bloom_defrag_hits'] + info_results.info['bf_bloom_defrag_misses'] > 0
 
    def check_values_present(self, scale_names, num_items_inserted_per_object):
        for index, scale in enumerate(scale_names[1::2]):
            # Create a list of numbers of 1 to number of items inserted into the current bloomfilter
            num_items = num_items_inserted_per_object[index]
            items = list(range(1, num_items + 1))
            
            # Perform a mexists for all the numbers 1 to num_items
            command = f'bf.mexists {scale} ' + ' '.join(map(str, items))
            results = self.client.execute_command(command)
            # All items should be present so we compare with an array of length num items where all items are 1
            expected_results = [1] * num_items
            assert results == expected_results, f"Unexpected results for scale {scale}: {results}"
