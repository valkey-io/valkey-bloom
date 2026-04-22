import time
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *
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
            # Use the key name as item prefix so each filter gets a unique digest
            # The new_add_operation_idx means all numbers from 1 to it should all return 1 when called with bf.exists
            _, new_add_operation_idx = self.add_items_till_capacity(self.client, scale, 100,  1, f"{scale}_")
            # We delete every other object so only need to keep the ones with a odd index
            if index % 2 == 1:
                num_items_inserted_per_object.append(new_add_operation_idx)

        # Delete every other item to create fragmentation
        for scale in scale_names[::2]:
            self.client.execute_command(f'DEL {scale}')
        remaining_keys = scale_names[1::2]
        digests_before_defrag = {scale: self.client.execute_command(f'DEBUG DIGEST-VALUE {scale}') for scale in remaining_keys}

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
        self.check_values_present(remaining_keys, num_items_inserted_per_object)

        digests_after_defrag = {scale: self.client.execute_command(f'DEBUG DIGEST-VALUE {scale}') for scale in remaining_keys}
        assert digests_before_defrag == digests_after_defrag, "Digest mismatch after defrag"

        info_results = self.client.info("bf")
        assert info_results['bf_bloom_defrag_hits'] + info_results['bf_bloom_defrag_misses'] > 0
        self.client.execute_command('BGSAVE')
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
        self.check_values_present(remaining_keys, num_items_inserted_per_object)

        digests_after_rdb_load = {scale: self.client.execute_command(f'DEBUG DIGEST-VALUE {scale}') for scale in remaining_keys}
        assert digests_before_defrag == digests_after_rdb_load, "Digest mismatch after RDB load and defrag"
        info_results = self.client.info("bf")
        assert info_results['bf_bloom_defrag_hits'] + info_results['bf_bloom_defrag_misses'] > 0
 
    def test_bloom_defrag_then_scale(self):
        # Set defragmentation thresholds
        self.client.config_set('activedefrag', 'no')
        self.client.config_set('active-defrag-ignore-bytes', '1')
        self.client.config_set('active-defrag-threshold-lower', '2')

        max_memory = 40 * 1024 * 1024
        self.client.config_set('maxmemory', str(max_memory))

        # Each bloom object is (reserve arguments, items before defrag, items after defrag).
        BLOOM_OBJECTS = [
            # Non-scaling: 1 filter
            ("0.01 100 NONSCALING", 80,  90),
            ("0.001 200 NONSCALING", 150, 180),
            ("0.05 50 NONSCALING", 40,  45),
            # Expansion 1:
            # 3 filters -> scale to 5
            ("0.01 100 EXPANSION 1", 300, 500),
            # 1 filter -> scale to 3
            ("0.001 200 EXPANSION 1", 200, 600),
            # 7 filters -> scale to 8
            ("0.01 50 EXPANSION 1", 350, 400),
            # 2 filters -> scale to 5
            ("0.001 150 EXPANSION 1", 300, 750),
            # Expansion 2:
            # 1 filter -> scale to 3
            ("0.01 100 EXPANSION 2", 100, 400),
            # 3 filters -> scale to 5
            ("0.001 50 EXPANSION 2", 200, 800),
            # Expansion 3:
            # 1 filter -> scale to 3 
            ("0.01 30 EXPANSION 3", 30,  200),
        ]

        all_keys = []

        # Create 200 bloom filters of each parameter above (2400 total)
        for bloom_example in range(len(BLOOM_OBJECTS)):
            for i in range(200):
                reserve_args, items_before, _ = BLOOM_OBJECTS[bloom_example]
                key = f'defrag_test_{bloom_example}_{i}'
                all_keys.append(key)
                self.client.execute_command(f'BF.RESERVE {key} {reserve_args}')
                self.add_items_till_capacity(self.client, key, items_before, 1, f"{key}_")

        # Delete half the keys of each different bloom object parameters
        for key in all_keys[::2]:
            self.client.execute_command(f'DEL {key}')
        remaining_keys = all_keys[1::2]

        digests_before_defrag = {
            key: self.client.execute_command(f'DEBUG DIGEST-VALUE {key}')
            for key in remaining_keys
        }

        # Enable defragmentation and defrag items.
        self.client.config_set('activedefrag', 'yes')
        wait_for_equal(lambda: int(self.parse_valkey_info("STATS").get('total_active_defrag_time')) > 5000, True)

        items_before_per_key = [BLOOM_OBJECTS[int(key.split('_')[2])][1] for key in remaining_keys]
        self.check_values_present(remaining_keys, items_before_per_key)

        digests_after_defrag = {
            key: self.client.execute_command(f'DEBUG DIGEST-VALUE {key}')
            for key in remaining_keys
        }
        assert digests_before_defrag == digests_after_defrag, "Digest mismatch after defrag"

        for key in remaining_keys:
            bloom_object_index = int(key.split('_')[2])
            _, items_before, items_after = BLOOM_OBJECTS[bloom_object_index]
            self.add_items_till_capacity(self.client, key, items_after, items_before + 1, f"{key}_")

        # Create a list of number of keys left after defrag and scaling
        items_after_per_key = [BLOOM_OBJECTS[int(key.split('_')[2])][2] for key in remaining_keys]
        self.check_values_present(remaining_keys, items_after_per_key)

        self.client.execute_command('BGSAVE')
        self.server.wait_for_save_done()

        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        self.check_values_present(remaining_keys, items_after_per_key)

    def check_values_present(self, keys, num_items_per_key):
        """Verify all items are present in each bloom filter. No false negatives allowed.
        keys: list of key names
        num_items_per_key: list of item counts (same length as keys), or a single int for all keys
        """
        if isinstance(num_items_per_key, int):
            num_items_per_key = [num_items_per_key] * len(keys)
        for key, num_items in zip(keys, num_items_per_key):
            items = [f'{key}_{i}' for i in range(1, num_items + 1)]
            results = self.client.execute_command(f'BF.MEXISTS {key} ' + ' '.join(items))
            expected_results = [1] * num_items
            assert results == expected_results, f"Unexpected results for {key}: {results}"
