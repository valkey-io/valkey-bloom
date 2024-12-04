import time
from valkeytests.valkey_test_case import ValkeyAction
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytests.conftest import resource_port_tracker
from util.waiters import *
import pytest

@pytest.mark.skip_for_asan(reason="These tests are skipped due to not being able to set activedefrag to yes when valkey server is an ASAN build")
class TestBloomDefrag(ValkeyBloomTestCaseBase):
    
    def test_bloom_defrag(self):        
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


        # Create fragmentation by inserting and deleting data
        scale_names = [f'scale_{i}' for i in range(1, 2000)]
        # Insert data
        for scale in scale_names:
            command = f'bf.insert {scale} CAPACITY 1 EXPANSION 2 ITEMS ' + ' '.join(str(i) for i in range(1, 100))
            self.client.execute_command(command)

        # Delete every other item to create fragmentation
        for scale in scale_names[::2]:
            self.client.execute_command(f'DEL {scale}')
        # Add a wait due to lazy delete where if we call info to early we wont get the correct memory info
        time.sleep(5)
        # Will probably want to check a field in this instead of printing once we understand more
        print("Memory info after insertions and deletions:")
        print(self.client.execute_command('INFO MEMORY'))
        memory_info_non_defragged = self.parse_valkey_info("MEMORY")

        # Enable defragmentation and defrag items.
        self.client.config_set('activedefrag', 'yes')
        self.wait_for_defrag(initial_defrag_hits, initial_defrag_misses)

        # Will probably want to check a field in this instead of printing once we understand more
        print("Memory info after first defragmentation:")
        memory_info_after_defrag = self.parse_valkey_info("MEMORY")

        first_defrag_stats = self.parse_valkey_info("STATS")
        first_defrag_hits = int(first_defrag_stats.get('active_defrag_hits', 0))
        first_defrag_misses = int(first_defrag_stats.get('active_defrag_misses', 0))

        # Assertion we got hits and misses when defragging
        assert first_defrag_hits > initial_defrag_hits and first_defrag_misses > initial_defrag_misses
        assert float(memory_info_after_defrag.get('allocator_frag_ratio', 0)) < float(memory_info_non_defragged.get('allocator_frag_ratio', 0))
        # Check that items we added still exist in the respective bloom objects
        counter = 1
        for scale in scale_names[1::2]:
            command = f'bf.exists {scale} {str(counter)}'
            assert self.client.execute_command(command) == 1
            counter += 1
            if counter >= 100:
                counter = 1

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

        # Defrag items again and make sure no crashes happen
        self.wait_for_defrag(first_defrag_hits, first_defrag_misses)

        final_stats = self.parse_valkey_info("STATS")
        final_defrag_hits = int(final_stats.get('active_defrag_hits', 0))
        final_defrag_misses = int(final_stats.get('active_defrag_misses', 0))
        assert  final_defrag_hits > initial_defrag_hits or final_defrag_misses > initial_defrag_misses, "No defragmentation occurred after RDB load"
        # Check that items we added still exist in the respective bloom objects
        counter = 1
        for scale in scale_names[1::2]:
            command = f'bf.exists {scale} {str(counter)}'
            assert self.client.execute_command(command) == 1
            counter += 1
            if counter >= 100:
                counter = 1

    def parse_valkey_info(self, section):
        mem_info = self.client.execute_command('INFO ' + section)
        print (mem_info)
        lines = mem_info.decode('utf-8').split('\r\n')        
        stats_dict = {}
        for line in lines:
            if ':' in line:
                key, value = line.split(':', 1)
                stats_dict[key.strip()] = value.strip()
        return stats_dict
    
    def wait_for_defrag(self, initial_hits, initial_misses):
        max_wait_time = 20
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            time.sleep(10)
            stats = self.parse_valkey_info("STATS")
            defrag_hits = int(stats.get('active_defrag_hits', 0))
            defrag_misses = int(stats.get('active_defrag_misses', 0))
            
            if defrag_hits > initial_hits or defrag_misses > initial_misses:
                return
            

    def test_bloom_defrag_non_scale(self):        
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


        # Create fragmentation by inserting and deleting data
        scale_names = [f'scale_{i}' for i in range(1, 2000)]
        # Insert data
        for scale in scale_names:
            command = f'bf.insert {scale} CAPACITY 200 EXPANSION 2 ITEMS ' + ' '.join(str(i) for i in range(1, 100))
            self.client.execute_command(command)

        # Delete every other item to create fragmentation
        for scale in scale_names[::2]:
            self.client.execute_command(f'DEL {scale}')
        # Add a wait due to lazy delete where if we call info to early we wont get the correct memory info
        time.sleep(5)
        # Will probably want to check a field in this instead of printing once we understand more
        print("Memory info after insertions and deletions:")
        print(self.client.execute_command('INFO MEMORY'))
        memory_info_non_defragged = self.parse_valkey_info("MEMORY")

        # Enable defragmentation and defrag items.
        self.client.config_set('activedefrag', 'yes')
        self.wait_for_defrag(initial_defrag_hits, initial_defrag_misses)

        # Will probably want to check a field in this instead of printing once we understand more
        print("Memory info after first defragmentation:")
        memory_info_after_defrag = self.parse_valkey_info("MEMORY")

        first_defrag_stats = self.parse_valkey_info("STATS")
        first_defrag_hits = int(first_defrag_stats.get('active_defrag_hits', 0))
        first_defrag_misses = int(first_defrag_stats.get('active_defrag_misses', 0))

        # Assertion we got hits and misses when defragging
        assert first_defrag_hits > initial_defrag_hits and first_defrag_misses > initial_defrag_misses
        assert float(memory_info_after_defrag.get('allocator_frag_ratio', 0)) < float(memory_info_non_defragged.get('allocator_frag_ratio', 0))
        # Check that items we added still exist in the respective bloom objects
        counter = 1
        for scale in scale_names[1::2]:
            command = f'bf.exists {scale} {str(counter)}'
            assert self.client.execute_command(command) == 1
            counter += 1
            if counter >= 100:
                counter = 1
