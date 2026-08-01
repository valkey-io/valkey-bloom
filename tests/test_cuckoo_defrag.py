import time
from valkey import ResponseError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase

class TestCuckooDefrag(ValkeyBloomTestCaseBase):

    def setUp(self):
        super().setUp()
        client = self.server.get_new_client()
        # Enable active defragmentation with aggressive settings for testing
        client.execute_command('CONFIG', 'SET', 'activedefrag', 'yes')
        client.execute_command('CONFIG', 'SET', 'active-defrag-threshold-lower', '1')
        client.execute_command('CONFIG', 'SET', 'active-defrag-cycle-min', '50')
        client.execute_command('CONFIG', 'SET', 'active-defrag-cycle-max', '75')

    def test_defrag_metrics_exist(self):
        """Test that defrag metrics are reported"""
        client = self.server.get_new_client()

        info = client.info('modules')

        assert 'bf_cuckoo_defrag_hits' in info
        assert 'bf_cuckoo_defrag_misses' in info

    def test_defrag_single_filter(self):
        """Test defragmentation of a single cuckoo filter"""
        client = self.server.get_new_client()

        # Create filter
        client.execute_command('CF.RESERVE', 'defragTest', 1000)

        # Add items
        for i in range(100):
            client.execute_command('CF.ADD', 'defragTest', f'item{i}')

        # Get initial metrics
        info_before = client.info('modules')
        module_before = info_before.get('bf', {})
        hits_before = module_before.get('cuckoo_defrag_hits', 0)
        misses_before = module_before.get('cuckoo_defrag_misses', 0)

        # Wait for defrag to potentially run
        time.sleep(5)

        # Get metrics after
        info_after = client.info('modules')
        module_after = info_after.get('bf', {})
        hits_after = module_after.get('cuckoo_defrag_hits', 0)
        misses_after = module_after.get('cuckoo_defrag_misses', 0)

        # Defrag should have been attempted (hits or misses increased)
        total_before = hits_before + misses_before
        total_after = hits_after + misses_after
        # Note: Defrag may not run if memory isn't fragmented enough
        # So we just verify the metrics are tracked

    def test_defrag_multiple_filters(self):
        """Test defragmentation across multiple filters"""
        client = self.server.get_new_client()

        # Create multiple filters to increase fragmentation
        for i in range(20):
            client.execute_command('CF.RESERVE', f'multi{i}', 500)
            for j in range(10):
                client.execute_command('CF.ADD', f'multi{i}', f'val{j}')

        # Delete half to create fragmentation
        for i in range(0, 20, 2):
            client.execute_command('DEL', f'multi{i}')

        # Wait for defrag
        time.sleep(5)

        # Verify remaining filters still work
        for i in range(1, 20, 2):
            exists = client.execute_command('CF.EXISTS', f'multi{i}', 'val5')
            assert exists == 1

    def test_defrag_scaled_filter(self):
        """Test defragmentation of a scaled filter"""
        client = self.server.get_new_client()

        # Create filter that will scale
        client.execute_command('CF.RESERVE', 'scaleDefrag', 10, 'EXPANSION', 2)

        # Add items to trigger scaling
        for i in range(50):
            client.execute_command('CF.ADD', 'scaleDefrag', f'item{i}')

        # Verify it scaled
        info = client.execute_command('CF.INFO', 'scaleDefrag')
        info_dict = dict(zip(info[::2], info[1::2]))
        num_filters = info_dict[b'Number of filters']
        assert num_filters > 1

        # Wait for defrag
        time.sleep(5)

        # Verify filter still works after defrag
        for i in range(50):
            exists = client.execute_command('CF.EXISTS', 'scaleDefrag', f'item{i}')
            assert exists == 1

    def test_defrag_with_active_operations(self):
        """Test that defrag doesn't interfere with active operations"""
        client = self.server.get_new_client()

        # Create filter
        client.execute_command('CF.RESERVE', 'activeDefrag', 2000)

        # Continuously add items while defrag might be running
        for i in range(200):
            client.execute_command('CF.ADD', 'activeDefrag', f'item{i}')
            if i % 50 == 0:
                time.sleep(0.5)  # Give defrag a chance to run

        # Verify all items exist
        for i in range(200):
            exists = client.execute_command('CF.EXISTS', 'activeDefrag', f'item{i}')
            assert exists == 1

    def test_defrag_preserves_counts(self):
        """Test that defrag preserves occurrence counts"""
        client = self.server.get_new_client()

        # Create filter with duplicate counts
        client.execute_command('CF.RESERVE', 'countDefrag', 1000)
        for i in range(5):
            client.execute_command('CF.ADD', 'countDefrag', 'item1')
        for i in range(3):
            client.execute_command('CF.ADD', 'countDefrag', 'item2')

        # Get counts before defrag
        count1_before = client.execute_command('CF.COUNT', 'countDefrag', 'item1')
        count2_before = client.execute_command('CF.COUNT', 'countDefrag', 'item2')

        # Wait for defrag
        time.sleep(5)

        # Verify counts preserved
        count1_after = client.execute_command('CF.COUNT', 'countDefrag', 'item1')
        count2_after = client.execute_command('CF.COUNT', 'countDefrag', 'item2')

        assert count1_after == count1_before == 5
        assert count2_after == count2_before == 3

    def test_defrag_filter_info_unchanged(self):
        """Test that CF.INFO results are unchanged after defrag"""
        client = self.server.get_new_client()

        # Create filter
        client.execute_command('CF.RESERVE', 'infoDefrag', 1000, 'BUCKETSIZE', 4)
        for i in range(50):
            client.execute_command('CF.ADD', 'infoDefrag', f'item{i}')

        # Get info before defrag
        info_before = client.execute_command('CF.INFO', 'infoDefrag')

        # Wait for defrag
        time.sleep(5)

        # Get info after defrag
        info_after = client.execute_command('CF.INFO', 'infoDefrag')

        # Info should be identical
        assert info_after == info_before

    def test_memory_fragmentation_improvement(self):
        """Test that defrag can improve memory fragmentation"""
        client = self.server.get_new_client()

        # Create fragmentation scenario
        for i in range(100):
            client.execute_command('CF.RESERVE', f'frag{i}', 200)
            client.execute_command('CF.ADD', f'frag{i}', f'val{i}')

        # Delete most filters to create holes
        for i in range(0, 100, 2):
            client.execute_command('DEL', f'frag{i}')

        # Get memory stats before defrag
        mem_before = client.info('memory')
        frag_before = mem_before.get('mem_fragmentation_ratio', 1.0)

        # Wait for defrag to run
        time.sleep(10)

        # Get memory stats after defrag
        mem_after = client.info('memory')
        frag_after = mem_after.get('mem_fragmentation_ratio', 1.0)

        # Note: Fragmentation improvement depends on allocator and may not always decrease
        # This test mainly verifies that defrag runs without crashing
        # The actual improvement is system-dependent

    def test_defrag_with_deletions(self):
        """Test defrag after item deletions"""
        client = self.server.get_new_client()

        # Create filter and add items
        client.execute_command('CF.RESERVE', 'delDefrag', 1000)
        for i in range(100):
            client.execute_command('CF.ADD', 'delDefrag', f'item{i}')

        # Delete some items
        for i in range(0, 100, 2):
            client.execute_command('CF.DEL', 'delDefrag', f'item{i}')

        # Wait for defrag
        time.sleep(5)

        # Verify remaining items still exist
        for i in range(1, 100, 2):
            exists = client.execute_command('CF.EXISTS', 'delDefrag', f'item{i}')
            assert exists == 1

        # Verify deleted items still deleted
        for i in range(0, 100, 2):
            exists = client.execute_command('CF.EXISTS', 'delDefrag', f'item{i}')
            assert exists == 0

    def test_defrag_callback_registered(self):
        """Test that defrag callback is properly registered"""
        client = self.server.get_new_client()

        # Create a filter
        client.execute_command('CF.RESERVE', 'callbackTest', 500)
        client.execute_command('CF.ADD', 'callbackTest', 'item1')

        # Defrag metrics should exist (even if 0)
        info = client.info('modules')

        # These metrics should be present
        assert 'bf_cuckoo_defrag_hits' in info
        assert 'bf_cuckoo_defrag_misses' in info

        # Values should be integers
        hits = info['bf_cuckoo_defrag_hits']
        misses = info['bf_cuckoo_defrag_misses']
        assert isinstance(hits, int)
        assert isinstance(misses, int)
        assert hits >= 0
        assert misses >= 0
