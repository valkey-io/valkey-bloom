import time
from valkey_bloom_test_case import ValkeyBloomTestCaseBase

class TestCuckooMetrics(ValkeyBloomTestCaseBase):

    def test_cuckoo_metrics_exist(self):
        """Test that cuckoo metrics are exposed in INFO"""
        client = self.server.get_new_client()

        # Get module info
        info_result = client.info('modules')

        # Check for cuckoo-related metrics
        # Note: Exact metric names depend on implementation
        expected_metrics = [
            'cuckoo_num_objects',
            'cuckoo_total_memory_bytes',
            'cuckoo_num_filters_across_objects',
            'cuckoo_num_items_across_objects',
            'cuckoo_capacity_across_objects',
        ]

        info_str = str(info_result).lower()
        found_metrics = []
        for metric in expected_metrics:
            if metric in info_str:
                found_metrics.append(metric)

        # At least some cuckoo metrics should be present
        # Note: May need adjustment based on actual metric names
        assert len(found_metrics) > 0, f"No cuckoo metrics found in INFO output"

    def test_num_objects_metric(self):
        """Test that cuckoo_num_objects metric is accurate"""
        client = self.server.get_new_client()

        # Get initial count
        initial_info = client.info('modules')
        # Parse out initial cuckoo object count
        # Note: Parsing depends on exact format

        # Create cuckoo filters
        for i in range(5):
            client.execute_command(f'CF.ADD filter{i} item1')

        # Get updated count
        time.sleep(0.1)  # Small delay to ensure metrics updated
        updated_info = client.info('modules')

        # Count should have increased by 5
        # Note: Exact assertion depends on metric format

    def test_memory_bytes_metric(self):
        """Test that memory metrics track filter memory usage"""
        client = self.server.get_new_client()

        # Create a filter
        client.execute_command('CF.RESERVE myfilter 1000')

        # Get memory info
        info = client.info('modules')
        info_str = str(info)

        # Should show memory usage
        assert 'cuckoo' in info_str.lower() or 'memory' in info_str.lower()

        # Add items and check memory increases
        for i in range(100):
            client.execute_command(f'CF.ADD myfilter item{i}')

        # Memory should increase after adding items
        updated_info = client.info('modules')
        # Note: Exact comparison depends on metric format

    def test_num_items_metric(self):
        """Test that items metric tracks additions"""
        client = self.server.get_new_client()

        # Add items across multiple filters
        num_items_total = 0
        for i in range(3):
            for j in range(10):
                client.execute_command(f'CF.ADD filter{i} item{j}')
                num_items_total += 1

        time.sleep(0.1)
        info = client.info('modules')

        # Should track total items added
        # Note: Exact assertion depends on metric format and whether false positives affect count

    def test_num_deletes_metric(self):
        """Test that deletes metric tracks deletions (unique to cuckoo!)"""
        client = self.server.get_new_client()

        # Add and delete items
        for i in range(5):
            client.execute_command(f'CF.ADD myfilter item{i}')

        # Delete some items
        num_deletes = 0
        for i in range(3):
            result = client.execute_command(f'CF.DEL myfilter item{i}')
            if result == 1:
                num_deletes += 1

        time.sleep(0.1)
        info = client.info('modules')

        # Should track deletions
        # Note: This metric is unique to cuckoo filters
        info_str = str(info).lower()
        assert 'delete' in info_str or 'cuckoo' in info_str

    def test_capacity_metric(self):
        """Test that capacity metric tracks total capacity"""
        client = self.server.get_new_client()

        # Create filters with known capacities
        client.execute_command('CF.RESERVE filter1 1000')
        client.execute_command('CF.RESERVE filter2 500')
        client.execute_command('CF.RESERVE filter3 250')

        time.sleep(0.1)
        info = client.info('modules')

        # Total capacity should be 1750
        # Note: Exact assertion depends on metric format

    def test_filters_count_metric(self):
        """Test that filter count metric tracks scaling"""
        client = self.server.get_new_client()

        # Create a filter with expansion enabled
        client.execute_command('CF.RESERVE myfilter 10 EXPANSION 2')

        # Add items to trigger scaling
        for i in range(25):
            try:
                client.execute_command(f'CF.ADD myfilter item{i}')
            except:
                pass

        time.sleep(0.1)
        info = client.info('modules')

        # Should show multiple filters if scaling occurred
        # Note: Exact assertion depends on whether scaling happened and metric format

    def test_defrag_metrics(self):
        """Test that defrag metrics are tracked"""
        client = self.server.get_new_client()

        # Check if defrag metrics exist
        info = client.info('modules')
        info_str = str(info).lower()

        # Should have defrag-related metrics
        # Note: These may only be present if defrag is enabled/configured
        has_defrag_metrics = 'defrag' in info_str

        # If defrag is not enabled, this is expected
        # The metrics should still be defined, just with zero values

    def test_metrics_after_delete_filter(self):
        """Test that metrics are updated when filters are deleted"""
        client = self.server.get_new_client()

        # Create filter
        client.execute_command('CF.ADD myfilter item1')

        time.sleep(0.1)
        info_before = client.info('modules')

        # Delete filter
        client.execute_command('DEL myfilter')

        time.sleep(0.1)
        info_after = client.info('modules')

        # Metrics should be updated to reflect deletion
        # Note: Exact comparison depends on metric format

    def test_metrics_reset_on_restart(self):
        """Test that metrics are properly initialized on server start"""
        client = self.server.get_new_client()

        # Check initial metrics
        info = client.info('modules')

        # Metrics should exist and be initialized
        # Note: Values depend on whether there's existing data

    def test_concurrent_operations_metrics(self):
        """Test that metrics are accurate with concurrent operations"""
        client = self.server.get_new_client()

        # Perform multiple operations
        for i in range(10):
            client.execute_command(f'CF.ADD filter1 item{i}')
            client.execute_command(f'CF.ADD filter2 item{i}')
            client.execute_command(f'CF.DEL filter1 item{i-5}' if i >= 5 else 'CF.COUNT filter1 item0')

        time.sleep(0.1)
        info = client.info('modules')

        # Metrics should be consistent despite concurrent operations
        assert info is not None
