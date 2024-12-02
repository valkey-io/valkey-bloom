import pytest
from valkeytests.conftest import resource_port_tracker
from valkey_bloom_test_case import ValkeyBloomTestCaseBase

class TestBloomCorrectness(ValkeyBloomTestCaseBase):

    def test_non_scaling_filter(self):
        client = self.server.get_new_client()
        item_prefix = self.generate_random_string()
        # 1 in every 1000 operations is expected to be a false positive.
        expected_fp_rate = 0.001
        capacity = 10000
        # Create a non scaling bloom filter and validate its behavior.
        filter_name = "filter1"
        assert client.execute_command(f'BF.RESERVE {filter_name} {expected_fp_rate} {capacity} NONSCALING') == b"OK"
        # Add items and fill the filter to capacity.
        error_count, add_operation_idx = self.add_items_till_capacity(client, filter_name, capacity, 1, item_prefix)
        with pytest.raises(Exception, match="non scaling filter is full"):
            client.execute_command(f'BF.ADD {filter_name} new_item')
        # Validate that is is filled.
        info = client.execute_command(f'BF.INFO {filter_name}')
        it = iter(info)
        info_dict = dict(zip(it, it))
        assert info_dict[b'Capacity'] == capacity
        assert info_dict[b'Number of items inserted'] == capacity
        assert info_dict[b'Number of filters'] == 1
        assert info_dict[b'Size'] > 0
        assert info_dict[b'Expansion rate'] == None
        # Use a margin on the expected_fp_rate when asserting for correctness.
        fp_margin = 0.002
        # Validate that item "add" operations on bloom filters are ensuring correctness.
        # False positives should be close to the configured fp_rate.
        self.fp_assert(error_count, add_operation_idx, expected_fp_rate, fp_margin)
        # Validate item "exists" operations on bloom filters are ensuring correctness.
        # This tests for items already added to the filter and expects them to exist.
        # False negatives should not be possible.
        error_count, num_operations = self.check_items_exist(
            client,
            filter_name,
            1,
            add_operation_idx,
            True,
            item_prefix,
        )
        assert error_count == 0
        # This tests for items which are not added to the filter and expects them to not exist.
        # False positives should be close to the configured fp_rate.
        error_count, num_operations = self.check_items_exist(
            client,
            filter_name,
            add_operation_idx + 1,
            add_operation_idx * 2,
            False,
            item_prefix,
        )
        self.fp_assert(error_count, num_operations, expected_fp_rate, fp_margin)
        # Validate correctness on a copy of a non scaling bloom filter.
        self.validate_copied_bloom_correctness(client, filter_name, item_prefix, add_operation_idx, expected_fp_rate, fp_margin, info_dict)

    def test_scaling_filter(self):
        client = self.server.get_new_client()
        item_prefix = self.generate_random_string()
        expected_fp_rate = 0.001
        initial_capacity = 10000
        expansion = 2
        num_filters_to_scale = 5
        filter_name = "filter1"
        # Create a scaling bloom filter and validate its behavior.
        assert client.execute_command(f'BF.RESERVE {filter_name} {expected_fp_rate} {initial_capacity} EXPANSION {expansion}') == b"OK"
        info = client.execute_command(f'BF.INFO {filter_name}')
        it = iter(info)
        info_dict = dict(zip(it, it))
        assert info_dict[b'Capacity'] == initial_capacity
        assert info_dict[b'Number of items inserted'] == 0
        assert info_dict[b'Number of filters'] == 1
        assert info_dict[b'Size'] > 0
        assert info_dict[b'Expansion rate'] == expansion

        # Scale out by adding items.
        total_error_count = 0
        add_operation_idx = 0
        for filter_idx in range(1, num_filters_to_scale + 1):
            expected_total_capacity = initial_capacity * ((expansion ** filter_idx) - 1)
            error_count, new_add_operation_idx = self.add_items_till_capacity(client, filter_name, expected_total_capacity, add_operation_idx + 1, item_prefix)
            add_operation_idx = new_add_operation_idx
            total_error_count += error_count
            # Validate from BF.INFO that is filter is scaling correctly.
            info = client.execute_command(f'BF.INFO {filter_name}')
            it = iter(info)
            info_dict = dict(zip(it, it))
            assert info_dict[b'Capacity'] == expected_total_capacity
            assert info_dict[b'Number of items inserted'] == expected_total_capacity
            assert info_dict[b'Number of filters'] == filter_idx
            assert info_dict[b'Size'] > 0
            assert info_dict[b'Expansion rate'] == expansion

        # Use a margin on the expected_fp_rate when asserting for correctness.
        fp_margin = 0.002
        # Validate that item "add" operations on bloom filters are ensuring correctness.
        # False positives should be close to the configured fp_rate.
        self.fp_assert(total_error_count, add_operation_idx, expected_fp_rate, fp_margin)
        # Validate item "exists" operations on bloom filters are ensuring correctness.
        # This tests for items already added to the filter and expects them to exist.
        # False negatives should not be possible.
        error_count, num_operations = self.check_items_exist(
            client,
            filter_name,
            1,
            add_operation_idx,
            True,
            item_prefix,
        )
        assert error_count == 0
        # This tests for items which are not added to the filter and expects them to not exist.
        # False positives should be close to the configured fp_rate.
        error_count, num_operations = self.check_items_exist(
            client,
            filter_name,
            add_operation_idx + 1,
            add_operation_idx * 2,
            False,
            item_prefix,
        )
        self.fp_assert(error_count, num_operations, expected_fp_rate, fp_margin)
        # Track INFO on the scaled out bloom filter.
        info = client.execute_command(f'BF.INFO {filter_name}')
        it = iter(info)
        info_dict = dict(zip(it, it))
        # Validate correctness on a copy of a scaling bloom filter.
        self.validate_copied_bloom_correctness(client, filter_name, item_prefix, add_operation_idx, expected_fp_rate, fp_margin, info_dict)
