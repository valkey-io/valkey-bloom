import time
import pytest
from valkey import ResponseError
from valkeytests.valkey_test_case import ValkeyTestCase
from valkeytests.conftest import resource_port_tracker
import logging
import os
import random
import string

def generate_random_string(length=7):
    characters = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(characters) for _ in range(length))
    return random_string

def add_items_till_capacity(client, filter_name, capacity_needed, starting_item_idx, rand_prefix, batch_size=1000):
    new_item_idx = starting_item_idx
    fp_count = 0
    cardinality = client.execute_command(f'BF.CARD {filter_name}')
    while cardinality < capacity_needed:
        # Calculate how many more items we need to add. 
        remaining_capacity = capacity_needed - cardinality
        batch_to_add = min(batch_size, remaining_capacity)        
        # Prepare a batch of items
        items = [f"{rand_prefix}{new_item_idx + i}" for i in range(batch_to_add)]
        new_item_idx += batch_to_add
        result = client.execute_command(f'BF.MADD {filter_name} ' + ' '.join(items))
        # Process results
        for res in result:
            if res == 0:
                fp_count += 1
            elif res == 1:
                cardinality += 1
            else:
                raise RuntimeError(f"Unexpected return value from add_item: {res}")
    return fp_count, new_item_idx - 1

def check_items_exist(client, filter_name, start_idx, end_idx, expected_result, rand_prefix, batch_size=1000):
    error_count = 0
    num_operations = (end_idx - start_idx) + 1
    # Check that items exist in batches.
    for batch_start in range(start_idx, end_idx + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, end_idx)
        # Execute BF.MEXISTS with the batch of items
        items = [f"{rand_prefix}{i}" for i in range(batch_start, batch_end + 1)]
        result = client.execute_command(f'BF.MEXISTS {filter_name} ' + ' '.join(items))
        # Check the results
        for item_result in result:
            if item_result != expected_result:
                error_count += 1
    return error_count, num_operations

def fp_assert(error_count, num_operations, expected_fp_rate, fp_margin):
    real_fp_rate = error_count / num_operations
    fp_rate_with_margin = expected_fp_rate + fp_margin
    
    assert real_fp_rate < fp_rate_with_margin, f"The actual fp_rate, {real_fp_rate}, is greater than the configured fp_rate with margin. {fp_rate_with_margin}."

class TestBloomCorrectness(ValkeyTestCase):

    def get_custom_args(self):
        self.set_server_version(os.environ['SERVER_VERSION'])
        return {
            'loadmodule': os.getenv('MODULE_PATH'),
        }

    def test_non_scaling_filter(self):
        client = self.server.get_new_client()
        item_prefix = generate_random_string()
        # 1 in every 1000 operations is expected to be a false positive.
        expected_fp_rate = 0.001
        capacity = 10000
        # Create a non scaling bloom filter and validate its behavior.
        filter_name = "filter1"
        assert client.execute_command(f'BF.RESERVE {filter_name} {expected_fp_rate} {capacity} NONSCALING') == b"OK"
        # Add items and fill the filter to capacity.
        error_count, add_operation_idx = add_items_till_capacity(client, filter_name, capacity, 1, item_prefix)
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
        assert info_dict[b'Expansion rate'] == -1
        # Use a margin on the expected_fp_rate when asserting for correctness.
        fp_margin = 0.002
        # Validate that item "add" operations on bloom filters are ensuring correctness.
        # False positives should be close to the configured fp_rate.
        fp_assert(error_count, add_operation_idx, expected_fp_rate, fp_margin)
        # Validate item "exists" operations on bloom filters are ensuring correctness.
        # This tests for items already added to the filter and expects them to exist.
        # False negatives should not be possible.
        error_count, num_operations = check_items_exist(
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
        error_count, num_operations = check_items_exist(
            client,
            filter_name,
            add_operation_idx + 1,
            add_operation_idx * 2,
            False,
            item_prefix,
        )
        fp_assert(error_count, num_operations, expected_fp_rate, fp_margin)
        # Create a copy of the bloom filter.
        copy_filter_name = "filter_copy"
        assert client.execute_command(f'COPY {filter_name} {copy_filter_name}') == 1
        assert client.execute_command('DBSIZE') == 2
        copy_info = client.execute_command(f'BF.INFO {copy_filter_name}')
        copy_it = iter(copy_info)
        copy_info_dict = dict(zip(copy_it, copy_it))
        assert copy_info_dict[b'Capacity'] == info_dict[b'Capacity']
        assert copy_info_dict[b'Number of items inserted'] == info_dict[b'Number of items inserted']
        assert copy_info_dict[b'Number of filters'] == info_dict[b'Number of filters']
        assert copy_info_dict[b'Size'] == info_dict[b'Size']
        assert copy_info_dict[b'Expansion rate'] == info_dict[b'Expansion rate']
        # Items added to the original filter should still exist on the copy. False Negatives are not possible.
        error_count, num_operations = check_items_exist(
            client,
            copy_filter_name,
            1,
            add_operation_idx,
            True,
            item_prefix,
        )
        assert error_count == 0
        # Items not added to the original filter should not exist on the copy. False Positives should be close to configured fp_rate.
        error_count, num_operations = check_items_exist(
            client,
            copy_filter_name,
            add_operation_idx + 1,
            add_operation_idx * 2,
            False,
            item_prefix,
        )
        fp_assert(error_count, num_operations, expected_fp_rate, fp_margin)

    def test_scaling_filter(self):
        client = self.server.get_new_client()
        item_prefix = generate_random_string()
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
            error_count, new_add_operation_idx = add_items_till_capacity(client, filter_name, expected_total_capacity, add_operation_idx + 1, item_prefix)
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
        fp_assert(total_error_count, add_operation_idx, expected_fp_rate, fp_margin)
        # Validate item "exists" operations on bloom filters are ensuring correctness.
        # This tests for items already added to the filter and expects them to exist.
        # False negatives should not be possible.
        error_count, num_operations = check_items_exist(
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
        error_count, num_operations = check_items_exist(
            client,
            filter_name,
            add_operation_idx + 1,
            add_operation_idx * 2,
            False,
            item_prefix,
        )
        fp_assert(error_count, num_operations, expected_fp_rate, fp_margin)

        # Track INFO on the scaled out bloom filter.
        info = client.execute_command(f'BF.INFO {filter_name}')
        it = iter(info)
        info_dict = dict(zip(it, it))

        # Create a copy of the scaled out bloom filter.
        copy_filter_name = "filter_copy"
        assert client.execute_command(f'COPY {filter_name} {copy_filter_name}') == 1
        assert client.execute_command('DBSIZE') == 2
        copy_info = client.execute_command(f'BF.INFO {copy_filter_name}')
        copy_it = iter(copy_info)
        copy_info_dict = dict(zip(copy_it, copy_it))
        assert copy_info_dict[b'Capacity'] == info_dict[b'Capacity']
        assert copy_info_dict[b'Number of items inserted'] == info_dict[b'Number of items inserted']
        assert copy_info_dict[b'Number of filters'] == info_dict[b'Number of filters']
        assert copy_info_dict[b'Size'] == info_dict[b'Size']
        assert copy_info_dict[b'Expansion rate'] == info_dict[b'Expansion rate']
        # Items added to the original filter should still exist on the copy. False Negatives are not possible.
        error_count, num_operations = check_items_exist(
            client,
            copy_filter_name,
            1,
            add_operation_idx,
            True,
            item_prefix,
        )
        assert error_count == 0
        # Items not added to the original filter should not exist on the copy. False Positives should be close to configured fp_rate.
        error_count, num_operations = check_items_exist(
            client,
            copy_filter_name,
            add_operation_idx + 1,
            add_operation_idx * 2,
            False,
            item_prefix,
        )
        fp_assert(error_count, num_operations, expected_fp_rate, fp_margin)
