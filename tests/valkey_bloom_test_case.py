import os
import pytest
from valkeytests.valkey_test_case import ValkeyTestCase
from valkey import ResponseError
import random
import string

class ValkeyBloomTestCaseBase(ValkeyTestCase):

    # Global Parameterized Configs
    use_random_seed = 'no'

    def get_custom_args(self):
        self.set_server_version(os.environ['SERVER_VERSION'])
        return {
            'loadmodule': os.getenv('MODULE_PATH'),
            'bf.bloom-use-random-seed': self.use_random_seed,
        }

    @pytest.fixture(autouse=True)
    def use_random_seed_fixture(self, bloom_config_parameterization):
        if bloom_config_parameterization == "random-seed":
            self.use_random_seed = "yes"
        elif bloom_config_parameterization == "fixed-seed":
            self.use_random_seed = "no"

    def verify_error_response(self, client, cmd, expected_err_reply):
        try:
            client.execute_command(cmd)
            assert False
        except ResponseError as e:
            assert_error_msg = f"Actual error message: '{str(e)}' is different from expected error message '{expected_err_reply}'"
            assert str(e) == expected_err_reply, assert_error_msg

    def verify_command_success_reply(self, client, cmd, expected_result):
        cmd_actual_result = client.execute_command(cmd)
        assert_error_msg = f"Actual command response '{cmd_actual_result}' is different from expected response '{expected_result}'"
        # For MEXISTS, MADD and INSERT due to false positives checking the return value can be flaky so instead we check that we get the correct 
        # number of results returned for the command
        if cmd.upper().startswith("BF.M") or cmd.upper().startswith("BF.INSERT") :
            assert len(cmd_actual_result) == expected_result, assert_error_msg
            for value in cmd_actual_result:
                assert value in [0, 1], f"Returned value: {value} is not 0 or 1"
        else:
            assert cmd_actual_result == expected_result, assert_error_msg

    def verify_bloom_filter_item_existence(self, client, key, value, should_exist=True):
        if should_exist:
            assert client.execute_command(f'BF.EXISTS {key} {value}') == 1, f"Item {key} {value} doesn't exist"
        else:
            assert client.execute_command(f'BF.EXISTS {key} {value}') == 0, f"Item {key} {value} exists"

    def verify_server_key_count(self, client, expected_num_keys):
        actual_num_keys = client.info_obj().num_keys()
        assert_num_key_error_msg = f"Actual key number {actual_num_keys} is different from expected key number {expected_num_keys}"
        assert actual_num_keys == expected_num_keys, assert_num_key_error_msg

    def create_bloom_filters_and_add_items(self, client, number_of_bf=5):
        """ Creates the specified number of bloom filter objects (`number_of_bf`) and adds an item to it named FOO.
        """
        for i in range(number_of_bf):
            assert client.execute_command(f'BF.ADD SAMPLE{i} FOO') == 1, f"Failed to insert bloom filter item SAMPLE{i} FOO"

    def generate_random_string(self, length=7):
        """ Creates a random string with specified length.
        """
        characters = string.ascii_letters + string.digits
        random_string = ''.join(random.choice(characters) for _ in range(length))
        return random_string

    def add_items_till_scaling_failure(self, client, filter_name, starting_item_idx, rand_prefix):
        """
        Adds items to the provided bloom filter object (filter_name) until we get a scaling error.
        Item names will start with the provided prefix (rand_prefix) followed by a counter (starting_item_idx onwards).
        """
        new_item_idx = starting_item_idx
        try:
            while True:
                item = f"{rand_prefix}{new_item_idx}"
                new_item_idx += 1
                result = client.execute_command(f'BF.ADD {filter_name} {item}')
                if result == 1:
                    raise RuntimeError("Unexpected return value 1 from BF.ADD")
        except Exception as e:
            if "non scaling filter is full" in str(e):
                return
            else:
                raise RuntimeError(f"Unexpected error BF.ADD: {e}")

    def add_items_till_capacity(self, client, filter_name, capacity_needed, starting_item_idx, rand_prefix, batch_size=1000):
        """
        Adds items to the provided bloom filter object (filter_name) until the specified capacity is reached.
        Item names will start with the provided prefix (rand_prefix) followed by a counter (starting_item_idx onwards).
        """
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

    def check_items_exist(self, client, filter_name, start_idx, end_idx, expected_result, rand_prefix, batch_size=1000):
        """
        Executes BF.MEXISTS on the given bloom filter. Items that we expect to exist are those starting with
        rand_prefix, followed by a number beginning with start_idx. The result is compared with `expected_result` based
        on whether we expect the item to exist or not.
        """
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

    def fp_assert(self, error_count, num_operations, expected_fp_rate, fp_margin):
        """
        Asserts that the actual false positive error rate is lower than the expected false positive rate with
        accounting for margin.
        """
        real_fp_rate = error_count / num_operations
        fp_rate_with_margin = expected_fp_rate + fp_margin
        assert real_fp_rate < fp_rate_with_margin, f"The actual fp_rate, {real_fp_rate}, is greater than the configured fp_rate with margin. {fp_rate_with_margin}."

    def validate_copied_bloom_correctness(self, client, original_filter_name, item_prefix, add_operation_idx, expected_fp_rate, fp_margin, original_info_dict):
        """ Validate correctness on a copy of the provided bloom filter.
        """
        copy_filter_name = "filter_copy"
        assert client.execute_command(f'COPY {original_filter_name} {copy_filter_name}') == 1
        object_digest = client.execute_command(f'DEBUG DIGEST-VALUE {original_filter_name}')
        copied_object_digest = client.execute_command(f'DEBUG DIGEST-VALUE {copy_filter_name}')
        assert copied_object_digest == object_digest
        assert client.execute_command('DBSIZE') == 2
        copy_info = client.execute_command(f'BF.INFO {copy_filter_name}')
        copy_it = iter(copy_info)
        copy_info_dict = dict(zip(copy_it, copy_it))
        assert copy_info_dict[b'Capacity'] == original_info_dict[b'Capacity']
        assert copy_info_dict[b'Number of items inserted'] == original_info_dict[b'Number of items inserted']
        assert copy_info_dict[b'Number of filters'] == original_info_dict[b'Number of filters']
        assert copy_info_dict[b'Size'] == original_info_dict[b'Size']
        assert copy_info_dict[b'Expansion rate'] == original_info_dict[b'Expansion rate']
        # Items added to the original filter should still exist on the copy. False Negatives are not possible.
        error_count, num_operations = self.check_items_exist(
            client,
            copy_filter_name,
            1,
            add_operation_idx,
            True,
            item_prefix,
        )
        assert error_count == 0
        # Items not added to the original filter should not exist on the copy. False Positives should be close to configured fp_rate.
        error_count, num_operations = self.check_items_exist(
            client,
            copy_filter_name,
            add_operation_idx + 1,
            add_operation_idx * 2,
            False,
            item_prefix,
        )
        self.fp_assert(error_count, num_operations, expected_fp_rate, fp_margin)

    def calculate_expected_capacity(self, initial_capacity, expansion, num_filters):
        """
            This function accepts the starting capacity (of the first filter), expansion and number of filters in
            the object to calculate the expected total capacity (across all the filters) within the bloom object.
        """
        curr_filt_capacity = initial_capacity
        total_capacity = curr_filt_capacity
        for i in range(2, num_filters + 1):
            new_filt_capacity = curr_filt_capacity * expansion
            curr_filt_capacity = new_filt_capacity
            total_capacity += curr_filt_capacity
        return total_capacity

    def verify_bloom_metrics(self, info_response, expected_memory, expected_num_objects, expected_num_filters, expected_num_items, expected_sum_capacity):
        """
            Verify the metric values are recorded properly, the expected values are as below
            expected_memory: the size of the memory used by the objects
            expected_num_objects: the number of module objects stored
            expected_num_filters: the number of filters currently created
        """
        response_str = info_response.decode('utf-8')
        lines = response_str.split('\r\n')
        total_memory_bites = -1
        num_objects = -1
        num_filters = -1
        num_items = -1
        sum_capacity = -1
        for line in lines:
            if line.startswith('bf_bloom_total_memory_bytes:'):
                total_memory_bites = int(line.split(':')[1])
            elif line.startswith('bf_bloom_num_objects:'):
                num_objects = int(line.split(':')[1])
            elif line.startswith('bf_bloom_num_filters_across_objects'):
                num_filters = int(line.split(':')[1])
            elif line.startswith('bf_bloom_num_items_across_objects'):
                num_items = int(line.split(':')[1])
            elif line.startswith('bf_bloom_capacity_across_objects'):
                sum_capacity = int(line.split(':')[1])

        assert total_memory_bites == expected_memory 
        assert num_objects == expected_num_objects
        assert num_filters == expected_num_filters
        assert num_items == expected_num_items
        assert sum_capacity == expected_sum_capacity

    """
    This method will parse the return of an INFO command and return a python dict where each metric is a key value pair.
    We can pass in specific sections in order to not have the dict store irrelevant fields related to what we want to check.
    Example of parsing the returned dict:
        stats = self.parse_valkey_info("STATS")
        stats.get('active_defrag_misses')
    """
    def parse_valkey_info(self, section):
        mem_info = self.client.execute_command('INFO ' + section)
        lines = mem_info.decode('utf-8').split('\r\n')        
        stats_dict = {}
        for line in lines:
            if ':' in line:
                key, value = line.split(':', 1)
                stats_dict[key.strip()] = value.strip()
        return stats_dict
