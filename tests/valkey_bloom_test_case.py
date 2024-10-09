import os
import pytest
from valkeytests.valkey_test_case import ValkeyTestCase
from valkey import ResponseError

class ValkeyBloomTestCaseBase(ValkeyTestCase):

    def get_custom_args(self):
        self.set_server_version(os.environ['SERVER_VERSION'])
        return {
            'loadmodule': os.getenv('MODULE_PATH'),
        }

    def verify_error_response(self, client, cmd, expected_err_reply):
        try:
            client.execute_command(cmd)
            assert False
        except ResponseError as e:
            assert_error_msg = f"Actual error message: '{str(e)}' is different from expected error message '{expected_err_reply}'"
            assert str(e) == expected_err_reply, assert_error_msg

    def verify_command_success_reply(self, client, cmd, expected_result):
        try:
            cmd_actual_result = client.execute_command(cmd)
            assert_error_msg = f"Actual command response '{cmd_actual_result}' is different from expected response '{expected_result}'"
            assert cmd_actual_result == expected_result, assert_error_msg
        except:
            print("Something went wrong in command behavior verification")

    def verify_bloom_filter_existence(self, client, key, value, should_exist=True):
        try:
            if should_exist:
                assert client.execute_command(f'BF.EXISTS {key} {value}') == 1, f"Item {key} {value} doesn't exist"
            else:
                assert client.execute_command(f'BF.EXISTS {key} {value}') == 0, f"Item {key} {value} exists"
        except:
            print("Something went wrong in bloom filter item existence verification")

    def verify_key_number(self, client, expected_num_key):
        try:
            actual_num_keys = client.num_keys()
            assert_num_key_error_msg = f"Actual key number {actual_num_keys} is different from expected key number {expected_num_key}"
            assert actual_num_keys == expected_num_key, assert_num_key_error_msg
        except:
            print("Something went wrong in key number verification")

    def insert_bloom_filter(self, client, number_of_bf=5):
        for i in range(number_of_bf):
            assert client.execute_command(f'BF.ADD SAMPLE{i} FOO') == 1, f"Failed to insert bloom filter item SAMPLE{i} FOO"
