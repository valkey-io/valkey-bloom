import time
import pytest
from valkey import ResponseError
from valkeytests.valkey_test_case import ValkeyTestCase
from valkeytests.conftest import resource_port_tracker
import logging
import os

class TestBloomBasic(ValkeyTestCase):

    def get_custom_args(self):
        self.set_server_version(os.environ['SERVER_VERSION'])
        return {
            'loadmodule': os.getenv('MODULE_PATH'),
        }

    def test_basic(self):
        client = self.server.get_new_client()
        # Validate that the valkey-bloom module is loaded.
        module_list_data = client.execute_command('MODULE LIST')
        module_list_count = len(module_list_data)
        assert module_list_count == 1
        module_loaded = False
        for module in module_list_data:
            if (module[b'name'] == b'bf'):
                module_loaded = True
                break
        assert(module_loaded)
        # Validate that all the BF.* commands are supported on the server.
        command_cmd_result = client.execute_command('COMMAND')
        bf_cmds = ["BF.ADD", "BF.EXISTS", "BF.MADD", "BF.MEXISTS", "BF.INFO", "BF.CARD", "BF.RESERVE", "BF.INSERT"]
        assert all(item in command_cmd_result for item in bf_cmds)
        # Basic bloom filter create, item add and item exists validation.
        bf_add_result = client.execute_command('BF.ADD filter1 item1')
        assert bf_add_result == 1
        bf_exists_result = client.execute_command('BF.EXISTS filter1 item1')
        assert bf_exists_result == 1
        bf_exists_result = client.execute_command('BF.EXISTS filter1 item2')
        assert bf_exists_result == 0

    def test_copy_and_exists_cmd(self):
        client = self.server.get_new_client()
        madd_result = client.execute_command('BF.MADD filter item1 item2 item3 item4')
        assert client.execute_command('EXISTS filter') == 1
        mexists_result = client.execute_command('BF.MEXISTS filter item1 item2 item3 item4')
        assert len(madd_result) == 4 and len(mexists_result) == 4
        assert client.execute_command('COPY filter new_filter') == 1
        assert client.execute_command('EXISTS new_filter') == 1
        copy_mexists_result = client.execute_command('BF.MEXISTS new_filter item1 item2 item3 item4')
        assert mexists_result == copy_mexists_result
    
    def test_memory_usage_cmd(self):
        client = self.server.get_new_client()
        assert client.execute_command('BF.ADD filter item1') == 1
        memory_usage = client.execute_command('MEMORY USAGE filter')
        info_size = client.execute_command('BF.INFO filter SIZE')
        assert memory_usage > info_size and info_size > 0 # DIFF is 32 bytes

    def test_module_data_type(self):
        # Validate the name of the Module data type.
        client = self.server.get_new_client()
        assert client.execute_command('BF.ADD filter item1') == 1
        type_result = client.execute_command('TYPE filter')
        assert type_result == b"bloomfltr"
        # Validate the name of the Module data type.
        encoding_result = client.execute_command('OBJECT ENCODING filter')
        assert encoding_result == b"raw"
