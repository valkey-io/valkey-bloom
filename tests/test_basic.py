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
        module_list_data = client.execute_command('MODULE LIST')
        module_list_count = len(module_list_data)
        assert module_list_count == 1
        module_loaded = False
        for module in module_list_data:
            if (module[b'name'] == b'bf'):
                module_loaded = True
                break
        assert(module_loaded)
        bf_add_result = client.execute_command('BF.ADD filter1 item1')
        assert bf_add_result == 1
        bf_exists_result = client.execute_command('BF.EXISTS filter1 item1')
        assert bf_exists_result == 1
        bf_exists_result = client.execute_command('BF.EXISTS filter1 item2')
        assert bf_exists_result == 0
