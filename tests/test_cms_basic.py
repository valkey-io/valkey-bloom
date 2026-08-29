#!/usr/bin/env python3

import pytest
from valkey import ResponseError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

class TestCMSBasic(ValkeyBloomTestCaseBase):
    """
    Basic coverage for the ability to create count min sketch data structures, add, and query them.
    """

    def test_basic_dim(self):
        client = self.server.get_new_client()
        module_loaded = False
        module_list_data = client.execute_command('MODULE LIST')      
        for module in module_list_data:
            if (module[b'name'] == b'bf'):
                module_loaded = True
                break
        assert(module_loaded)
        # Validate that all the CMS.* commands are supported on the server.
        command_cmd_result = client.execute_command('COMMAND')
        cms_cmds = ["CMS.INITBYDIM", "CMS.INITBYPROB", "CMS.INCRBY", "CMS.QUERY", "CMS.INFO"]
        assert all(item in command_cmd_result for item in cms_cmds)
        #Create CMS by Dimensions, add item, estimate the item, increment, estimate
        assert client.execute_command('CMS.INITBYDIM sketch1 10 5') == b'OK'
        assert client.execute_command('CMS.QUERY sketch1 item1') == [0]
        assert client.execute_command('CMS.INCRBY sketch1 item1 1 item2 1') == [1, 1]

        #CMS guarantees that we have the frequency at LEAST the size of the increment for the item
        assert client.execute_command('CMS.QUERY sketch1 item1')[0] >= 1

        #Check Info piece setup
        assert client.execute_command('CMS.INFO sketch1') == [b'width', 10, b'depth', 5, b'count', 2]
        assert client.execute_command('CMS.INFO sketch1 WIDTH') == 10
        assert client.execute_command('CMS.INFO sketch1 DEPTH') == 5
        assert client.execute_command('CMS.INFO sketch1 COUNT') == 2

    def test_basic_prob(self):
        client = self.server.get_new_client()
        module_loaded = False
        module_list_data = client.execute_command('MODULE LIST')      
        for module in module_list_data:
            if (module[b'name'] == b'bf'):
                module_loaded = True
                break
        assert(module_loaded)
        # Validate that all the CMS.* commands are supported on the server.
        command_cmd_result = client.execute_command('COMMAND')
        cms_cmds = ["CMS.INITBYDIM", "CMS.INITBYPROB", "CMS.INCRBY", "CMS.QUERY", "CMS.INFO"]
        assert all(item in command_cmd_result for item in cms_cmds)
        #Create CMS by Dimensions, add item, estimate the item, increment, estimate
        assert client.execute_command('CMS.INITBYPROB sketch1 0.001 0.01') == b'OK'
        assert client.execute_command('CMS.QUERY sketch1 item1') == [0]
        assert client.execute_command('CMS.INCRBY sketch1 item1 1 item2 1') == [1, 1]

        #CMS guarantees that we have the frequency at LEAST the size of the increment for the item
        assert client.execute_command('CMS.QUERY sketch1 item1')[0] >= 1

    def test_module_data_type(self):
        # Validate the name of the Module data type.
        client = self.server.get_new_client()
        assert client.execute_command('CMS.INITBYDIM sketch 5 3') == b'OK'
        type_result = client.execute_command('TYPE sketch')
        assert type_result == b"cntmnskch"
        # Validate the name of the Module data type.
        encoding_result = client.execute_command('OBJECT ENCODING sketch')
        assert encoding_result == b"raw"
        

    def test_cms_obj_access(self):
        client = self.server.get_new_client()
        # check count min sketch with basic valkey command
        # cmd touch
        assert client.execute_command('CMS.INITBYDIM sketch1 5 3') == b'OK'
        assert client.execute_command('CMS.INITBYDIM sketch2 10 4') == b'OK'
        
        assert client.execute_command('CMS.INCRBY sketch1 val1 1') == [1]
        assert client.execute_command('CMS.INCRBY sketch2 val2 2') == [2]
        
        assert client.execute_command('TOUCH sketch1 sketch2') == 2
        assert client.execute_command('TOUCH sketch3') == 0
        self.verify_server_key_count(client, 2)
        assert client.execute_command('DBSIZE') == 2
        random_key = client.execute_command('RANDOMKEY')
        assert random_key == b"sketch1" or random_key == b"sketch2"

