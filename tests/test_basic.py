import time
import pytest
from util.waiters import *
from valkey import ResponseError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytests.conftest import resource_port_tracker
import logging
import os

class TestBloomBasic(ValkeyBloomTestCaseBase):

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

    def test_bloom_modification(self):
        client = self.server.get_new_client()
        # check bloom filter with basic valkey command
        # cmd touch
        assert client.execute_command('BF.ADD key1 val1') == 1
        assert client.execute_command('BF.ADD key2 val2') == 1
        assert client.execute_command('TOUCH key1 key2') == 2
        assert client.execute_command('TOUCH key3') == 0
        self.verify_key_number(client, 2)

    def test_bloom_transaction(self):
        client = self.server.get_new_client()
        # cmd multi, exec
        assert client.execute_command('MULTI') == b'OK'
        assert client.execute_command('BF.ADD M1 V1') == b'QUEUED'
        assert client.execute_command('BF.ADD M2 V2') == b'QUEUED'
        assert client.execute_command('BF.EXISTS M1 V1') == b'QUEUED'
        assert client.execute_command('DEL M1') == b'QUEUED'
        assert client.execute_command('BF.EXISTS M1 V1') == b'QUEUED'
        assert client.execute_command('EXEC') == [1, 1, 1, 1, 0]
        self.verify_bloom_filter_existence(client, 'M2', 'V2')
        self.verify_bloom_filter_existence(client, 'M1', 'V1', should_exist=False)
        self.verify_key_number(client, 1)

    def test_bloom_lua(self):
        client = self.server.get_new_client()
        # lua
        load_filter = """
        redis.call('BF.ADD', 'LUA1', 'ITEM1');
        redis.call('BF.ADD', 'LUA2', 'ITEM2');
        redis.call('BF.MADD', 'LUA2', 'ITEM3', 'ITEM4', 'ITEM5');
        """
        client.eval(load_filter, 0)
        assert client.execute_command('BF.MEXISTS LUA2 ITEM1 ITEM3 ITEM4') == [0, 1, 1]
        self.verify_key_number(client, 2)

    def test_bloom_deletes(self):
        client = self.server.get_new_client()
        # delete
        assert client.execute_command('BF.ADD filter1 item1') == 1
        self.verify_bloom_filter_existence(client, 'filter1', 'item1')
        self.verify_key_number(client, 1)
        assert client.execute_command('DEL filter1') == 1
        self.verify_bloom_filter_existence(client, 'filter1', 'item1', should_exist=False)
        self.verify_key_number(client, 0)

        # flush
        self.insert_bloom_filter(client, number_of_bf=10)
        self.verify_key_number(client, 10)
        assert client.execute_command('FLUSHALL')
        self.verify_key_number(client, 0)

        # unlink
        assert client.execute_command('BF.ADD A ITEMA') == 1
        assert client.execute_command('BF.ADD B ITEMB') == 1
        self.verify_bloom_filter_existence(client, 'A', 'ITEMA')
        self.verify_bloom_filter_existence(client, 'B', 'ITEMB')
        self.verify_bloom_filter_existence(client, 'C', 'ITEMC', should_exist=False)
        self.verify_key_number(client, 2)
        assert client.execute_command('UNLINK A B C') == 2
        assert client.execute_command('BF.MEXISTS A ITEMA ITEMB') == [0, 0]
        self.verify_bloom_filter_existence(client, 'A', 'ITEMA', should_exist=False)
        self.verify_bloom_filter_existence(client, 'B', 'ITEMB', should_exist=False)
        self.verify_key_number(client, 0)

    def test_bloom_expiration(self):
        client = self.server.get_new_client()
        # expiration
        # cmd object idletime
        self.verify_key_number(client, 0)
        assert client.execute_command('BF.ADD TEST_IDLE val3') == 1
        self.verify_bloom_filter_existence(client, 'TEST_IDLE', 'val3')
        self.verify_key_number(client, 1)
        time.sleep(1)
        assert client.execute_command('OBJECT IDLETIME test_idle') == None
        assert client.execute_command('OBJECT IDLETIME TEST_IDLE') > 0
        # cmd ttl, expireat
        assert client.execute_command('BF.ADD TEST_EXP ITEM') == 1
        assert client.execute_command('TTL TEST_EXP') == -1
        self.verify_bloom_filter_existence(client, 'TEST_EXP', 'ITEM')
        self.verify_key_number(client, 2)
        curr_time = int(time.time())
        assert client.execute_command(f'EXPIREAT TEST_EXP {curr_time + 5}') == 1
        wait_for_equal(lambda: client.execute_command('BF.EXISTS TEST_EXP ITEM'), 0)
        self.verify_key_number(client, 1)
        # cmd persist
        assert client.execute_command('BF.ADD TEST_PERSIST ITEM') == 1
        assert client.execute_command('TTL TEST_PERSIST') == -1
        self.verify_bloom_filter_existence(client, 'TEST_PERSIST', 'ITEM')
        self.verify_key_number(client, 2)
        assert client.execute_command(f'EXPIREAT TEST_PERSIST {curr_time + 100000}') == 1
        assert client.execute_command('TTL TEST_PERSIST') > 0
        assert client.execute_command('PERSIST TEST_PERSIST') == 1
        assert client.execute_command('TTL TEST_PERSIST') == -1
