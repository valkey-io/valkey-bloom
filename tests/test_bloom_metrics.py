import pytest, time
import os
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytests.conftest import resource_port_tracker
from util.waiters import *

DEFAULT_BLOOM_FILTER_SIZE = 179960
DEFAULT_BLOOM_FILTER_CAPACITY = 100000
class TestBloomMetrics(ValkeyBloomTestCaseBase):

    def test_basic_command_metrics(self):
        # Check that bloom metrics stats start at 0
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), 0, 0, 0, 0, 0)
        self.verify_bloom_metrics(self.client.execute_command("INFO Modules"), 0, 0, 0, 0, 0)

        # Create a default bloom filter and check its metrics values are correct
        assert(self.client.execute_command('BF.ADD key item') == 1)
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), DEFAULT_BLOOM_FILTER_SIZE, 1, 1, 1, DEFAULT_BLOOM_FILTER_CAPACITY)
        self.verify_bloom_metrics(self.client.execute_command("INFO Modules"), DEFAULT_BLOOM_FILTER_SIZE, 1, 1, 1, DEFAULT_BLOOM_FILTER_CAPACITY)

        # Check that other commands don't influence metrics
        assert(self.client.execute_command('BF.EXISTS key item') == 1)
        assert(self.client.execute_command('BF.ADD key item2') == 1)
        assert(self.client.execute_command('BF.MADD key item3 item4')== [1, 1])
        assert(self.client.execute_command('BF.MEXISTS key item3 item5')== [1, 0])
        assert(self.client.execute_command('BF.CARD key') == 4)
        self.client.execute_command("BF.INFO key")
        assert(self.client.execute_command('BF.INSERT key ITEMS item5 item6')== [1, 1])
        
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), DEFAULT_BLOOM_FILTER_SIZE, 1, 1, 6, DEFAULT_BLOOM_FILTER_CAPACITY)
        self.verify_bloom_metrics(self.client.execute_command("INFO Modules"), DEFAULT_BLOOM_FILTER_SIZE, 1, 1, 6, DEFAULT_BLOOM_FILTER_CAPACITY)

        # Create a new default bloom filter and check metrics again 
        assert(self.client.execute_command('BF.ADD key2 item') == 1)
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), DEFAULT_BLOOM_FILTER_SIZE*2, 2, 2, 7, DEFAULT_BLOOM_FILTER_CAPACITY * 2)
        self.verify_bloom_metrics(self.client.execute_command("INFO Modules"), DEFAULT_BLOOM_FILTER_SIZE*2, 2, 2, 7, DEFAULT_BLOOM_FILTER_CAPACITY * 2)

        # Create a non default filter with BF.RESERVE and check its metrics are correct
        assert(self.client.execute_command('BF.RESERVE key3 0.001 2917251') == b'OK')
        info_obj = self.client.execute_command('BF.INFO key3')

        # We want to check the size of the newly created bloom filter but metrics contains the size of all bloomfilters so we must minus the 
        # two default bloomfilters we already created
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), info_obj[3] + DEFAULT_BLOOM_FILTER_SIZE * 2, 3, 3, 7, DEFAULT_BLOOM_FILTER_CAPACITY * 2 + 2917251)
        self.verify_bloom_metrics(self.client.execute_command("INFO Modules"), info_obj[3] + DEFAULT_BLOOM_FILTER_SIZE * 2, 3, 3, 7, DEFAULT_BLOOM_FILTER_CAPACITY * 2 + 2917251)

        # Delete a non default key and make sure the metrics stats are still correct
        self.client.execute_command('DEL key3')
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), DEFAULT_BLOOM_FILTER_SIZE * 2, 2, 2, 7, DEFAULT_BLOOM_FILTER_CAPACITY * 2)
        self.verify_bloom_metrics(self.client.execute_command("INFO Modules"), DEFAULT_BLOOM_FILTER_SIZE * 2, 2, 2, 7, DEFAULT_BLOOM_FILTER_CAPACITY * 2)

        # Create a default filter with BF.INSERT and check its metrics are correct
        assert(self.client.execute_command('BF.INSERT key4 ITEMS item1 item2') == [1, 1])
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), DEFAULT_BLOOM_FILTER_SIZE * 3, 3, 3, 9, DEFAULT_BLOOM_FILTER_CAPACITY * 3)
        self.verify_bloom_metrics(self.client.execute_command("INFO Modules"), DEFAULT_BLOOM_FILTER_SIZE * 3, 3, 3, 9, DEFAULT_BLOOM_FILTER_CAPACITY * 3)

        # Delete a default key and make sure the metrics are still correct
        self.client.execute_command('UNLINK key')
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), DEFAULT_BLOOM_FILTER_SIZE * 2, 2, 2, 3, DEFAULT_BLOOM_FILTER_CAPACITY * 2)
        self.verify_bloom_metrics(self.client.execute_command("INFO Modules"), DEFAULT_BLOOM_FILTER_SIZE * 2, 2, 2, 3, DEFAULT_BLOOM_FILTER_CAPACITY * 2)

        # Create a key then cause it to expire and check if metrics are updated correctly
        assert self.client.execute_command('BF.ADD TEST_EXP ITEM') == 1
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), DEFAULT_BLOOM_FILTER_SIZE * 3, 3, 3, 4, DEFAULT_BLOOM_FILTER_CAPACITY * 3)
        self.verify_bloom_metrics(self.client.execute_command("INFO Modules"), DEFAULT_BLOOM_FILTER_SIZE * 3, 3, 3, 4, DEFAULT_BLOOM_FILTER_CAPACITY * 3)
        assert self.client.execute_command('TTL TEST_EXP') == -1
        self.verify_bloom_filter_item_existence(self.client, 'TEST_EXP', 'ITEM')
        curr_time = int(time.time())
        assert self.client.execute_command(f'EXPIREAT TEST_EXP {curr_time + 5}') == 1
        wait_for_equal(lambda: self.client.execute_command('BF.EXISTS TEST_EXP ITEM'), 0)
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), DEFAULT_BLOOM_FILTER_SIZE * 2, 2, 2, 3, DEFAULT_BLOOM_FILTER_CAPACITY * 2)
        self.verify_bloom_metrics(self.client.execute_command("INFO Modules"), DEFAULT_BLOOM_FILTER_SIZE * 2, 2, 2, 3, DEFAULT_BLOOM_FILTER_CAPACITY * 2)

        # Flush database so all keys should now be gone and metrics should all be at 0
        self.client.execute_command('FLUSHDB')
        wait_for_equal(lambda: self.client.execute_command('BF.EXISTS key2 item'), 0)
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), 0, 0, 0, 0, 0)
        self.verify_bloom_metrics(self.client.execute_command("INFO Modules"), 0, 0, 0, 0, 0)

    def test_scaled_bloomfilter_metrics(self):
        self.client.execute_command('BF.RESERVE key1 0.001 7000')
        # We use a number greater than 7000 in order to have a buffer for any false positives
        variables = [f"key{i+1}" for i in range(7500)]

        # Get original size to compare against size after scaled
        info_obj = self.client.execute_command('BF.INFO key1')
        # Add keys until bloomfilter will scale out
        for var in variables:
            self.client.execute_command(f'BF.ADD key1 {var}')

        # Check info for scaled bloomfilter matches metrics data for bloomfilter
        new_info_obj = self.client.execute_command(f'BF.INFO key1')
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"),  new_info_obj[3], 1, 2, 7500, 21000)

        # Check bloomfilter size has increased
        assert new_info_obj[3] > info_obj[3]

        # Delete the scaled bloomfilter to check both filters are deleted and metrics stats are set accordingly
        self.client.execute_command('DEL key1')
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), 0, 0, 0, 0, 0)


    def test_copy_metrics(self):
        # Create a bloomfilter and copy it
        assert(self.client.execute_command('BF.ADD key{123} item') == 1)
        assert(self.client.execute_command('COPY key{123} copiedkey{123}') == 1)
    
        # Verify that the metrics were updated correctly after copying
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), DEFAULT_BLOOM_FILTER_SIZE * 2, 2, 2, 2, DEFAULT_BLOOM_FILTER_CAPACITY * 2)

        # Perform a FLUSHALL which should set all metrics data to 0
        self.client.execute_command('FLUSHALL')
        wait_for_equal(lambda: self.client.execute_command('BF.EXISTS key{123} item'), 0)
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), 0, 0, 0, 0, 0)


    def test_save_and_restore_metrics(self):
        # Create default bloom filter
        assert(self.client.execute_command('BF.ADD testSave item') == 1)

        # Create scaled bloom filter
        self.client.execute_command('BF.RESERVE key1 0.001 7000')
        variables = [f"key{i+1}" for i in range(7500)]
        for var in variables:
            self.client.execute_command(f'BF.ADD key1 {var}')
        
        # Get info and metrics stats of bloomfilter before rdb load
        original_info_obj = self.client.execute_command('BF.INFO key1')

        self.client.bgsave()
        self.server.wait_for_save_done()

        # Restart, and verify metrics are correct
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)

        # Compare original and loaded scaled bloomfilter infos
        new_client = self.server.get_new_client()
        restored_info_obj = new_client.execute_command('BF.INFO key1')
        for i in range(1, len(original_info_obj), 2):
            assert original_info_obj[i] == restored_info_obj[i]

        self.verify_bloom_metrics(new_client.execute_command("INFO bf"), original_info_obj[3] + DEFAULT_BLOOM_FILTER_SIZE, 2, 3, 7501, 21000 + DEFAULT_BLOOM_FILTER_CAPACITY)
