from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util.waiters import *

class TestTopkSaveRestore(ValkeyBloomTestCaseBase):

    def test_basic_save_and_restore(self):
        client = self.server.get_new_client()
        assert client.execute_command('TOPK.RESERVE testSave 5 50 4 0.9 SEED 42') == b'OK'
        client.execute_command('TOPK.ADD testSave apple banana cherry')
        client.execute_command('TOPK.INCRBY testSave apple 5 banana 3')
        list_before = client.execute_command('TOPK.LIST testSave WITHCOUNT')
        item_count_before = self.server.num_keys(client=client)
        # num_items is persisted in the RDB separately from the sketch
        assert client.execute_command('TOPK.INFO testSave TOTALITEMSADDED') == 11
        size_before = client.execute_command('TOPK.INFO testSave SIZE')
        assert size_before > 0

        # save rdb, restart server.
        client.execute_command('BGSAVE')
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)

        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)
        self.server.verify_string_in_logfile("Loading RDB produced by Valkey")
        self.server.verify_string_in_logfile("Done loading RDB, keys loaded: 1, keys expired: 0")

        # verify restore results
        item_count_after = self.server.num_keys(client=client)
        assert item_count_after == item_count_before
        assert client.execute_command('TOPK.LIST testSave WITHCOUNT') == list_before
        assert client.execute_command('TOPK.INFO testSave K') == 5
        assert client.execute_command('TOPK.INFO testSave WIDTH') == 50
        assert client.execute_command('TOPK.INFO testSave DEPTH') == 4
        assert client.execute_command('TOPK.INFO testSave DECAY') == b'0.9'
        assert client.execute_command('TOPK.INFO testSave TOTALITEMSADDED') == 11
        # size may be slightly smaller after restore, as RDB reload rebuilds the sketch
        assert 0 < client.execute_command('TOPK.INFO testSave SIZE') <= size_before

    def test_basic_save_many(self):
        client = self.server.get_new_client()
        count = 500
        for i in range(0, count):
            name = str(i) + "key"
            assert client.execute_command('TOPK.RESERVE ' + name + ' 5 50 4 0.9 SEED 42') == b'OK'
            client.execute_command('TOPK.ADD ' + name + ' item')

        item_count_before = self.server.num_keys(client=client)
        assert item_count_before == count
        # save rdb, restart server
        client.execute_command('BGSAVE')
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)

        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)
        self.server.verify_string_in_logfile("Loading RDB produced by Valkey")
        self.server.verify_string_in_logfile("Done loading RDB, keys loaded: 500, keys expired: 0")

        # verify all keys survived and remain queryable
        assert self.server.num_keys(client=client) == count
        assert client.execute_command('TOPK.QUERY 0key item') == [1]
