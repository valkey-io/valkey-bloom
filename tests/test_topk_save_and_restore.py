from valkey import ResponseError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util.waiters import *

class TestTopkSaveRestore(ValkeyBloomTestCaseBase):

    def test_basic_save_and_restore(self):
        client = self.server.get_new_client()
        assert client.execute_command('TOPK.RESERVE testSave 5 50 4 0.9 SEED 42') == b'OK'
        # First round: 3 items < k (5), so the priority queue is not full.
        client.execute_command('TOPK.ADD testSave apple banana cherry')
        client.execute_command('TOPK.INCRBY testSave apple 5 banana 3')
        info_before = client.execute_command('TOPK.INFO testSave')
        list_before = client.execute_command('TOPK.LIST testSave WITHCOUNT')
        assert len(info_before) != 0
        item_count_before = self.server.num_keys(client=client)
        # cmd debug digest
        server_digest = client.execute_command('DEBUG', 'DIGEST')
        assert server_digest != None or 0000000000000000000000000000000000000000
        object_digest = client.execute_command('DEBUG DIGEST-VALUE testSave')

        # save rdb, restart server.
        client.execute_command('BGSAVE')
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)

        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)
        restored_server_digest = client.execute_command('DEBUG', 'DIGEST')
        restored_object_digest = client.execute_command('DEBUG DIGEST-VALUE testSave')
        assert restored_server_digest == server_digest
        assert restored_object_digest == object_digest
        self.server.verify_string_in_logfile("Loading RDB produced by Valkey")
        self.server.verify_string_in_logfile("Done loading RDB, keys loaded: 1, keys expired: 0")

        # verify restore results
        item_count_after = self.server.num_keys(client=client)
        assert item_count_after == item_count_before
        assert client.execute_command('TOPK.INFO testSave') == info_before
        assert client.execute_command('TOPK.LIST testSave WITHCOUNT') == list_before

        # Second round: fill past k (5) so an item is evicted, then round-trip.
        client.execute_command('TOPK.ADD testSave date elderberry fig grape')
        client.execute_command('TOPK.INCRBY testSave date 4 elderberry 6')
        info_before = client.execute_command('TOPK.INFO testSave')
        list_before = client.execute_command('TOPK.LIST testSave WITHCOUNT')
        server_digest = client.execute_command('DEBUG', 'DIGEST')
        object_digest = client.execute_command('DEBUG DIGEST-VALUE testSave')

        client.execute_command('BGSAVE')
        self.server.wait_for_save_done()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)

        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)
        assert client.execute_command('DEBUG', 'DIGEST') == server_digest
        assert client.execute_command('DEBUG DIGEST-VALUE testSave') == object_digest
        assert client.execute_command('TOPK.INFO testSave') == info_before
        assert client.execute_command('TOPK.LIST testSave WITHCOUNT') == list_before

    def test_basic_save_many(self):
        client = self.server.get_new_client()
        count = 500
        configs = [
            (5, 50, 4, 0.9, 42),
            (10, 200, 5, 0.5, 7),
            (3, 16, 6, 0.95, 0),
            (8, 64, 4, 0.99, 123456789),
        ]
        for i in range(0, count):
            name = str(i) + "key"
            k, width, depth, decay, seed = configs[i % len(configs)]
            assert client.execute_command(
                f'TOPK.RESERVE {name} {k} {width} {depth} {decay} SEED {seed}'
            ) == b'OK'
            client.execute_command(f'TOPK.ADD {name} item{i}')

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
        assert client.execute_command('TOPK.QUERY 0key item0') == [1]
        assert client.execute_command('TOPK.QUERY 499key item499') == [1]

    def test_rdb_restore_non_topk_compatibility(self):
        # Create an rdb on a module enabled server
        topk_client = self.server.get_new_client()
        topk_client.execute_command("TOPK.RESERVE key 5")
        topk_client.execute_command("TOPK.ADD key item")
        topk_client.execute_command("set string val")
        assert self.server.num_keys(client=topk_client) == 2
        assert topk_client.execute_command("del key") == 1
        assert self.server.num_keys(client=topk_client) == 1
        assert topk_client.get("string") == b"val"
        topk_client.execute_command('BGSAVE')
        self.server.wait_for_save_done()
        rdb_file = self.server.args["dbfilename"]

        # Create a server without the module
        new_server, new_client = self.create_server(testdir=self.testdir, server_path=self.server_path, args={"dbfilename": rdb_file})
        assert new_server.is_alive()
        wait_for_equal(lambda: new_server.is_rdb_done_loading(), True)

        # Verification
        assert new_client.execute_command("info bf") == b''
        try:
            new_client.execute_command("topk.reserve test 5")
            assert False
        except ResponseError as e:
            assert "unknown command" in str(e)

        assert self.server.num_keys(client=new_client) == 1
        assert new_client.get("string") == b"val"
