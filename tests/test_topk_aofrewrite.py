import time
from valkeytestframework.util.waiters import wait_for_equal
from valkeytestframework.valkey_test_case import ValkeyAction
from valkey_bloom_test_case import SkipSeedParameterizationMixin, ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401

class TestTopkAofRewrite(SkipSeedParameterizationMixin, ValkeyBloomTestCaseBase):

    def test_basic_aofrewrite_and_restore(self):
        client = self.server.get_new_client()
        # Disable the RDB preamble so the rewrite emits TOPK.LOAD commands.
        client.config_set('aof-use-rdb-preamble', 'no')
        # Enable AOF before adding data
        client.config_set('appendonly', 'yes')
        # Wait for any initial AOF rewrite to complete
        wait_for_equal(lambda: client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=30)

        assert client.execute_command('TOPK.RESERVE testSave 5 50 4 0.9 SEED 42') == b'OK'
        # Fill past k (5) so an item is evicted, exercising the priority queue.
        client.execute_command('TOPK.ADD testSave apple banana cherry date elderberry fig')
        client.execute_command('TOPK.INCRBY testSave apple 5 banana 3')
        info_before = client.execute_command('TOPK.INFO testSave')
        list_before = client.execute_command('TOPK.LIST testSave WITHCOUNT')
        assert len(info_before) != 0
        item_count_before = self.server.num_keys(client=client)

        # cmd debug digest
        server_digest = client.execute_command('DEBUG', 'DIGEST')
        assert server_digest != None or 0000000000000000000000000000000000000000
        object_digest = client.execute_command('DEBUG DIGEST-VALUE testSave')

        # save aof, restart server
        client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        # Keep the server running for 1 second more to have a larger uptime.
        time.sleep(1)
        # Add appendonly to server args so it loads AOF on restart
        self.server.args['appendonly'] = 'yes'
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()

        restored_server_digest = client.execute_command('DEBUG', 'DIGEST')
        restored_object_digest = client.execute_command('DEBUG DIGEST-VALUE testSave')
        assert restored_server_digest == server_digest
        assert restored_object_digest == object_digest

        # verify restore results
        item_count_after = self.server.num_keys(client=client)
        assert item_count_after == item_count_before
        assert client.execute_command('TOPK.INFO testSave') == info_before
        assert client.execute_command('TOPK.LIST testSave WITHCOUNT') == list_before
        client.execute_command('DEL testSave')

    def test_aofrewrite_topk_metrics(self):
        # Disable the RDB preamble so the rewrite emits TOPK.LOAD commands.
        self.client.config_set('aof-use-rdb-preamble', 'no')
        # Enable AOF before adding data
        self.client.config_set('appendonly', 'yes')
        # Wait for any initial AOF rewrite to complete
        wait_for_equal(lambda: self.client.info('persistence')['aof_rewrite_in_progress'], 0, timeout=30)

        # Create a TopK (k=5) and apply ADD (+3) and INCRBY (+9) -> 12 items.
        assert self.client.execute_command('TOPK.RESERVE key1 5 50 4 0.9 SEED 42') == b'OK'
        self.client.execute_command('TOPK.ADD key1 apple banana cherry')
        self.client.execute_command('TOPK.INCRBY key1 apple 5 banana 4')
        key_size = self.client.execute_command('TOPK.INFO key1')[9]
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), key_size, 1, 12, 5)

        # cmd debug digest
        server_digest = self.client.execute_command('DEBUG', 'DIGEST')
        assert server_digest != None or 0000000000000000000000000000000000000000
        object_digest = self.client.execute_command('DEBUG DIGEST-VALUE key1')

        # save aof, restart server
        self.client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        # Add appendonly to server args so it loads AOF on restart
        self.server.args['appendonly'] = 'yes'
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()

        restored_server_digest = self.client.execute_command('DEBUG', 'DIGEST')
        restored_object_digest = self.client.execute_command('DEBUG DIGEST-VALUE key1')
        assert restored_server_digest == server_digest
        assert restored_object_digest == object_digest

        # Metrics for the restored object match what they were before the rewrite.
        restored_key_size = self.client.execute_command('TOPK.INFO key1')[9]
        assert restored_key_size == key_size
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), key_size, 1, 12, 5)

        # Deleting the key returns all gauges to 0.
        assert self.client.execute_command('DEL key1') == 1
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), 0, 0, 0, 0)
