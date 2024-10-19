import time
from valkeytests.valkey_test_case import ValkeyAction
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytests.conftest import resource_port_tracker

class TestBloomAofRewrite(ValkeyBloomTestCaseBase):
    
    def get_custom_args(self):
        # test aof rewrite should avoid bloom filter override as rdb. use aof
        args = super().get_custom_args()
        args.update({'aof-use-rdb-preamble': 'no', 'appendonly': 'yes', 'appenddirname': 'aof-{}'.format(self.port)})
        return args

    def test_basic_aofrewrite_and_restore(self):
        client = self.server.get_new_client()
        bf_add_result_1 = client.execute_command('BF.ADD testSave item')
        assert bf_add_result_1 == 1
        bf_exists_result_1 = client.execute_command('BF.EXISTS testSave item')
        assert bf_exists_result_1 == 1
        bf_info_result_1 = client.execute_command('BF.INFO testSave')
        assert(len(bf_info_result_1)) != 0
        curr_item_count_1 = client.info_obj().num_keys()
        
        # save aof, restart sever
        client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        # Keep the server running for 1 second more to have a larger uptime.
        time.sleep(1)
        uptime_in_sec_1 = self.client.info_obj().uptime_in_secs()
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        uptime_in_sec_2 = self.client.info_obj().uptime_in_secs()
        assert self.server.is_alive()
        assert uptime_in_sec_1 > uptime_in_sec_2

        # verify restore results
        curr_item_count_2 = client.info_obj().num_keys()
        assert curr_item_count_2 == curr_item_count_1
        bf_exists_result_2 = client.execute_command('BF.EXISTS testSave item')
        assert bf_exists_result_2 == 1
        bf_info_result_2 = client.execute_command('BF.INFO testSave')
        assert bf_info_result_2 == bf_info_result_1
