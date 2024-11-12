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
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()

        # verify restore results
        curr_item_count_2 = client.info_obj().num_keys()
        assert curr_item_count_2 == curr_item_count_1
        bf_exists_result_2 = client.execute_command('BF.EXISTS testSave item')
        assert bf_exists_result_2 == 1
        bf_info_result_2 = client.execute_command('BF.INFO testSave')
        assert bf_info_result_2 == bf_info_result_1
        client.execute_command('DEL testSave')

    def test_aofrewrite_bloomfilter_metrics(self):
        self.client.execute_command('BF.RESERVE key1 0.001 7000')
        # We use a number greater than 7000 in order to have a buffer for any false positives
        variables = [f"key{i+1}" for i in range(7500)]

        # Get original size to compare against size after scaled
        info_obj = self.client.execute_command('BF.INFO key1')
        # Add keys until bloomfilter will scale out
        for var in variables:
            self.client.execute_command(f'BF.ADD key1 {var}')

        # save aof, restart sever
        self.client.bgrewriteaof()
        self.server.wait_for_action_done(ValkeyAction.AOF_REWRITE)
        # restart server
        time.sleep(1)
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        
        # Check info for scaled bloomfilter matches metrics data for bloomfilter
        new_info_obj = self.client.execute_command(f'BF.INFO key1')
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"),  new_info_obj[3], 1, 2)

        # Check bloomfilter size has increased
        assert new_info_obj[3] > info_obj[3]

        # Delete the scaled bloomfilter to check both filters are deleted and metrics stats are set accordingly
        self.client.execute_command('DEL key1')
        self.verify_bloom_metrics(self.client.execute_command("INFO bf"), 0, 0, 0)