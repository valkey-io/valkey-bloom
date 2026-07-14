import time
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkey_bloom_test_case import SkipSeedParameterizationMixin, ValkeyBloomTestCaseBase


class TestTopkBasic(SkipSeedParameterizationMixin, ValkeyBloomTestCaseBase):

    def test_basic(self):
        client = self.server.get_new_client()
        # Validate that the valkey-bloom module is loaded.
        module_list_data = client.execute_command('MODULE LIST')
        module_loaded = any(module[b'name'] == b'bf' for module in module_list_data)
        assert module_loaded
        # Validate that all the TOPK.* commands are supported on the server.
        command_cmd_result = client.execute_command('COMMAND')
        topk_cmds = ["TOPK.RESERVE", "TOPK.ADD", "TOPK.INCRBY", "TOPK.INFO", "TOPK.LIST", "TOPK.COUNT", "TOPK.QUERY"]
        assert all(item in command_cmd_result for item in topk_cmds)
        # Basic create, add, and membership query.
        assert client.execute_command('TOPK.RESERVE tk 3 50 4 0.9 SEED 42') == b'OK'
        assert client.execute_command('TOPK.ADD tk apple banana cherry') == [None, None, None]
        assert client.execute_command('TOPK.QUERY tk apple') == [1]
        assert client.execute_command('TOPK.QUERY tk missing') == [0]

    def test_copy_and_exists_cmd(self):
        client = self.server.get_new_client()
        assert client.execute_command('TOPK.RESERVE tk 3 50 4 0.9 SEED 42') == b'OK'
        client.execute_command('TOPK.INCRBY tk apple 5 banana 3 cherry 1')
        # cmd debug digest
        server_digest = client.execute_command('DEBUG', 'DIGEST')
        assert server_digest != None or 0000000000000000000000000000000000000000
        object_digest = client.execute_command('DEBUG DIGEST-VALUE tk')
        # COPY is a deep copy
        assert client.execute_command('COPY tk tk_copy') == 1
        copied_server_digest = client.execute_command('DEBUG', 'DIGEST')
        assert copied_server_digest != None or 0000000000000000000000000000000000000000
        copied_object_digest = client.execute_command('DEBUG DIGEST-VALUE tk_copy')
        assert client.execute_command('EXISTS tk_copy') == 1
        assert client.execute_command('TOPK.LIST tk') == client.execute_command('TOPK.LIST tk_copy')
        assert client.execute_command('TOPK.LIST tk WITHCOUNT') == client.execute_command('TOPK.LIST tk_copy WITHCOUNT')
        assert server_digest != copied_server_digest
        assert copied_object_digest == object_digest

        for cmd in [
            'TOPK.ADD tk date',
            'TOPK.INCRBY tk date 10',
            'TOPK.ADD tk apple banana',
            'TOPK.INCRBY tk elderberry 8 fig 6',
        ]:
            copy_cmd = cmd.replace('tk ', 'tk_copy ', 1)
            assert client.execute_command(cmd) == client.execute_command(copy_cmd)

        # Final membership, ordering, and per-item counts all still agree.
        assert client.execute_command('TOPK.LIST tk WITHCOUNT') == client.execute_command('TOPK.LIST tk_copy WITHCOUNT')
        for item in ['apple', 'banana', 'cherry', 'date', 'elderberry', 'fig', 'missing']:
            assert client.execute_command(f'TOPK.COUNT tk {item}') == \
                client.execute_command(f'TOPK.COUNT tk_copy {item}')
            assert client.execute_command(f'TOPK.QUERY tk {item}') == \
                client.execute_command(f'TOPK.QUERY tk_copy {item}')

        # The two sketches are independent
        before = client.execute_command('TOPK.LIST tk WITHCOUNT')
        client.execute_command('TOPK.INCRBY tk_copy grape 100')
        assert client.execute_command('TOPK.LIST tk WITHCOUNT') == before
        assert client.execute_command('DEBUG DIGEST-VALUE tk_copy') != \
            client.execute_command('DEBUG DIGEST-VALUE tk')

    def test_module_data_type(self):
        # Validate the name of the Module data type and its encoding.
        client = self.server.get_new_client()
        assert client.execute_command('TOPK.RESERVE tk 3 50 4 0.9') == b'OK'
        assert client.execute_command('TYPE tk') == b"topk-type"
        assert client.execute_command('OBJECT ENCODING tk') == b"raw"

    def test_topk_obj_access(self):
        client = self.server.get_new_client()
        # TopK keys work with generic key commands.
        assert client.execute_command('TOPK.RESERVE key1 3 50 4 0.9') == b'OK'
        assert client.execute_command('TOPK.RESERVE key2 3 50 4 0.9') == b'OK'
        assert client.execute_command('TOUCH key1 key2') == 2
        assert client.execute_command('TOUCH key3') == 0
        self.verify_server_key_count(client, 2)
        assert client.execute_command('DBSIZE') == 2
        random_key = client.execute_command('RANDOMKEY')
        assert random_key == b"key1" or random_key == b"key2"

    def test_topk_transaction(self):
        client = self.server.get_new_client()
        assert client.execute_command('MULTI') == b'OK'
        assert client.execute_command('TOPK.RESERVE M1 3 50 4 0.9') == b'QUEUED'
        assert client.execute_command('TOPK.ADD M1 V1') == b'QUEUED'
        assert client.execute_command('TOPK.QUERY M1 V1') == b'QUEUED'
        assert client.execute_command('DEL M1') == b'QUEUED'
        results = client.execute_command('EXEC')
        # RESERVE -> OK, ADD -> [nil], QUERY -> [1], DEL -> 1.
        assert results == [b'OK', [None], [1], 1]
        self.verify_server_key_count(client, 0)

    def test_topk_lua(self):
        client = self.server.get_new_client()
        load = """
        redis.call('TOPK.RESERVE', 'LUA1', '3', '50', '4', '0.9');
        redis.call('TOPK.ADD', 'LUA1', 'ITEM1');
        redis.call('TOPK.INCRBY', 'LUA1', 'ITEM2', '5');
        """
        client.eval(load, 0)
        assert client.execute_command('TOPK.QUERY LUA1 ITEM1 ITEM2 MISSING') == [1, 1, 0]
        self.verify_server_key_count(client, 1)

    def test_topk_deletes(self):
        client = self.server.get_new_client()
        # delete
        assert client.execute_command('TOPK.RESERVE filter1 3 50 4 0.9') == b'OK'
        self.verify_server_key_count(client, 1)
        assert client.execute_command('DEL filter1') == 1
        self.verify_server_key_count(client, 0)

        # flush
        for i in range(10):
            assert client.execute_command(f'TOPK.RESERVE SAMPLE{i} 3 50 4 0.9') == b'OK'
        self.verify_server_key_count(client, 10)
        assert client.execute_command('FLUSHALL')
        self.verify_server_key_count(client, 0)

        # unlink
        assert client.execute_command('TOPK.RESERVE A 3 50 4 0.9') == b'OK'
        assert client.execute_command('TOPK.RESERVE B 3 50 4 0.9') == b'OK'
        self.verify_server_key_count(client, 2)
        assert client.execute_command('UNLINK A B C') == 2
        self.verify_server_key_count(client, 0)

    def test_topk_expiration(self):
        client = self.server.get_new_client()
        self.verify_server_key_count(client, 0)
        # cmd object idletime
        assert client.execute_command('TOPK.RESERVE TEST_IDLE 3 50 4 0.9') == b'OK'
        self.verify_server_key_count(client, 1)
        time.sleep(1)
        assert client.execute_command('OBJECT IDLETIME TEST_IDLE') > 0
        # cmd ttl, expireat
        assert client.execute_command('TOPK.RESERVE TEST_EXP 3 50 4 0.9') == b'OK'
        assert client.execute_command('TTL TEST_EXP') == -1
        self.verify_server_key_count(client, 2)
        curr_time = int(time.time())
        assert client.execute_command(f'EXPIREAT TEST_EXP {curr_time + 5}') == 1
        wait_for_equal(lambda: client.execute_command('EXISTS TEST_EXP'), 0)
        self.verify_server_key_count(client, 1)
        # cmd persist
        assert client.execute_command('TOPK.RESERVE TEST_PERSIST 3 50 4 0.9') == b'OK'
        assert client.execute_command('TTL TEST_PERSIST') == -1
        assert client.execute_command(f'EXPIREAT TEST_PERSIST {curr_time + 100000}') == 1
        assert client.execute_command('TTL TEST_PERSIST') > 0
        assert client.execute_command('PERSIST TEST_PERSIST') == 1
        assert client.execute_command('TTL TEST_PERSIST') == -1

    def test_topk_add_no_eviction(self):
        assert self.client.execute_command('TOPK.RESERVE tk 3 50 4 0.9 SEED 42') == b'OK'
        assert self.client.execute_command(
            'TOPK.ADD tk apple banana cherry apple banana cherry'
        ) == [None] * 6
        # Repeated adds of an already-tracked item still return nil.
        assert self.client.execute_command('TOPK.ADD tk apple') == [None]

    def test_topk_add_returns_evicted_item(self):
        assert self.client.execute_command('TOPK.RESERVE tk 1 50 4 0.9 SEED 42') == b'OK'
        # apple seats in the only slot (count=1), nothing displaced.
        assert self.client.execute_command('TOPK.ADD tk apple') == [None]
        # banana reaches count=2 within the call, displacing apple.
        assert self.client.execute_command('TOPK.ADD tk banana banana') == [None, b'apple']

    def test_topk_add_eviction_count(self):
        assert self.client.execute_command('TOPK.RESERVE tk 2 50 4 0.9 SEED 42') == b'OK'
        self.client.execute_command('TOPK.ADD tk apple apple banana')
        result = self.client.execute_command('TOPK.ADD tk cherry cherry')
        assert len(result) == 2
        evictions = [r for r in result if r is not None]
        assert len(evictions) == 1

    def test_topk_incrby_accumulates_count(self):
        # Repeated increments of the same item accumulate. Verify the summed
        # count through TOPK.LIST WITHCOUNT.
        assert self.client.execute_command('TOPK.RESERVE tk 3 50 4 0.9 SEED 42') == b'OK'
        self.client.execute_command('TOPK.INCRBY tk apple 4')
        self.client.execute_command('TOPK.INCRBY tk apple 6')
        listed = self.client.execute_command('TOPK.LIST tk WITHCOUNT')
        it = iter(listed)
        counts = dict(zip(it, it))
        assert counts[b'apple'] == 10

    def test_topk_add_low_count_does_not_displace_min(self):
        # Once the top-k is full, an item whose count never beats the current
        # minimum tracked count is not admitted and displaces nothing.
        assert self.client.execute_command('TOPK.RESERVE tk 2 50 4 0.9 SEED 42') == b'OK'
        self.client.execute_command('TOPK.INCRBY tk hot 50 warm 30')
        assert self.client.execute_command('TOPK.ADD tk cold') == [None]
        listed = self.client.execute_command('TOPK.LIST tk')
        assert b'cold' not in listed

    def test_topk_count_never_exceeds_true_count(self):
        # Estimates never exceed true counts. Use a small, narrow sketch so
        # collisions are likely, then check the invariant holds.
        assert self.client.execute_command('TOPK.RESERVE tk_inv 5 4 2 0.9 SEED 42') == b'OK'
        true_counts = {f'item-{i}': i + 1 for i in range(50)}
        for item, count in true_counts.items():
            self.client.execute_command(f'TOPK.INCRBY tk_inv {item} {count}')
        items = list(true_counts.keys())
        estimates = self.client.execute_command('TOPK.COUNT tk_inv ' + ' '.join(items))
        for item, estimate in zip(items, estimates):
            assert estimate <= true_counts[item], f'{item}: {estimate} > {true_counts[item]}'

    def test_memory_usage_cmd(self):
        assert self.client.execute_command('TOPK.RESERVE tk 5 50 4 0.9 SEED 42') == b'OK'
        self.client.execute_command('TOPK.ADD tk apple banana cherry')
        info = self.client.execute_command('TOPK.INFO tk')
        info_size = dict(zip(info[::2], info[1::2]))[b'Size']
        assert self.client.execute_command('MEMORY USAGE tk') >= info_size and info_size > 0

    def test_too_large_topk_obj(self):
        obj_exceeds_size_err = "operation exceeds topk object memory limit"
        # Normal sizes are within the default 128MB limit.
        assert self.client.execute_command('TOPK.RESERVE tk_ok 5 200 5 0.9') == b'OK'

        # A huge width*depth is rejected before allocating.
        self.verify_error_response(self.client, 'TOPK.RESERVE tk_big 5 4000000000 1000 0.9', obj_exceeds_size_err)
        assert self.client.execute_command('EXISTS tk_big') == 0

        # A huge k is also rejected (the priority queue reserves capacity k).
        self.verify_error_response(self.client, 'TOPK.RESERVE tk_big_k 4000000000 50 4 0.9', obj_exceeds_size_err)
        assert self.client.execute_command('EXISTS tk_big_k') == 0

        # Lowering the limit rejects a sketch that was previously allowed.
        self.client.config_set('bf.topk-memory-usage-limit', '10000')
        self.verify_error_response(self.client, 'TOPK.RESERVE tk_small 5 200 5 0.9', obj_exceeds_size_err)

