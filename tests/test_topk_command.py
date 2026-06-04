from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401

class TestTopkCommand(ValkeyBloomTestCaseBase):

    def test_topk_command_arity(self):
        self.verify_command_arity('TOPK.RESERVE', -1)
        self.verify_command_arity('TOPK.ADD', -1)
        self.verify_command_arity('TOPK.INCRBY', -1)
        self.verify_command_arity('TOPK.INFO', -1)
        self.verify_command_arity('TOPK.LIST', -1)

    def test_topk_command_error(self):
        # test set up
        assert self.client.execute_command('TOPK.RESERVE dup 5') == b'OK'
        assert self.client.execute_command('SET strkey hello') == b'OK'
        basic_error_test_cases = [
            # ---- TOPK.RESERVE ----
            # re-reserving an existing key: params are immutable.
            ('TOPK.RESERVE dup 5', 'BUSYKEY Target key name already exists.'),
            # wrong type
            ('TOPK.RESERVE strkey 5', 'WRONGTYPE Operation against a key holding the wrong kind of value'),
            # k must parse as u32 and be > 0.
            ('TOPK.RESERVE k1 abc', 'bad topk'),
            ('TOPK.RESERVE k1 -1', 'bad topk'),
            ('TOPK.RESERVE k1 0', '(topk should be larger than 0)'),
            # width must parse as u32 and be > 0.
            ('TOPK.RESERVE k1 5 abc 4 0.9', 'bad width'),
            ('TOPK.RESERVE k1 5 0 4 0.9', '(width should be larger than 0)'),
            # depth must parse as u32 and be > 0.
            ('TOPK.RESERVE k1 5 50 abc 0.9', 'bad depth'),
            ('TOPK.RESERVE k1 5 50 0 0.9', '(depth should be larger than 0)'),
            # decay must parse as f64 and lie in (0, 1).
            ('TOPK.RESERVE k1 5 50 4 abc', 'bad decay'),
            ('TOPK.RESERVE k1 5 50 4 0', '(0 < decay < 1)'),
            ('TOPK.RESERVE k1 5 50 4 1', '(0 < decay < 1)'),
            ('TOPK.RESERVE k1 5 50 4 1.5', '(0 < decay < 1)'),
            # SEED token in the trailing position must literally match.
            ('TOPK.RESERVE k1 5 50 4 0.9 NOTSEED 42', 'ERROR'),
            # seed value must parse as u64.
            ('TOPK.RESERVE k1 5 SEED abc', 'invalid seed'),
            ('TOPK.RESERVE k1 5 SEED -1', 'invalid seed'),
            ('TOPK.RESERVE k1 5 50 4 0.9 SEED abc', 'invalid seed'),
            # valid arity (5) but token at position 3 is not SEED.
            ('TOPK.RESERVE key 5 50 4', 'ERROR'),
            ('TOPK.RESERVE key 5 NOTSEED 42', 'ERROR'),
            # wrong number of arguments (valid arities are 3, 5, 6, 8).
            ('TOPK.RESERVE', "wrong number of arguments for 'TOPK.RESERVE' command"),
            ('TOPK.RESERVE key', "wrong number of arguments for 'TOPK.RESERVE' command"),
            ('TOPK.RESERVE key 5 50', "wrong number of arguments for 'TOPK.RESERVE' command"),
            ('TOPK.RESERVE key 5 50 4 0.9 SEED', "wrong number of arguments for 'TOPK.RESERVE' command"),
            ('TOPK.RESERVE key 5 50 4 0.9 SEED 42 extra', "wrong number of arguments for 'TOPK.RESERVE' command"),
            # SEED both before and after the sketch params is ambiguous.
            ('TOPK.RESERVE key 5 SEED 1 SEED 2', "wrong number of arguments for 'TOPK.RESERVE' command"),
            # key must already be reserved; TOPK.ADD does not auto-create.
            ('TOPK.ADD missing apple', 'TopK: key does not exist'),
            # wrong type
            ('TOPK.ADD strkey apple', 'WRONGTYPE Operation against a key holding the wrong kind of value'),
            # wrong number of arguments
            ('TOPK.ADD', "wrong number of arguments for 'TOPK.ADD' command"),
            ('TOPK.ADD tk', "wrong number of arguments for 'TOPK.ADD' command"),
            # key must already be reserved
            ('TOPK.INCRBY missing apple 1', 'TopK: key does not exist'),
            # wrong type
            ('TOPK.INCRBY strkey apple 1', 'WRONGTYPE Operation against a key holding the wrong kind of value'),
            # increment must parse as u64 and be > 0.
            ('TOPK.INCRBY dup apple abc', 'bad increment'),
            ('TOPK.INCRBY dup apple -1', 'bad increment'),
            ('TOPK.INCRBY dup apple 0', 'bad increment'),
            # wrong number of arguments: needs complete item/increment pairs.
            ('TOPK.INCRBY', "wrong number of arguments for 'TOPK.INCRBY' command"),
            ('TOPK.INCRBY dup', "wrong number of arguments for 'TOPK.INCRBY' command"),
            ('TOPK.INCRBY dup apple', "wrong number of arguments for 'TOPK.INCRBY' command"),
            ('TOPK.INCRBY dup apple 1 banana', "wrong number of arguments for 'TOPK.INCRBY' command"),
            # key must exist.
            ('TOPK.INFO missing', 'TopK: key does not exist'),
            # wrong type
            ('TOPK.INFO strkey', 'WRONGTYPE Operation against a key holding the wrong kind of value'),
            # wrong number of arguments 
            ('TOPK.INFO', "wrong number of arguments for 'TOPK.INFO' command"),
            ('TOPK.INFO dup extra', "wrong number of arguments for 'TOPK.INFO' command"),
            # key must exist.
            ('TOPK.LIST missing', 'TopK: key does not exist'),
            # wrong type
            ('TOPK.LIST strkey', 'WRONGTYPE Operation against a key holding the wrong kind of value'),
            # only WITHCOUNT is accepted as the optional third argument.
            ('TOPK.LIST dup NOTWITHCOUNT', 'ERROR'),
            # wrong number of arguments.
            ('TOPK.LIST', "wrong number of arguments for 'TOPK.LIST' command"),
            ('TOPK.LIST dup WITHCOUNT extra', "wrong number of arguments for 'TOPK.LIST' command"),
        ]
        for cmd, expected_err_reply in basic_error_test_cases:
            self.verify_error_response(self.client, cmd, expected_err_reply)

    def test_topk_command_behavior(self):
        reserve_success_cases = [
            'TOPK.RESERVE tk1 5',                    # arity 3: defaults, random seed
            'TOPK.RESERVE tk2 5 SEED 42',            # arity 5: defaults + seed
            'TOPK.RESERVE tk3 5 seed 42',            # case-insensitive SEED token
            'TOPK.RESERVE tk4 5 50 4 0.9',           # arity 6: full tuning
            'TOPK.RESERVE tk5 10 200 5 0.5 SEED 42', # arity 8: tuning + trailing seed
            'TOPK.RESERVE tk6 10 SEED 42 200 5 0.5', # arity 8: leading seed
            'TOPK.RESERVE tk7 10 seed 42 200 5 0.5', # leading seed, lower-case
        ]
        for cmd in reserve_success_cases:
            assert self.client.execute_command(cmd) == b'OK'
        assert self.client.execute_command('DBSIZE') == len(reserve_success_cases)

        # A reserved name is free again once deleted.
        assert self.client.execute_command('DEL tk1') == 1
        assert self.client.execute_command('TOPK.RESERVE tk1 5') == b'OK'
        assert self.client.execute_command('TOPK.RESERVE tk_add 5 50 4 0.9') == b'OK'
        add_success_cases = [
            ('TOPK.ADD tk_add apple', 1),
            ('TOPK.ADD tk_add apple banana cherry', 3),
            ('TOPK.ADD tk_add a b c d e f g', 7),
        ]
        for cmd, expected_len in add_success_cases:
            assert len(self.client.execute_command(cmd)) == expected_len

        # TOPK.INCRBY behaves like ADD but with explicit per-item increments.
        assert self.client.execute_command('TOPK.RESERVE tk_incr 5 50 4 0.9') == b'OK'
        incrby_success_cases = [
            ('TOPK.INCRBY tk_incr apple 1', 1),
            ('TOPK.INCRBY tk_incr apple 5 banana 3', 2),
            ('TOPK.INCRBY tk_incr a 1 b 2 c 3', 3),
        ]
        for cmd, expected_len in incrby_success_cases:
            assert len(self.client.execute_command(cmd)) == expected_len

        # TOPK.INFO reports k, width, depth, and decay of an existing sketch.
        def info_dict(key):
            raw = self.client.execute_command(f'TOPK.INFO {key}')
            it = iter(raw)
            return dict(zip(it, it))
        info = info_dict('tk1')
        assert info[b'k'] == 5
        assert info[b'width'] == 8
        assert info[b'depth'] == 7
        assert info[b'decay'] == b'0.9'

        info = info_dict('tk5')
        assert info[b'k'] == 10
        assert info[b'width'] == 200
        assert info[b'depth'] == 5
        assert info[b'decay'] == b'0.5'

        # TOPK.LIST returns tracked items by descending count, at most k.
        assert self.client.execute_command('TOPK.RESERVE tk_list 3 50 4 0.9 SEED 42') == b'OK'
        self.client.execute_command('TOPK.INCRBY tk_list apple 10 banana 5 cherry 2')

        listed = self.client.execute_command('TOPK.LIST tk_list')
        assert listed == [b'apple', b'banana', b'cherry']

        # WITHCOUNT interleaves each item with its estimated count.
        listed_wc = self.client.execute_command('TOPK.LIST tk_list WITHCOUNT')
        it = iter(listed_wc)
        counts = dict(zip(it, it))
        assert counts[b'apple'] == 10
        assert counts[b'banana'] == 5
        assert counts[b'cherry'] == 2

        # WITHCOUNT is case-insensitive.
        assert self.client.execute_command('TOPK.LIST tk_list withcount') == listed_wc

        # The list never exceeds k even when more distinct items are added.
        self.client.execute_command('TOPK.INCRBY tk_list durian 1 elderberry 1')
        assert len(self.client.execute_command('TOPK.LIST tk_list')) <= 3
