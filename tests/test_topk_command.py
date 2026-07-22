from valkey_bloom_test_case import SkipSeedParameterizationMixin, ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401

class TestTopkCommand(SkipSeedParameterizationMixin, ValkeyBloomTestCaseBase):

    def test_topk_command_arity(self):
        self.verify_command_arity('TOPK.RESERVE', -1)
        self.verify_command_arity('TOPK.ADD', -1)
        self.verify_command_arity('TOPK.INCRBY', -1)
        self.verify_command_arity('TOPK.INFO', -1)
        self.verify_command_arity('TOPK.LIST', -1)
        self.verify_command_arity('TOPK.COUNT', -1)
        self.verify_command_arity('TOPK.QUERY', -1)

    def test_topk_command_error(self):
        # test set up
        assert self.client.execute_command('TOPK.RESERVE dup 5') == b'OK'
        assert self.client.execute_command('SET strkey hello') == b'OK'
        basic_error_test_cases = [
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
            # an unrecognized field token is rejected.
            ('TOPK.INFO dup bogus', 'invalid information value'),
            # wrong number of arguments: key plus at most one field token.
            ('TOPK.INFO', "wrong number of arguments for 'TOPK.INFO' command"),
            ('TOPK.INFO dup K extra', "wrong number of arguments for 'TOPK.INFO' command"),
            # key must exist.
            ('TOPK.LIST missing', 'TopK: key does not exist'),
            # wrong type
            ('TOPK.LIST strkey', 'WRONGTYPE Operation against a key holding the wrong kind of value'),
            # only WITHCOUNT is accepted as the optional third argument.
            ('TOPK.LIST dup NOTWITHCOUNT', 'ERROR'),
            # wrong number of arguments.
            ('TOPK.LIST', "wrong number of arguments for 'TOPK.LIST' command"),
            ('TOPK.LIST dup WITHCOUNT extra', "wrong number of arguments for 'TOPK.LIST' command"),
            # key must exist.
            ('TOPK.COUNT missing apple', 'TopK: key does not exist'),
            # wrong type
            ('TOPK.COUNT strkey apple', 'WRONGTYPE Operation against a key holding the wrong kind of value'),
            # wrong number of arguments: needs key plus at least one item.
            ('TOPK.COUNT', "wrong number of arguments for 'TOPK.COUNT' command"),
            ('TOPK.COUNT dup', "wrong number of arguments for 'TOPK.COUNT' command"),
            # key must exist.
            ('TOPK.QUERY missing apple', 'TopK: key does not exist'),
            # wrong type
            ('TOPK.QUERY strkey apple', 'WRONGTYPE Operation against a key holding the wrong kind of value'),
            # wrong number of arguments: needs key plus at least one item.
            ('TOPK.QUERY', "wrong number of arguments for 'TOPK.QUERY' command"),
            ('TOPK.QUERY dup', "wrong number of arguments for 'TOPK.QUERY' command"),
            # loading over an existing key is rejected before decode.
            ('TOPK.LOAD dup blob', 'BUSYKEY Target key name already exists.'),
            ('TOPK.LOAD strkey blob', 'WRONGTYPE Operation against a key holding the wrong kind of value'),
            # a blob that is not valid TopK serialization is rejected.
            ('TOPK.LOAD newkey garbage', 'topk object decoding failed'),
            # wrong number of arguments.
            ('TOPK.LOAD', "wrong number of arguments for 'TOPK.LOAD' command"),
            ('TOPK.LOAD key', "wrong number of arguments for 'TOPK.LOAD' command"),
            ('TOPK.LOAD key blob extra', "wrong number of arguments for 'TOPK.LOAD' command"),
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
            ('TOPK.ADD tk_add 12345 67890', 2),
            ('TOPK.ADD tk_add item-1 item_2 item.3 item:4', 4),
            ('TOPK.ADD tk_add Item1 ITEM2 iTeM3', 3),
            ('TOPK.ADD tk_add dup dup dup', 3),
            ('TOPK.ADD tk_add ' + 'x' * 256, 1),
        ]
        for cmd, expected_len in add_success_cases:
            assert len(self.client.execute_command(cmd)) == expected_len

        # TOPK.INCRBY behaves like ADD but with explicit per-item increments.
        assert self.client.execute_command('TOPK.RESERVE tk_incr 5 50 4 0.9') == b'OK'
        incrby_success_cases = [
            ('TOPK.INCRBY tk_incr apple 1', 1),
            ('TOPK.INCRBY tk_incr apple 5 banana 3', 2),
            ('TOPK.INCRBY tk_incr a 1 b 2 c 3', 3),
            ('TOPK.INCRBY tk_incr 12345 7 item-1 2', 2),
            ('TOPK.INCRBY tk_incr whale 1000000', 1),
            ('TOPK.INCRBY tk_incr dup 2 dup 3', 2),
        ]
        for cmd, expected_len in incrby_success_cases:
            assert len(self.client.execute_command(cmd)) == expected_len
        # Total items added is the sum of every increment
        tk_incr_total = sum(
            int(inc)
            for cmd, _ in incrby_success_cases
            for inc in cmd.split()[3::2]
        )

        # TOPK.INFO reports k, width, depth, decay, size, and total items added
        # for an existing sketch.
        def info_dict(key):
            raw = self.client.execute_command(f'TOPK.INFO {key}')
            it = iter(raw)
            return dict(zip(it, it))
        info = info_dict('tk1')
        assert info[b'k'] == 5
        assert info[b'width'] == 8
        assert info[b'depth'] == 7
        assert info[b'decay'] == b'0.9'
        assert info[b'size'] > 0
        # tk1 was re-reserved with no items, so nothing has been added yet.
        assert info[b'total items added'] == 0

        info = info_dict('tk5')
        assert info[b'k'] == 10
        assert info[b'width'] == 200
        assert info[b'depth'] == 5
        assert info[b'decay'] == b'0.5'
        assert info[b'size'] > 0
        assert info[b'total items added'] == 0

        info = info_dict('tk_incr')
        assert info[b'total items added'] == tk_incr_total

        # TOPK.INFO key <field> returns just that single value.
        assert self.client.execute_command('TOPK.INFO tk5 K') == 10
        assert self.client.execute_command('TOPK.INFO tk5 WIDTH') == 200
        assert self.client.execute_command('TOPK.INFO tk5 depth') == 5
        assert self.client.execute_command('TOPK.INFO tk5 DECAY') == b'0.5'
        assert self.client.execute_command('TOPK.INFO tk5 SIZE') == info_dict('tk5')[b'size']
        assert self.client.execute_command('TOPK.INFO tk_incr TOTALITEMSADDED') == tk_incr_total

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

        # TOPK.COUNT returns the estimated count per item, in order.
        assert self.client.execute_command('TOPK.RESERVE tk_count 3 50 4 0.9 SEED 42') == b'OK'
        self.client.execute_command('TOPK.INCRBY tk_count apple 10 banana 5 cherry 2')
        assert self.client.execute_command('TOPK.COUNT tk_count apple') == [10]
        assert self.client.execute_command('TOPK.COUNT tk_count apple banana cherry') == [10, 5, 2]
        assert self.client.execute_command('TOPK.COUNT tk_count missing') == [0]
        assert self.client.execute_command('TOPK.COUNT tk_count apple missing banana') == [10, 0, 5]
        assert self.client.execute_command('TOPK.COUNT tk_count apple apple') == [10, 10]

        # TOPK.QUERY reports top-k membership (1/0) per item, in order.
        assert self.client.execute_command('TOPK.RESERVE tk_query 3 50 4 0.9 SEED 42') == b'OK'
        self.client.execute_command('TOPK.INCRBY tk_query apple 10 banana 5 cherry 2')
        assert self.client.execute_command('TOPK.QUERY tk_query apple') == [1]
        assert self.client.execute_command('TOPK.QUERY tk_query missing') == [0]
        assert self.client.execute_command('TOPK.QUERY tk_query apple banana cherry') == [1, 1, 1]
        assert self.client.execute_command('TOPK.QUERY tk_query apple missing banana') == [1, 0, 1]

        # QUERY agrees with the membership reported by LIST.
        listed = set(self.client.execute_command('TOPK.LIST tk_query'))
        for item in [b'apple', b'banana', b'cherry', b'missing']:
            expected = 1 if item in listed else 0
            assert self.client.execute_command('TOPK.QUERY tk_query', item) == [expected]

        # An item displaced from the top-k reports 0
        assert self.client.execute_command('TOPK.RESERVE tk_evict 2 50 4 0.9 SEED 42') == b'OK'
        self.client.execute_command('TOPK.INCRBY tk_evict apple 5 banana 10 cherry 20')
        assert self.client.execute_command('TOPK.QUERY tk_evict apple') == [0]
        assert self.client.execute_command('TOPK.QUERY tk_evict banana cherry') == [1, 1]
