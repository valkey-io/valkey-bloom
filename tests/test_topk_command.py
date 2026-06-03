from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401

class TestTopkCommand(ValkeyBloomTestCaseBase):

    def verify_command_arity(self, command, expected_arity):
        command_info = self.client.execute_command('COMMAND', 'INFO', command)
        actual_arity = command_info.get(command).get('arity')
        assert actual_arity == expected_arity, f"Arity mismatch for command '{command}'"

    def test_topk_command_arity(self):
        self.verify_command_arity('TOPK.RESERVE', -1)
        self.verify_command_arity('TOPK.ADD', -1)

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

        # ---- TOPK.ADD: no eviction ----
        # With k=3 and only three distinct items, nothing is ever displaced.
        assert self.client.execute_command('TOPK.RESERVE tk_noevict 3 50 4 0.9') == b'OK'
        assert self.client.execute_command(
            'TOPK.ADD tk_noevict apple banana cherry apple banana cherry'
        ) == [None] * 6
        # Repeated adds of an already-tracked item still return nil.
        assert self.client.execute_command('TOPK.ADD tk_noevict apple') == [None]

        # ---- TOPK.ADD: eviction returns the displaced item ----
        # k=1 means the second distinct item to outweigh the first evicts it.
        assert self.client.execute_command('TOPK.RESERVE tk_evict 1 50 4 0.9') == b'OK'
        assert self.client.execute_command('TOPK.ADD tk_evict apple') == [None]
        assert self.client.execute_command('TOPK.ADD tk_evict banana banana') == [None, b'apple']

        # ---- TOPK.ADD: mix of eviction and non-eviction in one call ----
        # k=2: first two distinct items fill the queue, a third displaces one
        # once it climbs high enough. Exactly one eviction of banana happens
        # across the two cherry adds.
        assert self.client.execute_command('TOPK.RESERVE tk_mixed 2 50 4 0.9') == b'OK'
        self.client.execute_command('TOPK.ADD tk_mixed apple apple banana')
        result = self.client.execute_command('TOPK.ADD tk_mixed cherry cherry')
        assert len(result) == 2
        assert [r for r in result if r is not None] == [b'banana']
