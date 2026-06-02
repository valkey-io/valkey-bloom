from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkey import ResponseError


class TestTopkCommand(ValkeyBloomTestCaseBase):
    """
    Basic coverage for TOPK.RESERVE.

    Syntax:
        TOPK.RESERVE key topk [SEED seed] [width depth decay] [SEED seed]

    The width/depth/decay group is all-or-nothing; defaults apply when the
    group is omitted (DEFAULT_WIDTH=8, DEFAULT_DEPTH=7, DEFAULT_DECAY=0.9).
    The SEED keyword may appear either right after `topk` or at the very
    end, but not both. Valid arities are 3, 5, 6, and 8.
    
    """

    def test_topk_reserve_success(self):
        # arity 3: defaults for width/depth/decay, server-generated seed.
        assert self.client.execute_command('TOPK.RESERVE tk1 5') == b'OK'

        # arity 5: defaults plus an explicit seed (case-insensitive token).
        assert self.client.execute_command(
            'TOPK.RESERVE tk2 5 SEED 42'
        ) == b'OK'
        assert self.client.execute_command(
            'TOPK.RESERVE tk3 5 seed 42'
        ) == b'OK'

        # arity 6: full sketch tuning, server-generated seed.
        assert self.client.execute_command(
            'TOPK.RESERVE tk4 5 50 4 0.9'
        ) == b'OK'

        # arity 8: full tuning plus explicit seed.
        assert self.client.execute_command(
            'TOPK.RESERVE tk5 10 200 5 0.5 SEED 42'
        ) == b'OK'

        # arity 8: SEED block leading, before the sketch params.
        assert self.client.execute_command(
            'TOPK.RESERVE tk6 10 SEED 42 200 5 0.5'
        ) == b'OK'

        # Case-insensitive SEED token in the leading position.
        assert self.client.execute_command(
            'TOPK.RESERVE tk7 10 seed 42 200 5 0.5'
        ) == b'OK'

        assert self.client.execute_command('DBSIZE') == 7

    def test_topk_reserve_key_already_exists(self):
        assert self.client.execute_command('TOPK.RESERVE dup 5') == b'OK'
        # Re-reserving the same key is rejected. TOPK params are immutable.
        self.verify_error_response(
            self.client,
            'TOPK.RESERVE dup 5',
            'BUSYKEY Target key name already exists.',
        )

        # After the key is deleted, the name is free to be reserved again.
        assert self.client.execute_command('DEL dup') == 1
        assert self.client.execute_command('TOPK.RESERVE dup 5') == b'OK'

    def test_topk_reserve_wrong_arity(self):
        # The handler accepts exactly 3, 5, 6, or 8 args. Anything else is a
        # wrong-arity error from valkey-server.
        wrong_arity_cases = [
            'TOPK.RESERVE',                              # 1
            'TOPK.RESERVE key',                          # 2
            'TOPK.RESERVE key 5 50',                     # 4: partial sketch params
            'TOPK.RESERVE key 5 50 4 0.9 SEED',          # 7: token without value
            'TOPK.RESERVE key 5 50 4 0.9 SEED 42 extra', # 9: trailing junk
        ]
        for cmd in wrong_arity_cases:
            self.verify_error_response(
                self.client,
                cmd,
                "wrong number of arguments for 'TOPK.RESERVE' command",
            )

        # These have a *valid* arity (5) but the token at position 3 is not
        # SEED, so they fall through to the syntax-error path.
        for cmd in [
            'TOPK.RESERVE key 5 50 4',         # arity 5, "50" where SEED expected
            'TOPK.RESERVE key 5 NOTSEED 42',   # arity 5, wrong literal token
        ]:
            self.verify_error_response(self.client, cmd, 'ERROR')

        # SEED appearing both before and after the sketch params is ambiguous
        # and rejected, even though arity 8 is otherwise valid.
        self.verify_error_response(
            self.client,
            'TOPK.RESERVE key 5 SEED 1 SEED 2',
            "wrong number of arguments for 'TOPK.RESERVE' command",
        )

    def test_topk_reserve_bad_params(self):
        validation_cases = [
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
            # Seed value must parse as u64.
            ('TOPK.RESERVE k1 5 SEED abc', 'invalid seed'),
            ('TOPK.RESERVE k1 5 SEED -1', 'invalid seed'),
            ('TOPK.RESERVE k1 5 50 4 0.9 SEED abc', 'invalid seed'),
        ]
        for cmd, expected_err in validation_cases:
            self.verify_error_response(self.client, cmd, expected_err)

        # None of the failed RESERVE calls should have created a key.
        assert self.client.execute_command('DBSIZE') == 0

    def test_topk_reserve_wrong_type(self):
        # Reserving on a key that already holds a non-TOPK value should fail.
        assert self.client.execute_command('SET strkey hello') == b'OK'
        try:
            self.client.execute_command('TOPK.RESERVE strkey 5')
            assert False, 'TOPK.RESERVE on a string key should fail'
        except ResponseError as e:
            # WRONGTYPE is the standard Valkey error for this case.
            assert 'WRONGTYPE' in str(e), f'unexpected error: {e}'
