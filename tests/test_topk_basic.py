from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkey_bloom_test_case import ValkeyBloomTestCaseBase


class TestTopkBasic(ValkeyBloomTestCaseBase):
    """
    Basic behavioral coverage for TOPK.ADD eviction semantics.

    These tests exercise which item (if any) is displaced from the top-k by
    an insertion. The displaced item depends on the sketch's hash seed, so we
    reserve with an explicit SEED to keep results deterministic across runs.
    """

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

