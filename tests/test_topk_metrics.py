from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util.waiters import *

# Placeholder per-object memory: size_of::<TopKObject>(), excluding the sketch
# heap, so it is fixed regardless of k/width/depth.
DEFAULT_TOPK_SIZE = 360


class TestTopkMetrics(ValkeyBloomTestCaseBase):

    def test_basic_command_metrics(self):
        # Metrics start at 0.
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), 0, 0, 0, 0)
        self.verify_topk_metrics(self.client.execute_command("INFO Modules"), 0, 0, 0, 0)

        # Reserve a TopK (k=5). One object, no items yet, sum_k = 5.
        assert self.client.execute_command('TOPK.RESERVE key 5 50 4 0.9 SEED 42') == b'OK'
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), DEFAULT_TOPK_SIZE, 1, 0, 5)

        # TOPK.ADD counts +1 per item. Three items -> num_items = 3.
        self.client.execute_command('TOPK.ADD key apple banana cherry')
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), DEFAULT_TOPK_SIZE, 1, 3, 5)

        # TOPK.INCRBY counts += increment. +5 and +4 -> num_items = 3 + 9 = 12.
        self.client.execute_command('TOPK.INCRBY key apple 5 banana 4')
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), DEFAULT_TOPK_SIZE, 1, 12, 5)

        # Read-only commands must not move any gauge.
        self.client.execute_command('TOPK.QUERY key apple missing')
        self.client.execute_command('TOPK.COUNT key apple')
        self.client.execute_command('TOPK.LIST key')
        self.client.execute_command('TOPK.INFO key')
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), DEFAULT_TOPK_SIZE, 1, 12, 5)

        # A second object (k=4) adds to num_objects, memory, and sum_k (5 + 4 = 9).
        assert self.client.execute_command('TOPK.RESERVE key2 4 50 4 0.9 SEED 42') == b'OK'
        self.client.execute_command('TOPK.ADD key2 x y')
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), DEFAULT_TOPK_SIZE * 2, 2, 14, 9)

        # Deleting a key removes its full contribution (memory, k=4, its 2 items).
        self.client.execute_command('DEL key2')
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), DEFAULT_TOPK_SIZE, 1, 12, 5)

        # FLUSHDB drops everything; all gauges return to 0.
        self.client.execute_command('FLUSHDB')
        wait_for_equal(lambda: self.client.execute_command('DBSIZE'), 0)
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), 0, 0, 0, 0)
        self.verify_topk_metrics(self.client.execute_command("INFO Modules"), 0, 0, 0, 0)

    def test_copy_metrics(self):
        # Reserve a TopK (k=5), add 3 items, then increment by 5 -> num_items = 8.
        assert self.client.execute_command('TOPK.RESERVE orig 5 50 4 0.9 SEED 42') == b'OK'
        self.client.execute_command('TOPK.ADD orig apple banana cherry')
        self.client.execute_command('TOPK.INCRBY orig apple 5')
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), DEFAULT_TOPK_SIZE, 1, 8, 5)

        # Deep copy
        assert self.client.execute_command('COPY orig copied') == 1
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), DEFAULT_TOPK_SIZE * 2, 2, 16, 10)

        # The copy holds the same Top-K members as the source.
        assert self.client.execute_command('TOPK.LIST orig') == self.client.execute_command('TOPK.LIST copied')

        # Deleting the source leaves the copy's contribution intact.
        self.client.execute_command('DEL orig')
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), DEFAULT_TOPK_SIZE, 1, 8, 5)

        # Perform a FLUSHALL which should set all metrics data to 0
        self.client.execute_command('FLUSHALL')
        wait_for_equal(lambda: self.client.execute_command('DBSIZE'), 0)
        self.verify_topk_metrics(self.client.execute_command("INFO bf"), 0, 0, 0, 0)
