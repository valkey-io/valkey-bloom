import time
from valkey_bloom_test_case import SkipSeedParameterizationMixin, ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import *
import pytest

@pytest.mark.skip_for_asan(reason="These tests are skipped due to not being able to set activedefrag to yes when valkey server is an ASAN build")
class TestTopkDefrag(SkipSeedParameterizationMixin, ValkeyBloomTestCaseBase):

    def test_topk_defrag(self):
        # Set defragmentation thresholds
        self.client.config_set('activedefrag', 'no')
        self.client.config_set('active-defrag-ignore-bytes', '1')
        self.client.config_set('active-defrag-threshold-lower', '2')
        max_memory = 300 * 1024 * 1024
        self.client.config_set('maxmemory', str(max_memory))

        # Initial stats
        stats = self.parse_valkey_info("STATS")
        initial_defrag_hits = int(stats.get('active_defrag_hits', 0))

        # Create list of key names we will operate with. A wide sketch makes the
        # lobby/heavy cell arrays large enough to be worth relocating.
        key_names = [f'topk_{i}' for i in range(1, 1500)]

        # Insert data. Use the key name as item prefix so each sketch is distinct.
        for key in key_names:
            self.client.execute_command(f'TOPK.RESERVE {key} 20 200 5 0.9')
            self.client.execute_command(
                f'TOPK.ADD {key} ' + ' '.join(f'{key}_{i}' for i in range(1, 40))
            )

        # Delete every other object to create fragmentation.
        for key in key_names[::2]:
            self.client.execute_command(f'DEL {key}')
        remaining_keys = key_names[1::2]

        # Capture per-key state before defrag for a strict equality check after.
        digests_before_defrag = {key: self.client.execute_command(f'DEBUG DIGEST-VALUE {key}') for key in remaining_keys}
        lists_before_defrag = {key: self.client.execute_command(f'TOPK.LIST {key} WITHCOUNT') for key in remaining_keys}

        # Add a wait due to lazy delete where if we call info too early we wont get the correct memory info
        time.sleep(5)

        # Fragmentation before defrag, for a strict before/after comparison.
        frag_ratio_before = float(self.parse_valkey_info("MEMORY").get('allocator_frag_ratio', 0))

        # Enable defragmentation and wait until the topk callback has relocated something.
        self.client.config_set('activedefrag', 'yes')
        wait_for_equal(lambda: self.client.info("bf")['bf_topk_defrag_hits'] > 0, True)

        first_defrag_stats = self.parse_valkey_info("STATS")
        first_defrag_hits = int(first_defrag_stats.get('active_defrag_hits', 0))

        # The valkey-level defrag ran and our topk callback relocated allocations.
        assert first_defrag_hits > initial_defrag_hits

        # Fragmentation must actually drop once defrag has done its work. We wait
        # on the ratio itself (rather than sampling once) since defrag runs
        # asynchronously; a timeout here means fragmentation was never reduced.
        wait_for_equal(
            lambda: float(self.parse_valkey_info("MEMORY").get('allocator_frag_ratio', 0)) < frag_ratio_before,
            True,
        )

        # Data must be unchanged by defrag: digests and top-k lists match exactly.
        digests_after_defrag = {key: self.client.execute_command(f'DEBUG DIGEST-VALUE {key}') for key in remaining_keys}
        assert digests_before_defrag == digests_after_defrag, "Digest mismatch after defrag"
        lists_after_defrag = {key: self.client.execute_command(f'TOPK.LIST {key} WITHCOUNT') for key in remaining_keys}
        assert lists_before_defrag == lists_after_defrag, "TOPK.LIST mismatch after defrag"

        # The module-level defrag callback ran for the topk data type.
        info_results = self.client.info("bf")
        assert info_results['bf_topk_defrag_hits'] + info_results['bf_topk_defrag_misses'] > 0

        # A defragged object must still behave correctly under writes.
        sample_keys = remaining_keys[:20]
        evicted_any = False
        for key in sample_keys:
            # 'hot' is new to this sketch; repeated adds beat the count-1 residents.
            result = self.client.execute_command(f'TOPK.ADD {key} ' + ' '.join(['hot'] * 10))
            assert len(result) == 10, f"unexpected TOPK.ADD reply for {key}: {result}"
            evicted_any = evicted_any or any(item is not None for item in result)
            # 'hot' is now tracked, and the sketch still holds no more than k items.
            assert self.client.execute_command(f'TOPK.QUERY {key} hot') == [1], f"hot not tracked in {key}"
            listed = self.client.execute_command(f'TOPK.LIST {key}')
            assert len(listed) <= 20, f"{key} exceeds k after post-defrag adds: {len(listed)}"
        assert evicted_any, "no eviction observed on any defragged object"

        # Snapshot the post-write state so the RDB round-trip is checked against
        # the objects as they stand now.
        digests_after_writes = {key: self.client.execute_command(f'DEBUG DIGEST-VALUE {key}') for key in remaining_keys}

        # Round-trip through RDB and confirm the defragged, mutated objects survive.
        self.client.execute_command('BGSAVE')
        self.server.wait_for_save_done()

        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert self.server.is_alive()
        wait_for_equal(lambda: self.server.is_rdb_done_loading(), True)

        digests_after_rdb_load = {key: self.client.execute_command(f'DEBUG DIGEST-VALUE {key}') for key in remaining_keys}
        assert digests_after_writes == digests_after_rdb_load, "Digest mismatch after RDB load"
