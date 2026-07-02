import pytest, os
from valkey import ResponseError
from valkeytestframework.valkey_test_case import ReplicationTestCase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkey_bloom_test_case import setup_replication_servers, TopkFixedSeedMixin

class TestTopkReplication(TopkFixedSeedMixin, ReplicationTestCase):

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        setup_replication_servers(self)

    def test_replication_behavior(self):
        use_external = os.environ.get("VALKEY_EXTERNAL_SERVER", "false").lower() == "true"
        if use_external:
            self.wait_for_primary_link_up_all_replicas()
        else:
            self.setup_replication(num_replicas=1)

        # Test replication for write commands.
        topk_write_cmds = [
            ('TOPK.RESERVE', 'TOPK.RESERVE key 5 50 4 0.9 SEED 42'),
            ('TOPK.ADD', 'TOPK.ADD key item'),
            ('TOPK.INCRBY', 'TOPK.INCRBY key item 3'),
        ]
        for test_case in topk_write_cmds:
            prefix = test_case[0]
            cmd = test_case[1]
            self.client.execute_command(cmd)
            assert self.client.execute_command('EXISTS key') == 1
            self.waitForReplicaToSyncUp(self.replicas[0])
            assert self.replicas[0].client.execute_command('EXISTS key') == 1
            # The command was replayed on the replica under its own name.
            assert ('cmdstat_' + prefix) in self.replicas[0].client.info("Commandstats")

            # cmd debug digest
            server_digest_primary = self.client.execute_command('DEBUG', 'DIGEST')
            server_digest_replica = self.replicas[0].client.execute_command('DEBUG', 'DIGEST')
            assert server_digest_primary == server_digest_replica
            object_digest_primary = self.client.execute_command('DEBUG', 'DIGEST-VALUE', 'key')
            debug_digest_replica = self.replicas[0].client.execute_command('DEBUG', 'DIGEST-VALUE', 'key')
            assert object_digest_primary == debug_digest_replica

        self.client.execute_command('CONFIG RESETSTAT')
        self.replicas[0].client.execute_command('CONFIG RESETSTAT')

        # Read commands executed on the primary will not be replicated.
        read_commands = [
            ('TOPK.QUERY', 'TOPK.QUERY key item'),
            ('TOPK.COUNT', 'TOPK.COUNT key item'),
            ('TOPK.LIST', 'TOPK.LIST key'),
            ('TOPK.INFO', 'TOPK.INFO key'),
        ]
        for test_case in read_commands:
            prefix = test_case[0]
            cmd = test_case[1]
            self.client.execute_command(cmd)
            assert ('cmdstat_' + prefix) in self.client.info("Commandstats")
            assert ('cmdstat_' + prefix) not in self.replicas[0].client.info("Commandstats")

        # Deletes of topk objects are replicated
        assert self.client.execute_command("EXISTS key") == 1
        assert self.replicas[0].client.execute_command('EXISTS key') == 1
        assert self.client.execute_command("DEL key") == 1
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.client.execute_command("EXISTS key") == 0
        assert self.replicas[0].client.execute_command('EXISTS key') == 0

        # Write commands with errors are not replicated.
        invalid_topk_write_cmds = [
            ('TOPK.RESERVE', 'TOPK.RESERVE key 5 50 4 5'),
            ('TOPK.ADD', 'TOPK.ADD missing item'),
            ('TOPK.INCRBY', 'TOPK.INCRBY missing item 1'),
        ]
        for test_case in invalid_topk_write_cmds:
            prefix = test_case[0]
            cmd = test_case[1]
            self.client.execute_command('CONFIG RESETSTAT')
            self.replicas[0].client.execute_command('CONFIG RESETSTAT')
            try:
                self.client.execute_command(cmd)
                assert False
            except ResponseError:
                pass
            primary_cmd_stats = self.client.info("Commandstats")['cmdstat_' + prefix]
            assert primary_cmd_stats["calls"] == 1
            assert primary_cmd_stats["failed_calls"] == 1
            assert ('cmdstat_' + prefix) not in self.replicas[0].client.info("Commandstats")

    def test_topk_digest_consistency_after_many_adds(self):
        use_external = os.environ.get("VALKEY_EXTERNAL_SERVER", "false").lower() == "true"
        if use_external:
            self.wait_for_primary_link_up_all_replicas()
        else:
            self.setup_replication(num_replicas=1)

        # Cover both seeding paths
        reserve_cmds = [
            ('tk_fixed', 'TOPK.RESERVE tk_fixed 50 200 5 0.9 SEED 42'),
            ('tk_random', 'TOPK.RESERVE tk_random 50 200 5 0.9'),
        ]
        for key, reserve_cmd in reserve_cmds:
            assert self.client.execute_command(reserve_cmd) == b'OK'
            # k=50 with 800 distinct items forces heavy eviction/contention.
            for i in range(0, 5000):
                self.client.execute_command(f'TOPK.INCRBY {key} item{i % 800} {i % 7 + 1}')

        self.waitForReplicaToSyncUp(self.replicas[0])

        server_digest_primary = self.client.execute_command('DEBUG', 'DIGEST')
        server_digest_replica = self.replicas[0].client.execute_command('DEBUG', 'DIGEST')
        assert server_digest_primary == server_digest_replica

        for key, _ in reserve_cmds:
            object_digest_primary = self.client.execute_command('DEBUG', 'DIGEST-VALUE', key)
            object_digest_replica = self.replicas[0].client.execute_command('DEBUG', 'DIGEST-VALUE', key)
            assert object_digest_primary == object_digest_replica
            # The Top-K list itself should also be identical.
            assert self.client.execute_command(f'TOPK.LIST {key} WITHCOUNT') == \
                self.replicas[0].client.execute_command(f'TOPK.LIST {key} WITHCOUNT')
