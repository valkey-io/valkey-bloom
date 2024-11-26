import pytest
from valkey import ResponseError
from valkeytests.valkey_test_case import ReplicationTestCase
from valkeytests.conftest import resource_port_tracker
import os

class TestBloomReplication(ReplicationTestCase):

    # Global Parameterized Configs
    use_random_seed = 'no'

    def get_custom_args(self):
        self.set_server_version(os.environ['SERVER_VERSION'])
        return {
            'loadmodule': os.getenv('MODULE_PATH'),
            'bf.bloom-use-random-seed': self.use_random_seed,
        }

    @pytest.fixture(autouse=True)
    def use_random_seed_fixture(self, bloom_config_parameterization):
        if bloom_config_parameterization == "random-seed":
            self.use_random_seed = "yes"
        elif bloom_config_parameterization == "fixed-seed":
            self.use_random_seed = "no"

    def test_replication_behavior(self):
        self.setup_replication(num_replicas=1)
        is_random_seed = self.client.execute_command('CONFIG GET bf.bloom-use-random-seed')
        # Test replication for write commands.
        bloom_write_cmds = [
            ('BF.ADD', 'BF.ADD key item', 'BF.ADD key item1', 2),
            ('BF.MADD', 'BF.MADD key item', 'BF.MADD key item1', 2),
            ('BF.RESERVE', 'BF.RESERVE key 0.001 100000', 'BF.ADD key item1', 1),
            ('BF.INSERT', 'BF.INSERT key items item', 'BF.INSERT key items item1', 2),
        ]
        for test_case in bloom_write_cmds:
            prefix = test_case[0]
            create_cmd = test_case[1]
            # New bloom object being created is replicated.
            self.client.execute_command(create_cmd)
            assert self.client.execute_command('EXISTS key') == 1
            self.waitForReplicaToSyncUp(self.replicas[0])
            assert self.replicas[0].client.execute_command('EXISTS key') == 1

            # New item added to an existing bloom is replicated.
            item_add_cmd = test_case[2]
            self.client.execute_command(item_add_cmd)
            assert self.client.execute_command('BF.EXISTS key item1') == 1
            self.waitForReplicaToSyncUp(self.replicas[0])
            assert self.replicas[0].client.execute_command('BF.EXISTS key item1') == 1

            # Validate that the bloom object creation command and item add command was replicated.
            expected_calls = test_case[3]
            primary_cmd_stats = self.client.info("Commandstats")['cmdstat_' + prefix]
            replica_cmd_stats = self.replicas[0].client.info("Commandstats")['cmdstat_' + prefix]
            assert primary_cmd_stats["calls"] == expected_calls and replica_cmd_stats["calls"] == expected_calls

            # Attempting to add an existing item to an existing bloom will NOT replicated.
            self.client.execute_command(item_add_cmd)
            self.waitForReplicaToSyncUp(self.replicas[0])
            primary_cmd_stats = self.client.info("Commandstats")
            replica_cmd_stats = self.replicas[0].client.info("Commandstats")
            if prefix == 'BF.RESERVE':
                assert primary_cmd_stats['cmdstat_' + prefix]["calls"] == 1 and replica_cmd_stats['cmdstat_' + prefix]["calls"] == 1
                assert primary_cmd_stats['cmdstat_BF.ADD']["calls"] == 2 and replica_cmd_stats['cmdstat_BF.ADD']["calls"] == 1
            else:
                assert primary_cmd_stats['cmdstat_' + prefix]["calls"] == (expected_calls + 1) and replica_cmd_stats['cmdstat_' + prefix]["calls"] == expected_calls
            
            # cmd debug digest
            server_digest_primary = self.client.debug_digest()
            assert server_digest_primary != None or 0000000000000000000000000000000000000000
            server_digest_replica = self.client.debug_digest()
            assert server_digest_primary == server_digest_replica
            object_digest_primary = self.client.execute_command('DEBUG DIGEST-VALUE key')
            debug_digest_replica = self.replicas[0].client.execute_command('DEBUG DIGEST-VALUE key')
            # TODO: Update the test here to validate that digest always matches during replication. Once we implement
            # deterministic replication (including replicating seeds), this assert will be updated.
            if is_random_seed[1] == b'yes':
                assert object_digest_primary != debug_digest_replica
            else:
                assert object_digest_primary == debug_digest_replica

            self.client.execute_command('FLUSHALL')
            self.waitForReplicaToSyncUp(self.replicas[0])
            self.client.execute_command('CONFIG RESETSTAT')
            self.replicas[0].client.execute_command('CONFIG RESETSTAT')

        self.client.execute_command('BF.ADD key item1')
        self.waitForReplicaToSyncUp(self.replicas[0])

        # Read commands executed on the primary will not be replicated.
        read_commands = [
            ('BF.EXISTS', 'BF.EXISTS key item1', 1),
            ('BF.MEXISTS', 'BF.MEXISTS key item1 item2', 1),
            ('BF.INFO', 'BF.INFO key', 1),
            ('BF.INFO', 'BF.INFO key Capacity', 2),
            ('BF.INFO', 'BF.INFO key ITEMS', 3),
            ('BF.INFO', 'BF.INFO key filters', 4),
            ('BF.INFO', 'BF.INFO key size', 5),
            ('BF.INFO', 'BF.INFO key expansion', 6),
            ('BF.CARD', 'BF.CARD key', 1)
        ]
        for test_case in read_commands:
            prefix = test_case[0]
            cmd = test_case[1]
            expected_primary_calls = test_case[2]
            self.client.execute_command(cmd)
            primary_cmd_stats = self.client.info("Commandstats")['cmdstat_' + prefix]
            assert primary_cmd_stats["calls"] == expected_primary_calls
            assert ('cmdstat_' + prefix) not in self.replicas[0].client.info("Commandstats")

        # Deletes of bloom objects are replicated
        assert self.client.execute_command("EXISTS key") == 1
        assert self.replicas[0].client.execute_command('EXISTS key') == 1
        assert self.client.execute_command("DEL key") == 1
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.client.execute_command("EXISTS key") == 0
        assert self.replicas[0].client.execute_command('EXISTS key') == 0

        self.client.execute_command('CONFIG RESETSTAT')
        self.replicas[0].client.execute_command('CONFIG RESETSTAT')

        # Write commands with errors are not replicated.
        invalid_bloom_write_cmds = [
            ('BF.ADD', 'BF.ADD key item1 item2'),
            ('BF.MADD', 'BF.MADD key'),
            ('BF.RESERVE', 'BF.RESERVE key 1.001 100000'),
            ('BF.INSERT', 'BF.INSERT key CAPACITY 0 items item'),
        ]
        for test_case in invalid_bloom_write_cmds:
            prefix = test_case[0]
            cmd = test_case[1]
            try:
                self.client.execute_command(cmd)
                assert False
            except ResponseError as e:
                pass
            primary_cmd_stats = self.client.info("Commandstats")['cmdstat_' + prefix]
            assert primary_cmd_stats["calls"] == 1
            assert primary_cmd_stats["failed_calls"] == 1
            assert ('cmdstat_' + prefix) not in self.replicas[0].client.info("Commandstats")
