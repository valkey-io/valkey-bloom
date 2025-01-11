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

    def validate_cmd_stats(self, primary_cmd, replica_cmd, expected_primary_calls, expected_replica_calls):
        """
            Helper fn to validate cmd count on primary & replica for non BF.RESERVE cases for object creation & item add.
        """
        primary_cmd_stats = self.client.info("Commandstats")['cmdstat_' + primary_cmd]
        assert primary_cmd_stats["calls"] == expected_primary_calls
        replica_cmd_stats = self.replicas[0].client.info("Commandstats")['cmdstat_' + replica_cmd]
        assert replica_cmd_stats["calls"] == expected_replica_calls

    def validate_reserve_cmd_stats(self, primary_reserve_count, primary_add_count, replica_insert_count, replica_add_count):
        """
            Helper fn to validate cmd count on primary & replica for the BF.RESERVE case for object creation & item add.
        """
        primary_cmd_stats = self.client.info("Commandstats")
        replica_cmd_stats = self.replicas[0].client.info("Commandstats")
        assert primary_cmd_stats['cmdstat_BF.RESERVE']["calls"] == primary_reserve_count and primary_cmd_stats['cmdstat_BF.ADD']["calls"] == primary_add_count
        assert replica_cmd_stats['cmdstat_BF.INSERT']["calls"] == replica_insert_count and replica_cmd_stats['cmdstat_BF.ADD']["calls"] == replica_add_count

    def test_replication_behavior(self):
        self.setup_replication(num_replicas=1)
        # Test replication for write commands.
        bloom_write_cmds = [
            ('BF.ADD', 'BF.ADD key item', 'BF.ADD key item1', 1),
            ('BF.MADD', 'BF.MADD key item', 'BF.MADD key item1', 1),
            ('BF.RESERVE', 'BF.RESERVE key 0.001 100000', 'BF.ADD key item1', 1),
            ('BF.INSERT', 'BF.INSERT key items item', 'BF.INSERT key items item1', 2),
        ]
        for test_case in bloom_write_cmds:
            prefix = test_case[0]
            create_cmd = test_case[1]
            # New bloom object being created is replicated.
            # Validate that the bloom object creation command replicated as BF.INSERT.
            self.client.execute_command(create_cmd)
            assert self.client.execute_command('EXISTS key') == 1
            self.waitForReplicaToSyncUp(self.replicas[0])
            assert self.replicas[0].client.execute_command('EXISTS key') == 1
            self.validate_cmd_stats(prefix, 'BF.INSERT', 1, 1)

            # New item added to an existing bloom is replicated.
            item_add_cmd = test_case[2]
            expected_calls = test_case[3]
            self.client.execute_command(item_add_cmd)
            assert self.client.execute_command('BF.EXISTS key item1') == 1
            self.waitForReplicaToSyncUp(self.replicas[0])
            assert self.replicas[0].client.execute_command('BF.EXISTS key item1') == 1
            # Validate that item addition (not bloom creation) is using the original command
            if prefix != 'BF.RESERVE':
                self.validate_cmd_stats(prefix, prefix, 2, expected_calls)
            else:
                # In case of the BF.RESERVE test case, we use BF.ADD to add items. Validate this is replicated.
                self.validate_reserve_cmd_stats(1, 1, 1, 1)
            # Attempting to add an existing item to an existing bloom will NOT replicated.
            self.client.execute_command(item_add_cmd)
            self.waitForReplicaToSyncUp(self.replicas[0])
            primary_cmd_stats = self.client.info("Commandstats")
            replica_cmd_stats = self.replicas[0].client.info("Commandstats")
            if prefix != 'BF.RESERVE':
                self.validate_cmd_stats(prefix, prefix, 3, expected_calls)
            else:
                # In case of the BF.RESERVE test case, we use BF.ADD to add items. Validate this is not replicated since
                # the item already exists.
                self.validate_reserve_cmd_stats(1, 2, 1, 1)

            # cmd debug digest
            server_digest_primary = self.client.debug_digest()
            assert server_digest_primary != None or 0000000000000000000000000000000000000000
            server_digest_replica = self.client.debug_digest()
            assert server_digest_primary == server_digest_replica
            object_digest_primary = self.client.execute_command('DEBUG DIGEST-VALUE key')
            debug_digest_replica = self.replicas[0].client.execute_command('DEBUG DIGEST-VALUE key')
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

    def test_deterministic_replication(self):
        self.setup_replication(num_replicas=1)
        # Set non default global properties (config) on the primary node. Any bloom creation on the primary should be
        # replicated with the properties below.
        assert self.client.execute_command('CONFIG SET bf.bloom-capacity 1000') == b'OK'
        assert self.client.execute_command('CONFIG SET bf.bloom-expansion 3') == b'OK'
        assert self.client.execute_command('CONFIG SET bf.bloom-fp-rate 0.1') == b'OK'
        assert self.client.execute_command('CONFIG SET bf.bloom-tightening-ratio 0.75') == b'OK'
        # Test bloom object creation with every command type.
        bloom_write_cmds = [
            ('BF.ADD', 'BF.ADD key item'),
            ('BF.MADD', 'BF.MADD key item'),
            ('BF.RESERVE', 'BF.RESERVE key 0.001 100000'),
            ('BF.INSERT', 'BF.INSERT key items item'),
        ]
        for test_case in bloom_write_cmds:
            prefix = test_case[0]
            create_cmd = test_case[1]
            self.client.execute_command(create_cmd)
            server_digest_primary = self.client.debug_digest()
            assert server_digest_primary != None or 0000000000000000000000000000000000000000
            server_digest_replica = self.client.debug_digest()
            object_digest_primary = self.client.execute_command('DEBUG DIGEST-VALUE key')
            debug_digest_replica = self.replicas[0].client.execute_command('DEBUG DIGEST-VALUE key')
            assert server_digest_primary == server_digest_replica
            assert object_digest_primary == debug_digest_replica
            self.client.execute_command('FLUSHALL')
            self.waitForReplicaToSyncUp(self.replicas[0])
            assert self.replicas[0].client.execute_command('CONFIG GET bf.bloom-capacity')[1] == b'100'
            assert self.replicas[0].client.execute_command('CONFIG GET bf.bloom-expansion')[1] == b'2'
            assert self.replicas[0].client.execute_command('CONFIG GET bf.bloom-fp-rate')[1] == b'0.01'
            assert self.replicas[0].client.execute_command('CONFIG GET bf.bloom-tightening-ratio')[1] == b'0.5'
