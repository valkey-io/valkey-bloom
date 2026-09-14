import pytest, os
from valkey import ResponseError
from valkeytestframework.valkey_test_case import ReplicationTestCase
from valkeytestframework.conftest import resource_port_tracker

class TestCMSReplication(ReplicationTestCase):

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        use_external = os.environ.get("VALKEY_EXTERNAL_SERVER", "false").lower() == "true"
        
        if use_external:
            master_host = os.environ.get("VALKEY_HOST", "localhost")
            master_port = int(os.environ.get("VALKEY_PORT", "6379"))
            self.server, self.client = self.create_server(
                testdir=self.testdir,
                bind_ip=master_host,
                port=master_port,
                external_server=True
            )
            
            replica_host = os.environ.get("VALKEY_REPLICA_HOST", "localhost")
            replica_port = int(os.environ.get("VALKEY_REPLICA_PORT", "6380"))
            replica_server, replica_client = self.create_server(
                testdir=self.testdir,
                bind_ip=replica_host,
                port=replica_port,
                external_server=True
            )
            
            self.replicas = [replica_server]
            self.num_replicas = 1
        else:
            self.args = {"enable-debug-command":"yes", 'loadmodule': os.getenv('MODULE_PATH')}
            server_path = f"{os.path.dirname(os.path.realpath(__file__))}/build/binaries/{os.environ['SERVER_VERSION']}/valkey-server"
            self.server, self.client = self.create_server(testdir = self.testdir,  server_path=server_path, args=self.args)


            
    def validate_cmd_stats(self, primary_cmd, replica_cmd, expected_primary_calls, expected_replica_calls):
        """
            Helper fn to validate cmd count on primary & replica.
        """
        primary_cmd_stats = self.client.info("Commandstats")['cmdstat_' + primary_cmd]
        assert primary_cmd_stats["calls"] == expected_primary_calls
        replica_cmd_stats = self.replicas[0].client.info("Commandstats")['cmdstat_' + replica_cmd]
        assert replica_cmd_stats["calls"] == expected_replica_calls


    def test_replication_behavior(self):
        use_external = os.environ.get("VALKEY_EXTERNAL_SERVER", "false").lower() == "true"
        if use_external:
            self.wait_for_primary_link_up_all_replicas()
        else:
            self.setup_replication(num_replicas=1)

        # Test replication for write commands.
        # Note Merge is missing here for write commands
        cms_write_cmds = [
            ('CMS.INITBYDIM', 'CMS.INITBYDIM key 10 5', 'CMS.INCRBY key item1 1', 1),
            ('CMS.INITBYPROB', 'CMS.INITBYPROB key  0.001 0.01', 'CMS.INCRBY key item1 1', 1),
        ]
        for test_case in cms_write_cmds:
            prefix = test_case[0]
            create_cmd = test_case[1]
            # New cms object being created is replicated.
            self.client.execute_command(create_cmd)
            assert self.client.execute_command('EXISTS key') == 1
            self.waitForReplicaToSyncUp(self.replicas[0])
            assert self.replicas[0].client.execute_command('EXISTS key') == 1
            self.validate_cmd_stats(prefix, prefix, 1, 1)

            # New item added to an existing bloom is replicated.
            item_add_cmd = test_case[2]
            expected_calls = test_case[3]
            self.client.execute_command(item_add_cmd)
            assert self.client.execute_command('CMS.QUERY key item1') == [1]
            self.waitForReplicaToSyncUp(self.replicas[0])
            assert self.replicas[0].client.execute_command('CMS.QUERY key item1') == [1]

            # cmd debug digest
            # TO BE IMPLEMENTED ONCE DIGEST IS IMPLEMENTED

            self.client.execute_command('FLUSHALL')
            self.waitForReplicaToSyncUp(self.replicas[0])
            self.client.execute_command('CONFIG RESETSTAT')
            self.replicas[0].client.execute_command('CONFIG RESETSTAT')

        # Re-setup for read commands
        self.client.execute_command('CMS.INITBYDIM key 10 5')
        self.client.execute_command('CMS.INCRBY key item1 1')
        self.waitForReplicaToSyncUp(self.replicas[0])

        # Read commands executed on the primary will not be replicated.
        read_commands = [
            ('CMS.QUERY', 'CMS.QUERY key item1', 1),
            ('CMS.INFO', 'CMS.INFO key', 1),
            ('CMS.INFO', 'CMS.INFO key WIDTH', 2),
            ('CMS.INFO', 'CMS.INFO key DEPTH', 3),
            ('CMS.INFO', 'CMS.INFO key COUNT', 4),
        ]
        for test_case in read_commands:
            prefix = test_case[0]
            cmd = test_case[1]
            expected_primary_calls = test_case[2]
            self.client.execute_command(cmd)
            primary_cmd_stats = self.client.info("Commandstats")['cmdstat_' + prefix]
            assert primary_cmd_stats["calls"] == expected_primary_calls
            assert ('cmdstat_' + prefix) not in self.replicas[0].client.info("Commandstats")

        # Reset the stats for the invalid write commands testing below
        self.client.execute_command('FLUSHALL')
        self.waitForReplicaToSyncUp(self.replicas[0])
        self.client.execute_command('CONFIG RESETSTAT')
        self.replicas[0].client.execute_command('CONFIG RESETSTAT')


        # Write commands with errors are not replicated. (prefix, command)
        invalid_write_cmds = [
            ('CMS.INITBYDIM', 'CMS.INITBYDIM key 10 5 5'),
            ('CMS.INITBYPROB', 'CMS.INITBYPROB key 0 0.1'),
        ]
        for test_case in invalid_write_cmds:
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


