import pytest
from valkeytests.valkey_test_case import ReplicationTestCase
from valkeytests.conftest import resource_port_tracker
import os

class TestBloomReplication(ReplicationTestCase):

    def get_custom_args(self):
        self.set_server_version(os.environ['SERVER_VERSION'])
        return {
            'loadmodule': os.getenv('MODULE_PATH'),
        }

    def test_replication_success(self):
        self.setup_replication(num_replicas=1)
        bf_add_result = self.client.execute_command('BF.ADD key item1')
        assert bf_add_result == 1
        bf_exists_result = self.client.execute_command('BF.EXISTS key item1')
        bf_non_added_exists_result = self.client.execute_command('BF.EXISTS key item2')
        bf_info_result = self.client.execute_command('BF.INFO key')

        self.waitForReplicaToSyncUp(self.replicas[0])
        bf_replica_exists_result = self.replicas[0].client.execute_command('BF.EXISTS key item1')
        assert bf_exists_result == bf_replica_exists_result
        bf_replica_non_added_exists_result = self.replicas[0].client.execute_command('BF.EXISTS key item2')
        assert bf_non_added_exists_result == bf_replica_non_added_exists_result
        bf_replica_info_result = self.replicas[0].client.execute_command('BF.INFO key')
        assert bf_info_result == bf_replica_info_result
