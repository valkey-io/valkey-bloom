from valkeytestframework.conftest import resource_port_tracker
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytestframework.util.waiters import *

class TestTopkACLCategory(ValkeyBloomTestCaseBase):

    def test_topk_acl_category_permissions(self):
        # List of topk commands and the expected returns if the command is valid
        topk_commands = [
            ('TOPK.RESERVE reserve_key 50', b'OK'),
            ('TOPK.ADD reserve_key item', 1),
            ('TOPK.INCRBY reserve_key item 5', 1),
            ('TOPK.QUERY reserve_key item', [1]),
            ('TOPK.COUNT reserve_key item', [6]),
            ('TOPK.LIST reserve_key', [b'item']),
            ('TOPK.INFO reserve_key', 8),
        ]
        client = self.server.get_new_client()
        # Get a list of all commands with the acl category topk
        list_of_topk_commands = client.execute_command("COMMAND LIST FILTERBY ACLCAT topk")
        # Create users with different acl permissions
        client.execute_command("ACL SETUSER nontopkuser1 on >topk_pass -@topk")
        client.execute_command("ACL SETUSER nontopkuser2 on >topk_pass -@all")
        client.execute_command("ACL SETUSER topkuser1 on >topk_pass ~* &* +@all ")
        client.execute_command("ACL SETUSER topkuser2 on >topk_pass ~* &* -@all +@topk ")
        client.execute_command("ACL SETUSER topkuser3 on >topk_pass ~* &* -@all +@write +@read ")
        client.execute_command("ACL SETUSER topkuser4 on >topk_pass ~* &* -@all +@write +@topk")
        # Switch to the users with no topk command access and check error occurs as expected
        for i in range(1, 3):
            client.execute_command(f"AUTH nontopkuser{i} topk_pass")
            for cmd in topk_commands:
                self.verify_invalid_user_permissions(client, cmd, list_of_topk_commands)
        # Switch to the users with topk command access and check commands are run as expected
        for i in range(1, 5):
            client.execute_command(f"AUTH topkuser{i} topk_pass")
            for cmd in topk_commands:
                self.verify_valid_user_permissions(client, cmd)
            self.client.execute_command('FLUSHDB')
            wait_for_equal(lambda: self.client.execute_command('DBSIZE'), 0)

    def verify_valid_user_permissions(self, client, cmd):
        cmd_name = cmd[0].split()[0]
        try:
            result = client.execute_command(cmd[0])
            # An integer expectation means the command returns an array whose
            # length we check; anything else is compared exactly.
            if isinstance(cmd[1], int):
                assert len(result) == cmd[1], f"{cmd_name} returned an unexpected number of results"
            else:
                assert result == cmd[1], f"{cmd_name} should work for default user"
        except Exception as e:
            assert False, f"topkuser should be able to execute {cmd_name}: {str(e)}"

    def verify_invalid_user_permissions(self, client, cmd, list_of_topk_commands):
        cmd_name = cmd[0].split()[0]
        # Check that each command we try to run appeared in the list of commands with the topk acl category
        assert cmd_name.encode() in list_of_topk_commands
        try:
            result = client.execute_command(cmd[0])
            assert False, f"User with no topk category access shouldnt be able to run {cmd_name}"
        except Exception as e:
            assert f"has no permissions to run the '{cmd_name}' command" in str(e)

    def test_topk_command_acl_categories(self):
        # List of topk commands and their acl categories
        topk_commands = [
            ('TOPK.RESERVE', [b'write', b'denyoom', b'module', b'fast'], [b'@write', b'@fast', b'@topk']),
            ('TOPK.ADD', [b'write', b'denyoom', b'module', b'fast'], [b'@write', b'@fast', b'@topk']),
            ('TOPK.INCRBY', [b'write', b'denyoom', b'module', b'fast'], [b'@write', b'@fast', b'@topk']),
            ('TOPK.INFO', [b'readonly', b'module', b'fast'], [b'@read', b'@fast', b'@topk']),
            ('TOPK.LIST', [b'readonly', b'module'], [b'@read', b'@topk']),
            ('TOPK.COUNT', [b'readonly', b'module', b'fast'], [b'@read', b'@fast', b'@topk']),
            ('TOPK.QUERY', [b'readonly', b'module', b'fast'], [b'@read', b'@fast', b'@topk']),
        ]
        for cmd in topk_commands:
            # Get the info of the commands and compare the acl categories
            cmd_info = self.client.execute_command(f'COMMAND INFO {cmd[0]}')
            assert cmd_info[0][2] == cmd[1]
            for category in cmd[2]:
                assert category in cmd_info[0][6]
