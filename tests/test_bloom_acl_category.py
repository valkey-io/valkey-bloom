import pytest
from valkeytests.conftest import resource_port_tracker
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from util.waiters import *

class TestBloomACLCategory(ValkeyBloomTestCaseBase):

    def test_bloom_acl_category_permissions(self):
        # List of bloom commands and the expected returns if the command is valid
        bloom_commands = [
            ('BF.ADD add_key item', 1),
            ('BF.EXISTS add_key item', 1),
            ('BF.CARD add_key', 1),
            ('BF.MADD madd_key item1 item2 item3', 3),
            ('BF.MEXISTS madd_key item2 item3 item4', 3),
            ('BF.INSERT insert_key ITEMS item', [1]),
            ('BF.INFO insert_key filters', 1),
            ('BF.RESERVE reserve_key 0.01 1000', b'OK'),
        ]
        client = self.server.get_new_client()
        # Get a list of all commands with the acl category bloom
        list_of_bloom_commands = client.execute_command("COMMAND LIST FILTERBY ACLCAT bloom")
        # Create users with differnt acl permissions
        client.execute_command("ACL SETUSER nonbloomuser1 on >bloom_pass -@bloom")
        client.execute_command("ACL SETUSER nonbloomuser2 on >bloom_pass -@all")
        client.execute_command("ACL SETUSER bloomuser1 on >bloom_pass ~* &* +@all ")
        client.execute_command("ACL SETUSER bloomuser2 on >bloom_pass ~* &* -@all +@bloom ")
        client.execute_command("ACL SETUSER bloomuser3 on >bloom_pass ~* &* -@all +@write +@read ")
        client.execute_command("ACL SETUSER bloomuser4 on >bloom_pass ~* &* -@all +@write +@bloom")
        # Switch to the users with no bloom command access and check error occurs as expected
        for i in range(1, 3):
            client.execute_command(f"AUTH nonbloomuser{i} bloom_pass")
            for cmd in bloom_commands:
                self.verify_invalid_user_permissions(client, cmd, list_of_bloom_commands)
        # Switch to the users with bloom command access and check commands are run as expected
        for i in range(1, 5):
            client.execute_command(f"AUTH bloomuser{i} bloom_pass")
            for cmd in bloom_commands:
                self.verify_valid_user_permissions(client, cmd)
            self.client.execute_command('FLUSHDB')
            wait_for_equal(lambda: self.client.execute_command('DBSIZE'), 0)

    def verify_valid_user_permissions(self, client, cmd):
        cmd_name = cmd[0].split()[0]
        try:
            result = client.execute_command(cmd[0])
            if cmd[0].startswith("BF.M"):
                assert len(result) == cmd[1]
                # The first add in a new bloom object should always return 1. For MEXISTS the first item we check will have been added as well so should exist
                assert result[0] == 1
            else:
                assert result == cmd[1], f"{cmd_name} should work for default user"
        except Exception as e:
            assert False, f"bloomuser should be able to execute {cmd_name}: {str(e)}"

    def verify_invalid_user_permissions(self, client, cmd, list_of_bloom_commands):
        cmd_name = cmd[0].split()[0]
        # Check that each command we try to run appeared in the list of commands with the bloom acl category
        assert cmd_name.encode() in list_of_bloom_commands
        try:
            result = client.execute_command(cmd[0])
            assert False, f"User with no bloom category access shouldnt be able to run {cmd_name}"
        except Exception as e:
            assert f"has no permissions to run the '{cmd_name}' command" in str(e)

    def test_bloom_command_acl_categories(self):
        # List of bloom commands and their acl categories
        bloom_commands = [
            ('BF.ADD', [b'write' , b'denyoom', b'module', b'fast'], [b'@write', b'@fast', b'@bloom']),
            ('BF.EXISTS', [b'readonly', b'module', b'fast'], [b'@read', b'@fast', b'@bloom']),
            ('BF.MADD', [b'write', b'denyoom', b'module', b'fast'], [b'@write', b'@fast', b'@bloom']),
            ('BF.MEXISTS', [b'readonly', b'module', b'fast'], [b'@read', b'@fast', b'@bloom']),
            ('BF.INSERT', [b'write', b'denyoom', b'module', b'fast'], [b'@write', b'@fast', b'@bloom']),
            ('BF.INFO', [b'readonly', b'module', b'fast'], [b'@read', b'@fast', b'@bloom']),
            ('BF.CARD', [b'readonly', b'module', b'fast'], [b'@read', b'@fast', b'@bloom']),
            ('BF.RESERVE', [b'write', b'denyoom', b'module', b'fast'], [b'@write', b'@fast', b'@bloom']),
            ('BF.LOAD', [b'write', b'denyoom', b'module'], [b'@write', b'@bloom']),
        ]
        for cmd in bloom_commands:
            # Get the info of the commands and compare the acl categories
            cmd_info = self.client.execute_command(f'COMMAND INFO {cmd[0]}')
            assert cmd_info[0][2] == cmd[1]
            for category in cmd[2]:
                assert category in cmd_info[0][6]
