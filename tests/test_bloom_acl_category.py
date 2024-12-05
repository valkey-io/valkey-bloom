import pytest
from valkeytests.conftest import resource_port_tracker
from valkey_bloom_test_case import ValkeyBloomTestCaseBase

class TestBloomACLCategory(ValkeyBloomTestCaseBase):

    def test_bloom_acl_category(self):
        # List of bloom commands and the expected returns if the command is valid
        bloom_commands = [
            ('BF.ADD add_key item', 1),
            ('BF.EXISTS add_key item', 1),
            ('BF.MADD madd_key item1 item2 item3', [1, 1, 1]),
            ('BF.MEXISTS madd_key item2 item3 item4', [1, 1, 0]),
            ('BF.INSERT insert_key ITEMS item', [1]),
            ('BF.INFO insert_key filters', 1),
            ('BF.CARD madd_key', 3),
            ('BF.RESERVE reserve_key 0.01 1000', b'OK'),
        ]
        client = self.server.get_new_client()
        # Get a list of all commands with the acl category bloom
        list_of_bloom_commands = client.execute_command("COMMAND LIST FILTERBY ACLCAT bloom")

        # Create two users one with denied access to bloom commands and one with access to bloom commands and all keys
        client.execute_command("ACL SETUSER nonbloomuser on >bloom_pass -@bloom")
        client.execute_command("ACL SETUSER bloomuser on >bloom_pass +@bloom ~*")

        # Switch to the user with no bloom command access and check error occurs as expected
        client.execute_command("AUTH nonbloomuser bloom_pass")
        for cmd in bloom_commands:
            cmd_name = cmd[0].split()[0]
            # Check that each command we try to run appeared in the list of commands with the bloom acl category
            assert cmd_name.encode() in list_of_bloom_commands
            try:
                result = client.execute_command(cmd[0])
                assert False, f"User with no bloom category access shouldnt be able to run {cmd_name}"
            except Exception as e:
                assert str(e) == f"User nonbloomuser has no permissions to run the '{cmd_name}' command"

        # Switch to the user with bloom command access and check commands are run as expected
        client.execute_command(f"AUTH bloomuser bloom_pass")
        for cmd in bloom_commands:
            cmd_name = cmd[0].split()[0]
            try:
                result = client.execute_command(cmd[0])
                assert result == cmd[1], f"{cmd_name} should work for default user"
            except Exception as e:
                assert False, f"bloomuser should be able to execute {cmd_name}: {str(e)}"

    def test_bloom_command_acl_categories(self):
        # List of bloom commands and their acl categories
        bloom_commands = [
            ('BF.ADD', [b'write' , b'denyoom', b'module', b'fast']),
            ('BF.EXISTS', [b'readonly', b'module', b'fast']),
            ('BF.MADD', [b'write', b'denyoom', b'module', b'fast']),
            ('BF.MEXISTS', [b'readonly', b'module', b'fast']),
            ('BF.INSERT', [b'write', b'denyoom', b'module', b'fast']),
            ('BF.INFO', [b'readonly', b'module', b'fast']),
            ('BF.CARD', [b'readonly', b'module', b'fast']),
            ('BF.RESERVE', [b'write', b'denyoom', b'module', b'fast']),
        ]
        for cmd in bloom_commands:
            # Get the info of the commands and compare the acl categories
            cmd_info = self.client.execute_command(f'COMMAND INFO {cmd[0]}')
            assert cmd_info[0][2] == cmd[1]
            assert cmd_info[0][6] == [b'@bloom']
