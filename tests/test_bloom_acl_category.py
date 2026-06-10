from valkeytestframework.conftest import resource_port_tracker
from valkey_bloom_test_case import ValkeyBloomTestCaseBase

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
        self.run_acl_category_permissions_test("bloom", bloom_commands)

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
        self.verify_command_acl_categories(bloom_commands)
