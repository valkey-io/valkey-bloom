from valkeytestframework.conftest import resource_port_tracker
from valkey_bloom_test_case import ValkeyBloomTestCaseBase

class TestTopkACLCategory(ValkeyBloomTestCaseBase):

    def test_topk_acl_category_permissions(self):
        # List of topk commands and the expected returns if the command is valid.
        topk_commands = [
            ('TOPK.RESERVE reserve_key 50', b'OK'),
            ('TOPK.ADD reserve_key item', 1),
            ('TOPK.INCRBY reserve_key item 5', 1),
            ('TOPK.QUERY reserve_key item', [1]),
            ('TOPK.COUNT reserve_key item', [6]),
            ('TOPK.LIST reserve_key', [b'item']),
            ('TOPK.INFO reserve_key', 8),
        ]
        self.run_acl_category_permissions_test("topk", topk_commands)

    def verify_valid_user_permissions(self, client, cmd):
        cmd_name = cmd[0].split()[0]
        try:
            result = client.execute_command(cmd[0])
            if isinstance(cmd[1], int):
                assert len(result) == cmd[1], f"{cmd_name} returned an unexpected number of results"
            else:
                assert result == cmd[1], f"{cmd_name} should work for default user"
        except Exception as e:
            assert False, f"topkuser should be able to execute {cmd_name}: {str(e)}"

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
        self.verify_command_acl_categories(topk_commands)
