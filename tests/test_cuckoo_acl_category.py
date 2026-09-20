import pytest
from valkey.exceptions import NoPermissionError
from cuckoo_test_utils import CuckooTestCase


READ_COMMANDS = {'cf.exists', 'cf.mexists', 'cf.count', 'cf.info'}
WRITE_COMMANDS = {'cf.add', 'cf.addnx', 'cf.del', 'cf.insert',
                  'cf.insertnx', 'cf.reserve', 'cf.load'}


def category_commands(client, category):
    return {command.decode().lower()
            for command in client.execute_command('ACL', 'CAT', category)}


class TestCuckooACLCategory(CuckooTestCase):

    def test_cuckoo_acl_category(self):
        client = self.server.get_new_client()
        assert category_commands(client, 'cuckoo') == READ_COMMANDS | WRITE_COMMANDS

    def test_acl_restrictions(self):
        client = self.server.get_new_client()
        client.execute_command('ACL', 'SETUSER', 'testuser', 'reset', 'on',
                               '>password', '~*', '&*', '+@all', '-@cuckoo')
        restricted = self.server.get_new_client()
        try:
            assert restricted.execute_command('AUTH', 'testuser', 'password') is True
            assert restricted.ping()
            with pytest.raises(NoPermissionError, match=r"permissions.*'CF\.ADD'"):
                restricted.execute_command('CF.ADD', 'myfilter', 'item1')
            assert client.exists('myfilter') == 0
            client.execute_command('ACL', 'SETUSER', 'testuser', '+@cuckoo')
            assert restricted.execute_command('CF.ADD', 'myfilter', 'item1') == 1
        finally:
            restricted.close()
            client.execute_command('ACL', 'DELUSER', 'testuser')

    def test_read_write_categorization(self):
        client = self.server.get_new_client()
        reads = category_commands(client, 'read')
        writes = category_commands(client, 'write')
        assert READ_COMMANDS <= reads
        assert WRITE_COMMANDS <= writes
        assert not READ_COMMANDS & writes
        assert not WRITE_COMMANDS & reads

    def test_load_write_permission(self):
        client = self.server.get_new_client()
        client.execute_command('ACL', 'SETUSER', 'reader', 'reset', 'on',
                               '>password', '~*', '+@read')
        restricted = self.server.get_new_client()
        try:
            assert restricted.execute_command('AUTH', 'reader', 'password') is True
            assert restricted.execute_command('CF.EXISTS', 'myfilter', 'item1') == 0
            # ACL validation must reject the command before snapshot decoding.
            with pytest.raises(NoPermissionError, match=r"permissions.*'CF\.LOAD'"):
                restricted.execute_command('CF.LOAD', 'myfilter', b'invalid snapshot')
        finally:
            restricted.close()
            client.execute_command('ACL', 'DELUSER', 'reader')

    def test_eviction_commands_are_not_fast(self):
        fast = category_commands(self.server.get_new_client(), 'fast')
        assert not {'cf.add', 'cf.addnx', 'cf.insert', 'cf.insertnx'} & fast
