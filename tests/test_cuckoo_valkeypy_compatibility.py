from cuckoo_test_utils import CuckooTestCase


class TestValkeyCuckooCompatibility(CuckooTestCase):
    def test_client_commands_and_deleted_count(self):
        client = self.server.get_new_client()
        raw = self.server.get_new_client()
        cf = client.cf()
        assert cf.reserve('cf', 1000)
        assert cf.info('cf').get('deletedNum') == 0
        assert cf.add('cf', 'item') == 1
        assert cf.addnx('cf', 'item') == 0
        assert cf.insert('cf', ['item', 'another']) == [1, 1]
        assert cf.count('cf', 'item') == 2
        assert cf.delete('cf', 'item') == 1
        assert cf.count('cf', 'item') == 1
        assert cf.info('cf').get('deletedNum') == 1
        assert raw.execute_command('CF.INFO', 'cf', 'Number of items deleted') == 1
        assert cf.delete('cf', 'item') == 1
        assert cf.delete('cf', 'item') == 0
        assert cf.info('cf').get('deletedNum') == 2
        assert cf.info('cf').get('insertedNum') == 1
        assert raw.copy('cf', 'copy')
        assert cf.info('copy').get('deletedNum') == 2
        assert raw.dump('cf') == raw.dump('copy')
