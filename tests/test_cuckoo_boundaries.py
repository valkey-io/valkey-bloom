import struct

import pytest
from valkey import ResponseError

from cuckoo_test_utils import CuckooTestCase


def snapshot(filters, bucket_size=1, expansion=1):
    data = bytes([1]) + struct.pack('<5Q', expansion, bucket_size, 20, len(filters), 0)
    for capacity, buckets in filters:
        occupied = sum(value != 100 for value in buckets)
        data += struct.pack('<5Q', capacity, occupied, 0, 0, len(buckets)) + buckets
    return data


class TestCuckooBoundaries(CuckooTestCase):
    @pytest.mark.parametrize('expansion', [0, 32768, -1, 32769])
    def test_expansion_bounds(self, expansion):
        client = self.server.get_new_client()
        if 0 <= expansion <= 32768:
            assert client.execute_command('CF.RESERVE', 'filter', 1, 'EXPANSION', expansion) == b'OK'
            assert client.execute_command('CF.INFO', 'filter', 'Expansion rate') == expansion
        else:
            with pytest.raises(ResponseError, match='^bad expansion$'):
                client.execute_command('CF.RESERVE', 'filter', 1, 'EXPANSION', expansion)
            assert not client.exists('filter')

    @pytest.mark.parametrize('command,args', [
        ('ADD', ['item']), ('ADDNX', ['item']), ('DEL', ['item']),
        ('COUNT', ['item']), ('EXISTS', ['item']), ('MEXISTS', ['item']),
        ('INFO', []), ('RESERVE', [64]), ('INSERT', ['ITEMS', 'item']),
        ('INSERTNX', ['ITEMS', 'item']), ('LOAD', [snapshot([(1, b'd')])]),
    ])
    def test_wrongtype(self, command, args):
        client = self.server.get_new_client()
        client.set('string', 'unchanged')
        with pytest.raises(ResponseError, match='^WRONGTYPE'):
            client.execute_command('CF.' + command, 'string', *args)
        assert client.get('string') == b'unchanged'

    @pytest.mark.parametrize('name,minimum,maximum', [
        ('capacity', 1, 2**32), ('bucket-size', 1, 255),
        ('max-kicks', 1, 65535), ('expansion', 0, 32768),
    ])
    def test_config_bounds(self, name, minimum, maximum):
        client = self.server.get_new_client()
        name = 'bf.cuckoo-' + name
        original = client.config_get(name)[name]
        try:
            for valid in [minimum, maximum]:
                assert client.config_set(name, valid)
                assert int(client.config_get(name)[name]) == valid
                for invalid in [minimum - 1, maximum + 1]:
                    with pytest.raises(ResponseError):
                        client.config_set(name, invalid)
                    assert int(client.config_get(name)[name]) == valid
        finally:
            client.config_set(name, original)

    @pytest.mark.parametrize('item', [b'', b'a\x00b\xff', b'x' * (2 * 1024 * 1024)],
                             ids=['empty', 'binary', 'large'])
    def test_binary_items(self, item):
        client = self.server.get_new_client()
        assert client.execute_command('CF.ADD', 'filter', item) == 1
        assert client.execute_command('CF.INSERT', 'filter', 'ITEMS', item) == [1]
        assert client.execute_command('CF.COUNT', 'filter', item) == 2
        assert client.execute_command('CF.MEXISTS', 'filter', item) == [1]
        assert client.execute_command('CF.ADDNX', 'filter', item) == 0
        assert client.execute_command('CF.DEL', 'filter', item) == 1
        assert client.execute_command('CF.COUNT', 'filter', item) == 1
        assert client.execute_command('CF.DEL', 'filter', item) == 1
        assert client.execute_command('CF.DEL', 'filter', item) == 0
        assert client.execute_command('CF.EXISTS', 'filter', item) == 0

    @pytest.mark.parametrize('family', ['nul', 'large'])
    def test_binary_items_remain_distinct(self, family):
        # Fixed fixtures verified with one bucket: their fingerprints differ, so
        # exact counts below do not assume collision-free arbitrary inputs.
        if family == 'nul':
            items = [b'prefix', b'prefix\x00left', b'prefix\x00right', b'prefix\x00\xff']
        else:
            prefix = b'x' * 1024
            body = prefix + b'y' * (2 * 1024 * 1024 - len(prefix) - 1)
            items = [prefix, body + b'A', body + b'B', body + b'C']
        client = self.server.get_new_client()
        client.execute_command('CF.RESERVE', 'distinct', 1, 'BUCKETSIZE', 255)
        assert client.execute_command('CF.INFO', 'distinct', 'Number of buckets') == 1
        counts = [0] * len(items)

        def check_counts():
            assert [client.execute_command('CF.COUNT', 'distinct', item)
                    for item in items] == counts, 'distinct byte strings lost independent counts'
            assert client.execute_command('CF.MEXISTS', 'distinct', *items) == [
                int(count > 0) for count in counts]
            assert [client.execute_command('CF.EXISTS', 'distinct', item)
                    for item in items] == [int(count > 0) for count in counts]

        check_counts()
        for index, item in enumerate(items):
            assert client.execute_command('CF.ADDNX', 'distinct', item) == 1
            counts[index] = 1
            check_counts()
            assert client.execute_command('CF.DEL', 'distinct', item) == 1
            counts[index] = 0
            check_counts()
            assert client.execute_command('CF.INSERTNX', 'distinct', 'ITEMS', item) == [1]
            assert client.execute_command('CF.ADD', 'distinct', item) == 1
            assert client.execute_command('CF.INSERT', 'distinct', 'ITEMS',
                                          *([item] * (index + 1))) == [1] * (index + 1)
            counts[index] = index + 3
            assert client.execute_command('CF.ADDNX', 'distinct', item) == 0
            assert client.execute_command('CF.INSERTNX', 'distinct', 'ITEMS', item) == [0]
            check_counts()

        for index, item in enumerate(items):
            while counts[index]:
                assert client.execute_command('CF.DEL', 'distinct', item) == 1
                counts[index] -= 1
                check_counts()
            assert client.execute_command('CF.DEL', 'distinct', item) == 0

    @pytest.mark.parametrize('command', ['CF.INSERT', 'CF.INSERTNX'])
    @pytest.mark.parametrize('option', ['CAPACITY', 'BUCKETSIZE', 'MAXITERATIONS'])
    def test_insert_missing_option_value(self, command, option):
        client = self.server.get_new_client()
        with pytest.raises(ResponseError, match=f'^{option} requires an argument$'):
            client.execute_command(command, 'filter', 'NOCREATE', option)
        assert not client.exists('filter')

    @pytest.mark.parametrize('command', ['CF.INSERT', 'CF.INSERTNX'])
    def test_insert_rejects_expansion(self, command):
        client = self.server.get_new_client()
        with pytest.raises(ResponseError, match='^unknown option or missing ITEMS keyword$'):
            client.execute_command(command, 'filter', 'EXPANSION', 2, 'ITEMS', 'item')
        assert not client.exists('filter')

    @pytest.mark.parametrize('data,error', [
        (snapshot([(131073, bytes([7]) * (1024 * 255))], bucket_size=255, expansion=32768),
         'cuckoo object reached max capacity'),
        (snapshot([(1, b'\x07') for _ in range(1024)]),
         'cuckoo object reached max number of filters'),
    ], ids=['capacity-overflow', 'filter-count-limit'])
    def test_scaling_limits_preserve_state(self, data, error):
        client = self.server.get_new_client()
        assert client.execute_command('CF.LOAD', 'filter', data) == b'OK'
        before = client.dump('filter')
        digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'filter')
        with pytest.raises(ResponseError, match=f'^{error}$'):
            client.execute_command('CF.ADD', 'filter', 'item')
        assert client.dump('filter') == before
        assert client.execute_command('DEBUG', 'DIGEST-VALUE', 'filter') == digest

    def test_delete_reopens_old_filter_before_scaling(self):
        client = self.server.get_new_client()
        client.execute_command('CF.LOAD', 'probe', snapshot([(1, b'\x07')]))
        candidates = [f'item{i}' for i in range(4096)]
        matches = client.execute_command('CF.MEXISTS', 'probe', *candidates)
        item = candidates[matches.index(1)]
        client.delete('probe')
        client.execute_command('CF.LOAD', 'filter', snapshot([(1, b'\x07'), (1, b'\x08')]))
        assert client.execute_command('CF.DEL', 'filter', item) == 1
        assert client.execute_command('CF.ADD', 'filter', item) == 1
        assert client.execute_command('CF.INFO', 'filter', 'Number of filters') == 2
        assert client.execute_command('CF.COUNT', 'filter', item) == 1

    def test_colliding_fingerprints_are_counted_and_deleted(self):
        client = self.server.get_new_client()
        client.execute_command('CF.RESERVE', 'filter', 1, 'BUCKETSIZE', 4)
        client.execute_command('CF.ADD', 'filter', 'original')
        candidates = [f'collision{i}' for i in range(4096)]
        matches = client.execute_command('CF.MEXISTS', 'filter', *candidates)
        collision = candidates[matches.index(1)]
        assert client.execute_command('CF.ADD', 'filter', collision) == 1
        assert client.execute_command('CF.COUNT', 'filter', 'original') == 2
        assert client.execute_command('CF.DEL', 'filter', collision) == 1
        assert client.execute_command('CF.COUNT', 'filter', 'original') == 1
        assert client.execute_command('CF.DEL', 'filter', 'original') == 1
        assert client.execute_command('CF.COUNT', 'filter', collision) == 0
