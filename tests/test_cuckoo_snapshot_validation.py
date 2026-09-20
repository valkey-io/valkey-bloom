import struct

import pytest
from valkey import ResponseError

from cuckoo_test_utils import CuckooTestCase


def empty_snapshot(capacities):
    snapshot = bytes([1]) + struct.pack('<5Q', 2, 4, 20, len(capacities), 0)
    for capacity in capacities:
        snapshot += struct.pack('<5Q', capacity, 0, 0, 0, capacity) + bytes([100]) * capacity
    return snapshot


def rdb_length(value):
    if value < 64:
        return bytes([value])
    if value < 16384:
        return struct.pack('>H', value | 0x4000)
    if value <= 0xffffffff:
        return b'\x80' + struct.pack('>I', value)
    return b'\x81' + struct.pack('>Q', value)


def rdb_unsigned_fields(*fields):
    return b''.join(b'\x02' + rdb_length(value) for value in fields)


def rdb_bucket_chunk(data):
    return b'\x05' + rdb_length(len(data)) + data


class TestCuckooSnapshotValidation(CuckooTestCase):
    @pytest.mark.parametrize('key_type,error', [('cuckoo', '^item exists$'), ('string', '^WRONGTYPE')])
    def test_load_checks_existing_key_before_decoding(self, key_type, error):
        client = self.server.get_new_client()
        if key_type == 'cuckoo':
            client.execute_command('CF.ADD', 'existing', 'saved')
        else:
            client.set('existing', 'saved')
        before = client.dump('existing')
        memory = client.memory_usage('existing')
        metrics = client.info('modules')
        client.config_set('bf.cuckoo-memory-usage-limit', 1024)
        # Invalid and over-limit payloads would fail decoding. The destination
        # error must win, proving the snapshot decoder was not entered.
        for payload in [b'invalid', empty_snapshot([1048576])]:
            with pytest.raises(ResponseError, match=error):
                client.execute_command('CF.LOAD', 'existing', payload)
            assert client.dump('existing') == before
            assert client.memory_usage('existing') == memory
            after = client.info('modules')
            for name, value in metrics.items():
                if name.startswith('bf_cuckoo_') and 'defrag' not in name:
                    assert after[name] == value, name

    def test_invalid_rdb_chunks_leave_server_alive_and_metrics_unchanged(self):
        client = self.server.get_new_client()
        client.execute_command('CF.RESERVE', 'template', 32)
        template = client.dump('template')
        # DUMP's module-v2 type and 64-bit type identifier, then version/checksum.
        assert template[:2] == b'\x07\x81'
        prefix, footer = template[:10], template[-10:]
        client.delete('template')
        chunk = rdb_bucket_chunk(bytes([100]) * 1048576)
        header = rdb_unsigned_fields(2, 4, 20, 1, 0)
        large = rdb_unsigned_fields(16777216, 0, 0, 0, 16777216)
        small = rdb_unsigned_fields(32, 0, 0, 0, 32)
        valid_filter = small + rdb_bucket_chunk(bytes([100]) * 32)
        invalid = [
            header + large,  # Missing first chunk.
            header + large + chunk * 2,  # Missing a later chunk.
            header + large + rdb_bucket_chunk(b'x'),
            header + large + rdb_bucket_chunk(bytes(1048577)),
            header + rdb_unsigned_fields(2**64 - 1, 0, 0, 0, 2**64 - 1),
            header + small + rdb_bucket_chunk(bytes(32)),  # Occupancy disagrees with count.
            rdb_unsigned_fields(2, 4, 20, 2, 0) + valid_filter + large,
        ]
        # Exercise module parsing rather than rejecting our edited outer CRC.
        client.execute_command('DEBUG', 'SET-SKIP-CHECKSUM-VALIDATION', 1)
        try:
            # Positive control proves that the synthetic framing reaches the loader.
            client.restore('control', 0, prefix + header + valid_filter + b'\x00' + footer)
            assert client.execute_command('CF.INFO', 'control', 'Number of items inserted') == 0
            client.delete('control')
            metrics = client.info('modules')
            for payload in invalid:
                with pytest.raises(ResponseError, match='Bad data format'):
                    client.restore('invalid', 0, prefix + payload + b'\x00' + footer)
                assert not client.exists('invalid')
                assert client.ping()
                after = client.info('modules')
                for name, value in metrics.items():
                    if name.startswith('bf_cuckoo_') and 'defrag' not in name:
                        assert after[name] == value, name
        finally:
            client.execute_command('DEBUG', 'SET-SKIP-CHECKSUM-VALIDATION', 0)

    def test_restore_ignores_local_memory_limit(self):
        client = self.server.get_new_client()
        client.execute_command('CF.RESERVE', 'source', 1000000, 'BUCKETSIZE', 5)
        client.execute_command('CF.ADD', 'source', 'saved')
        dump = client.dump('source')
        client.delete('source')
        client.config_set('bf.cuckoo-memory-usage-limit', 1024)
        client.restore('restored', 0, dump)
        assert client.dump('restored') == dump
        assert client.execute_command('CF.EXISTS', 'restored', 'saved') == 1

    @pytest.mark.parametrize('capacities', [(1048576,), (524288, 524288)])
    def test_total_limit_includes_metadata_and_vector_capacity(self, capacities):
        client = self.server.get_new_client()
        snapshot = empty_snapshot(capacities)
        assert client.execute_command('CF.LOAD', 'sizing', snapshot) == b'OK'
        size = client.execute_command('CF.INFO', 'sizing', 'Size')
        client.delete('sizing')
        metrics = client.info('modules')
        for limit in [1024, size - 1]:
            client.config_set('bf.cuckoo-memory-usage-limit', limit)
            with pytest.raises(ResponseError, match='memory limit'):
                client.execute_command('CF.LOAD', 'rejected', snapshot)
            assert not client.exists('rejected')
            after = client.info('modules')
            for name, value in metrics.items():
                if name.startswith('bf_cuckoo_') and 'defrag' not in name:
                    assert after[name] == value, name
        client.config_set('bf.cuckoo-memory-usage-limit', size)
        assert client.execute_command('CF.LOAD', 'exact', snapshot) == b'OK'
        assert client.execute_command('CF.INFO', 'exact', 'Size') == size

    def test_corrupt_and_old_snapshots_leave_no_key_or_metrics(self):
        client = self.server.get_new_client()
        snapshot = empty_snapshot([32])
        invalid = [snapshot[:end] for end in range(len(snapshot))]
        invalid += [bytes([version]) + snapshot[1:] for version in [0, 2, 3, 4, 5, 255]]
        invalid += [snapshot + b'extra']
        for offset, value in [(25, 1025), (33, 2**63), (41, 2**64 - 1),
                              (49, 1), (65, 16), (73, 1), (73, 31), (73, 2**64 - 1)]:
            invalid.append(snapshot[:offset] + struct.pack('<Q', value) + snapshot[offset + 8:])
        metrics = client.info('modules')
        for data in invalid:
            with pytest.raises(ResponseError):
                client.execute_command('CF.LOAD', 'invalid', data)
            assert not client.exists('invalid')
        after = client.info('modules')
        for name, value in metrics.items():
            if name.startswith('bf_cuckoo_') and 'defrag' not in name:
                assert after[name] == value, name

    @pytest.mark.parametrize('version', [0, 2, 3, 4, 5, 255])
    def test_restore_rejects_unsupported_format(self, version):
        client = self.server.get_new_client()
        client.execute_command('CF.RESERVE', 'source', 32)
        dump = client.dump('source')
        assert dump[:2] == b'\x07\x81'
        type_id = int.from_bytes(dump[2:10], 'big')
        assert type_id & 1023 == 1
        invalid_id = (type_id & ~1023) | version
        invalid = dump[:2] + invalid_id.to_bytes(8, 'big') + dump[10:]
        client.execute_command('DEBUG', 'SET-SKIP-CHECKSUM-VALIDATION', 1)
        try:
            # Positive control ensures the unmodified framing can be restored.
            client.restore('control', 0, dump)
            with pytest.raises(ResponseError):
                client.restore('invalid', 0, invalid)
            assert not client.exists('invalid')
            assert client.ping()
        finally:
            client.execute_command('DEBUG', 'SET-SKIP-CHECKSUM-VALIDATION', 0)
