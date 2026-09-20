from cuckoo_test_utils import CuckooTestCase


METRICS = (
    'num_objects', 'total_memory_bytes', 'num_filters_across_objects',
    'num_items_across_objects', 'capacity_across_objects',
    'defrag_hits', 'defrag_misses',
)


def metrics(client):
    info = client.info('modules')
    return {name: int(info['bf_cuckoo_' + name]) for name in METRICS}


def filter_info(client, key):
    reply = client.execute_command('CF.INFO', key)
    return dict(zip(reply[::2], reply[1::2]))


class TestCuckooMetrics(CuckooTestCase):

    def test_initial_metrics(self):
        assert metrics(self.server.get_new_client()) == dict.fromkeys(METRICS, 0)

    def test_create_and_delete_metrics(self):
        client = self.server.get_new_client()
        for key, capacity in [('first', 1000), ('second', 500), ('third', 250)]:
            assert client.execute_command('CF.RESERVE', key, capacity) == b'OK'
        current = metrics(client)
        assert current['num_objects'] == current['num_filters_across_objects'] == 3
        assert current['capacity_across_objects'] == 1750
        assert current['num_items_across_objects'] == 0
        assert current['total_memory_bytes'] == sum(
            filter_info(client, key)[b'Size'] for key in ['first', 'second', 'third'])
        removed_size = filter_info(client, 'second')[b'Size']
        assert client.delete('second') == 1
        after = metrics(client)
        assert after['num_objects'] == after['num_filters_across_objects'] == 2
        assert after['capacity_across_objects'] == 1250
        assert after['total_memory_bytes'] == current['total_memory_bytes'] - removed_size
        assert client.delete('first', 'third') == 2
        assert metrics(client) == dict.fromkeys(METRICS, 0)

    def test_insert_duplicate_and_delete_metrics(self):
        client = self.server.get_new_client()
        client.execute_command('CF.RESERVE', 'items', 1000)
        reserved_memory = metrics(client)['total_memory_bytes']
        inserted = sum(client.execute_command('CF.ADDNX', 'items', f'item{i}')
                       for i in range(100))
        assert inserted > 0
        assert metrics(client)['num_items_across_objects'] == inserted
        assert client.execute_command('CF.ADD', 'items', 'item0') == 1
        assert metrics(client)['num_items_across_objects'] == inserted + 1
        assert metrics(client)['total_memory_bytes'] == reserved_memory
        assert client.execute_command('CF.DEL', 'items', 'item0') == 1
        assert metrics(client)['num_items_across_objects'] == inserted
        assert client.delete('items') == 1
        assert metrics(client) == dict.fromkeys(METRICS, 0)

    def test_scaling_metrics(self):
        client = self.server.get_new_client()
        client.execute_command('CF.RESERVE', 'scaled', 4, 'EXPANSION', 2)
        before = metrics(client)
        inserted = sum(client.execute_command('CF.ADDNX', 'scaled', f'item{i}')
                       for i in range(100))
        info = filter_info(client, 'scaled')
        count = info[b'Number of filters']
        assert count > 1
        current = metrics(client)
        assert current['num_objects'] == 1
        assert current['num_filters_across_objects'] == count
        assert current['num_items_across_objects'] == inserted
        assert current['capacity_across_objects'] == 4 * (2 ** count - 1)
        assert current['total_memory_bytes'] == info[b'Size']
        assert current['total_memory_bytes'] > before['total_memory_bytes']
        assert client.delete('scaled') == 1
        assert metrics(client) == dict.fromkeys(METRICS, 0)

    def test_metrics_after_rdb_restart(self):
        client = self.server.get_new_client()
        client.execute_command('CF.RESERVE', 'persisted', 1000)
        client.execute_command('CF.ADD', 'persisted', 'item')
        before = metrics(client)
        assert before['num_objects'] == before['num_items_across_objects'] == 1
        assert client.execute_command('SAVE')
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        client = self.server.get_new_client()
        assert metrics(client) == before
        assert client.execute_command('CF.EXISTS', 'persisted', 'item') == 1
        assert client.delete('persisted') == 1
        assert client.execute_command('SAVE')
        self.server.restart(remove_rdb=False, remove_nodes_conf=False, connect_client=True)
        assert metrics(self.server.get_new_client()) == dict.fromkeys(METRICS, 0)
