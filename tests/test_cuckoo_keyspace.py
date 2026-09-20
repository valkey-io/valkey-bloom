import time
import pytest
from valkey import ResponseError
from cuckoo_test_utils import CuckooTestCase
from cuckoo_test_utils import rewrite_cuckoo_aof

class TestCuckooKeyspace(CuckooTestCase):

    @pytest.fixture(autouse=True)
    def configure_keyspace_events(self, setup_test):
        client = self.server.get_new_client()
        client.execute_command('CONFIG', 'SET', 'notify-keyspace-events', 'AKEm')

    def test_cuckoo_add_event(self):
        """Test that CF.ADD generates cuckoo.add event"""
        client = self.server.get_new_client()
        pubsub = client.pubsub()

        # Subscribe to cuckoo.add events
        pubsub.psubscribe('__keyevent@0__:cuckoo.add')
        time.sleep(0.1)

        # Trigger the event
        client.execute_command('CF.ADD', 'eventTest', 'item1')
        time.sleep(0.1)

        # Check for event
        message = pubsub.get_message()
        assert message is not None
        message = pubsub.get_message()  # Skip subscribe confirmation
        assert message is not None
        assert message['type'] == 'pmessage'
        assert message['channel'] == b'__keyevent@0__:cuckoo.add'
        assert message['data'] == b'eventTest'

        pubsub.close()

    def test_cuckoo_del_event(self):
        """Test that CF.DEL generates cuckoo.del event"""
        client = self.server.get_new_client()

        # Create filter with item
        client.execute_command('CF.ADD', 'delEventTest', 'item1')

        pubsub = client.pubsub()
        pubsub.psubscribe('__keyevent@0__:cuckoo.del')
        time.sleep(0.1)

        # Trigger delete event
        client.execute_command('CF.DEL', 'delEventTest', 'item1')
        time.sleep(0.1)

        # Check for event
        message = pubsub.get_message()  # Skip subscribe
        message = pubsub.get_message()
        assert message is not None
        assert message['type'] == 'pmessage'
        assert message['channel'] == b'__keyevent@0__:cuckoo.del'

        pubsub.close()

    def test_cuckoo_reserve_event(self):
        """Test that CF.RESERVE generates cuckoo.reserve event"""
        client = self.server.get_new_client()
        pubsub = client.pubsub()

        pubsub.psubscribe('__keyevent@0__:cuckoo.reserve')
        time.sleep(0.1)

        # Trigger reserve event
        client.execute_command('CF.RESERVE', 'reserveTest', 1000)
        time.sleep(0.1)

        # Check for event
        message = pubsub.get_message()  # Skip subscribe
        message = pubsub.get_message()
        assert message is not None
        assert message['type'] == 'pmessage'
        assert message['channel'] == b'__keyevent@0__:cuckoo.reserve'
        assert message['data'] == b'reserveTest'

        pubsub.close()

    def test_cuckoo_insert_event(self):
        """Test that CF.INSERT generates cuckoo.insert event"""
        client = self.server.get_new_client()
        pubsub = client.pubsub()

        pubsub.psubscribe('__keyevent@0__:cuckoo.insert')
        time.sleep(0.1)

        # Trigger insert event
        client.execute_command('CF.INSERT', 'insertTest', 'ITEMS', 'val1', 'val2')
        time.sleep(0.1)

        # Check for event
        message = pubsub.get_message()  # Skip subscribe
        message = pubsub.get_message()
        assert message is not None
        assert message['type'] == 'pmessage'
        assert message['channel'] == b'__keyevent@0__:cuckoo.insert'

        pubsub.close()

    def test_cuckoo_create_event(self):
        """Test that auto-creating filter generates cuckoo.create event"""
        client = self.server.get_new_client()
        pubsub = client.pubsub()

        pubsub.psubscribe('__keyevent@0__:cuckoo.create')
        time.sleep(0.1)

        # Auto-create filter with CF.ADD
        client.execute_command('CF.ADD', 'autoCreate', 'item1')
        time.sleep(0.1)

        # Check for event
        message = pubsub.get_message()  # Skip subscribe
        message = pubsub.get_message()
        assert message is not None
        assert message['type'] == 'pmessage'
        assert message['channel'] == b'__keyevent@0__:cuckoo.create'
        assert message['data'] == b'autoCreate'

        pubsub.close()

    def test_multiple_events_same_key(self):
        """Test multiple operations on same key generate separate events"""
        client = self.server.get_new_client()
        pubsub = client.pubsub()

        # Subscribe to all cuckoo events
        pubsub.psubscribe('__keyevent@0__:cuckoo.*')
        time.sleep(0.1)

        # Perform multiple operations
        client.execute_command('CF.RESERVE', 'multiTest', 1000)
        time.sleep(0.1)
        client.execute_command('CF.ADD', 'multiTest', 'item1')
        time.sleep(0.1)
        client.execute_command('CF.DEL', 'multiTest', 'item1')
        time.sleep(0.2)

        # Collect all events
        events = []
        message = pubsub.get_message()  # Skip subscribe
        while True:
            message = pubsub.get_message()
            if message is None:
                break
            if message['type'] == 'pmessage':
                events.append(message['channel'])
            time.sleep(0.01)

        # Should have received reserve, add, and del events
        assert b'__keyevent@0__:cuckoo.reserve' in events
        assert b'__keyevent@0__:cuckoo.add' in events
        assert b'__keyevent@0__:cuckoo.del' in events

        pubsub.close()

    def test_no_event_for_read_operations(self):
        """Test that read operations don't generate events"""
        client = self.server.get_new_client()

        # Create filter
        client.execute_command('CF.ADD', 'readTest', 'item1')

        pubsub = client.pubsub()
        pubsub.psubscribe('__keyevent@0__:cuckoo.*')
        time.sleep(0.1)

        # Perform read operations
        client.execute_command('CF.EXISTS', 'readTest', 'item1')
        client.execute_command('CF.COUNT', 'readTest', 'item1')
        client.execute_command('CF.INFO', 'readTest')
        client.execute_command('CF.MEXISTS', 'readTest', 'item1', 'item2')
        time.sleep(0.2)

        # Should not receive any events
        message = pubsub.get_message()  # Skip subscribe
        message = pubsub.get_message()
        assert message is None or message['type'] != 'pmessage'

        pubsub.close()

    def test_event_pattern_matching(self):
        """Test pattern matching for cuckoo events"""
        client = self.server.get_new_client()
        pubsub = client.pubsub()

        # Use wildcard pattern
        pubsub.psubscribe('__keyevent@0__:cuckoo.*')
        time.sleep(0.1)

        # Trigger various events
        client.execute_command('CF.RESERVE', 'patternTest', 1000)
        time.sleep(0.1)

        # Should receive event
        message = pubsub.get_message()  # Skip subscribe
        message = pubsub.get_message()
        assert message is not None
        assert b'cuckoo' in message['channel']

        pubsub.close()

    def test_load_event(self):
        """Test that CF.LOAD generates cuckoo.load event"""
        client = self.server.get_new_client()

        # Create and serialize a filter using CF.LOAD roundtrip
        client.execute_command('CF.RESERVE', 'dumpTest', 100)
        client.execute_command('CF.ADD', 'dumpTest', 'item1')
        snapshot = rewrite_cuckoo_aof(client, self.server)[b'dumpTest']

        # Subscribe to load events
        with client.pubsub() as pubsub:
            pubsub.psubscribe('__keyevent@0__:cuckoo.load')
            confirmation = pubsub.get_message(timeout=5)
            assert confirmation is not None
            assert confirmation['type'] == 'psubscribe'
            assert client.execute_command('CF.LOAD', 'loaded', snapshot) == b'OK'
            message = pubsub.get_message(timeout=5)
            assert message is not None
            assert message['type'] == 'pmessage'
            assert message['channel'] == b'__keyevent@0__:cuckoo.load'
            assert message['data'] == b'loaded'
            assert client.dump('loaded') == client.dump('dumpTest')

    def test_duplicate_add_emits_event_but_addnx_does_not(self):
        client = self.server.get_new_client()
        client.execute_command('CF.ADD', 'duplicate', 'item')
        with client.pubsub() as pubsub:
            pubsub.subscribe('__keyevent@0__:cuckoo.add')
            assert pubsub.get_message(timeout=1)['type'] == 'subscribe'
            assert client.execute_command('CF.ADDNX', 'duplicate', 'item') == 0
            assert pubsub.get_message(timeout=0.1) is None
            assert client.execute_command('CF.ADD', 'duplicate', 'item') == 1
            message = pubsub.get_message(timeout=1)
            assert message['data'] == b'duplicate'

    def test_unsuccessful_delete_emits_no_event(self):
        client = self.server.get_new_client()
        client.execute_command('CF.RESERVE', 'filter', 64)
        with client.pubsub() as pubsub:
            pubsub.subscribe('__keyevent@0__:cuckoo.del')
            assert pubsub.get_message(timeout=5)['type'] == 'subscribe'
            for marker in [b'before-insert', b'after-delete']:
                assert client.execute_command('CF.DEL', 'filter', 'item') == 0
                # The pong follows any event emitted by the completed command.
                pubsub.ping(marker)
                message = pubsub.get_message(timeout=5)
                assert message['type'] == 'pong'
                assert message['data'] == marker
                if marker == b'before-insert':
                    assert client.execute_command('CF.ADD', 'filter', 'item') == 1
                    assert client.execute_command('CF.DEL', 'filter', 'item') == 1
                    message = pubsub.get_message(timeout=5)
                    assert message['type'] == 'message'
                    assert message['channel'] == b'__keyevent@0__:cuckoo.del'
                    assert message['data'] == b'filter'
