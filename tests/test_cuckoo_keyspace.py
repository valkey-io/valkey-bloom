import time
from valkey import ResponseError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase

class TestCuckooKeyspace(ValkeyBloomTestCaseBase):

    def setUp(self):
        super().setUp()
        # Enable keyspace notifications for module events
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

    def test_loadchunk_event(self):
        """Test that CF.LOADCHUNK generates cuckoo.loadchunk event"""
        client = self.server.get_new_client()

        # Create filter and get dump
        client.execute_command('CF.RESERVE', 'dumpTest', 100)
        client.execute_command('CF.ADD', 'dumpTest', 'item1')
        iterator = 0
        chunks = []

        while True:
            result = client.execute_command('CF.SCANDUMP', 'dumpTest', iterator)
            iterator = result[0]
            if iterator == 0:
                break
            chunks.append(result[1])

        # Subscribe to loadchunk events
        pubsub = client.pubsub()
        pubsub.psubscribe('__keyevent@0__:cuckoo.loadchunk')
        time.sleep(0.1)

        # Load chunks
        client.execute_command('DEL', 'loadTest')
        iterator = 0
        for chunk in chunks:
            iterator = client.execute_command('CF.LOADCHUNK', 'loadTest', iterator, chunk)
            time.sleep(0.1)

        # Should receive loadchunk events
        message = pubsub.get_message()  # Skip subscribe
        message = pubsub.get_message()
        assert message is not None
        assert message['channel'] == b'__keyevent@0__:cuckoo.loadchunk'

        pubsub.close()
