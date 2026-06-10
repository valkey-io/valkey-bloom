from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

class TestKeyEventNotifications(ValkeyBloomTestCaseBase):

    def test_keyspace_bloom_commands(self):
        self.create_subscribe_clients()
        # (command, events_expected, message_count). A BF.ADD/BF.MADD/BF.INSERT
        # against a missing key both creates the filter (bloom.reserve) and adds
        # items (bloom.add), so it emits both events.
        bloom_commands = [
            ('BF.ADD add_test key', ['bloom.reserve', 'bloom.add'], 4),
            ('BF.MADD madd_test key1 key2', ['bloom.reserve', 'bloom.add'], 4),
            ('BF.EXISTS exists_test key', [], 0),
            ('BF.INSERT insert_test ITEMS key1 key2', ['bloom.reserve', 'bloom.add'], 4),
            ('BF.RESERVE reserve_test 0.01 1000', ['bloom.reserve'], 2),
        ]

        for command, events_expected, expected_message_count in bloom_commands:
            key_name = command.split()[1]
            expected_messages = []
            for event in events_expected:
                keyspace_message, keyevent_message = self.build_keyspace_event_messages(event, key_name)
                expected_messages.extend([keyspace_message, keyevent_message])
            result_messages = self.get_subscribe_client_messages(self.keyspace_client, command, expected_message_count)
            self.check_keyspace_response(result_messages, expected_messages)

        # test del
        del_messages = self.get_subscribe_client_messages(self.keyspace_client, 'DEL add_test', 2)
        self.check_keyspace_response(del_messages, list(self.build_keyspace_event_messages('del', 'add_test')))
