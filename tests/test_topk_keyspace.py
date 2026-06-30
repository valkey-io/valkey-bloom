from valkey_bloom_test_case import TopkFixedSeedMixin, ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

class TestTopkKeyEventNotifications(TopkFixedSeedMixin, ValkeyBloomTestCaseBase):

    def test_keyspace_topk_commands(self):
        self.create_subscribe_clients()
        # (command, events_expected, message_count). TOPK.ADD and TOPK.INCRBY
        # both publish topk.add; neither auto-creates the key, so each is
        # preceded by its own TOPK.RESERVE. TOPK.QUERY stands in for the
        # read-only commands, none of which notify.
        topk_commands = [
            ('TOPK.RESERVE add_test 3 50 4 0.9', ['topk.reserve'], 2),
            ('TOPK.ADD add_test apple banana', ['topk.add'], 2),
            ('TOPK.RESERVE incr_test 3 50 4 0.9', ['topk.reserve'], 2),
            ('TOPK.INCRBY incr_test apple 5 banana 3', ['topk.add'], 2),
            # Read-only commands 
            ('TOPK.QUERY add_test apple', [], 0),
            ('TOPK.COUNT add_test apple', [], 0),
            ('TOPK.LIST add_test', [], 0),
            ('TOPK.INFO add_test', [], 0),
        ]

        for command, events_expected, expected_message_count in topk_commands:
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
