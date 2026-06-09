import time
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

class TestTopkKeyEventNotifications(ValkeyBloomTestCaseBase):
    RESERVE_KEYSPACE_MESSAGE = {'type': 'pmessage', 'pattern': b'__key*__:*', 'channel': b'__keyspace@0__:intermediate_val', 'data': b'topk.reserve'}
    RESERVE_KEYEVENT_MESSAGE = {'type': 'pmessage', 'pattern': b'__key*__:*', 'channel': b'__keyevent@0__:topk.reserve', 'data': b'intermediate_val'}
    ADD_KEYSPACE_MESSAGE = {'type': 'pmessage', 'pattern': b'__key*__:*', 'channel': b'__keyspace@0__:intermediate_val', 'data': b'topk.add'}
    ADD_KEYEVENT_MESSAGE = {'type': 'pmessage', 'pattern': b'__key*__:*', 'channel': b'__keyevent@0__:topk.add', 'data': b'intermediate_val'}

    def create_expected_message_list(self, reserve_expected, add_expected, key_name):
        expected_messages = []
        self.RESERVE_KEYSPACE_MESSAGE['channel'] = f"__keyspace@0__:{key_name}".encode('utf-8')
        self.RESERVE_KEYEVENT_MESSAGE['data'] = f"{key_name}".encode('utf-8')
        self.ADD_KEYSPACE_MESSAGE['channel'] = f"__keyspace@0__:{key_name}".encode('utf-8')
        self.ADD_KEYEVENT_MESSAGE['data'] = f"{key_name}".encode('utf-8')
        if reserve_expected:
            expected_messages.append(self.RESERVE_KEYEVENT_MESSAGE)
            expected_messages.append(self.RESERVE_KEYSPACE_MESSAGE)
        if add_expected:
            expected_messages.append(self.ADD_KEYSPACE_MESSAGE)
            expected_messages.append(self.ADD_KEYEVENT_MESSAGE)
        return expected_messages

    def check_response(self, result_messages, expected_messages):
        extra_message = self.keyspace_client_subscribe.get_message()
        if extra_message:
            assert False, f"Unexpected extra message returned: {extra_message}"
        for message in expected_messages:
            assert message in result_messages, f"{message} was not found in messages received"

    def get_subscribe_client_messages(self, client, cmd, expected_message_count):
        client.execute_command(cmd)
        count = 0
        messages = []
        timeout = time.time() + 5
        while expected_message_count != count:
            message = self.keyspace_client_subscribe.get_message()
            if message:
                if message.get('type') != 'pmessage':
                    continue
                messages.append(message)
                count = count + 1
            if timeout < time.time():
                assert False, f"The number of expected messages failed to return in time, messages received so far {messages}"
        return messages

    def test_keyspace_topk_commands(self):
        self.create_subscribe_clients()
        topk_commands = [
            ('TOPK.RESERVE add_test 3 50 4 0.9', True, False, 2),
            ('TOPK.ADD add_test apple banana', False, True, 2),
            ('TOPK.RESERVE incr_test 3 50 4 0.9', True, False, 2),
            ('TOPK.INCRBY incr_test apple 5 banana 3', False, True, 2),
            ('TOPK.QUERY add_test apple', False, False, 0)
        ]

        for command, reserve_expected, add_expected, expected_message_count in topk_commands:
            expected_messages = self.create_expected_message_list(reserve_expected, add_expected, command.split()[1]) if reserve_expected or add_expected else []
            result_messages = self.get_subscribe_client_messages(self.keyspace_client, command, expected_message_count)
            self.check_response(result_messages, expected_messages)

        # test del
        del_messages = self.get_subscribe_client_messages(self.keyspace_client, 'DEL add_test', 2)
        assert {'type': 'pmessage', 'pattern': b'__key*__:*', 'channel': b'__keyspace@0__:add_test', 'data': b'del'} in del_messages
        assert {'type': 'pmessage', 'pattern': b'__key*__:*', 'channel': b'__keyevent@0__:del', 'data': b'add_test'} in del_messages

    def create_subscribe_clients(self):
        self.keyspace_client = self.server.get_new_client()
        self.keyspace_client_subscribe = self.keyspace_client.pubsub()
        self.keyspace_client_subscribe.psubscribe('__key*__:*')
        self.keyspace_client.execute_command('CONFIG', 'SET', 'notify-keyspace-events', 'KEA')
