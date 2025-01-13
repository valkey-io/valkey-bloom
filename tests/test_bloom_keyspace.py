import logging, time
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytests.conftest import resource_port_tracker

class TestKeyEventNotifications(ValkeyBloomTestCaseBase):
    RESERVE_KEYSPACE_MESSAGE = {'type': 'pmessage', 'pattern': b'__key*__:*', 'channel': b'__keyspace@0__:intermediate_val', 'data': b'bloom.reserve'}
    RESERVE_KEYEVENT_MESSAGE = {'type': 'pmessage', 'pattern': b'__key*__:*', 'channel': b'__keyevent@0__:bloom.reserve', 'data': b'intermediate_val'}
    ADD_KEYSPACE_MESSAGE = {'type': 'pmessage', 'pattern': b'__key*__:*', 'channel': b'__keyspace@0__:intermediate_val', 'data': b'bloom.add'} 
    ADD_KEYEVENT_MESSAGE = {'type': 'pmessage', 'pattern': b'__key*__:*', 'channel': b'__keyevent@0__:bloom.add', 'data': b'intermediate_val'}

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
                # Only for the first time we get messages we should skip the first message gotten
                if count > 0 or "BF.ADD" not in cmd:
                    messages.append(message)    
                count = count + 1
            if timeout < time.time():
                assert False, f"The number of expected messages failed tor eturn in time, messages received so far {messages}"
        return messages
    
    def test_keyspace_bloom_commands(self):
        self.create_subscribe_clients()
        # The first call to get messages will return message that shows we subscribed to messages so we expect one more message than we need to check for
        # the first time we look at messages
        bloom_commands = [
            ('BF.ADD add_test key', True, True, 5),
            ('BF.MADD madd_test key1 key2', True, True, 4),
            ('BF.EXISTS exists_test key', False, False, 0),
            ('BF.INSERT insert_test ITEMS key1 key2', True, True, 4),
            ('BF.RESERVE reserve_test 0.01 1000', True, False, 2)
        ]

        for command, reserve_expected, add_expected, expected_message_count in bloom_commands:
            expected_messages = self.create_expected_message_list(reserve_expected, add_expected, command.split()[1]) if reserve_expected else []
            result_messages = self.get_subscribe_client_messages(self.keyspace_client, command, expected_message_count)
            self.check_response(result_messages, expected_messages)

    def create_subscribe_clients(self):
        self.keyspace_client = self.server.get_new_client()
        self.keyspace_client_subscribe = self.keyspace_client.pubsub()
        self.keyspace_client_subscribe.psubscribe('__key*__:*')
        self.keyspace_client.execute_command('CONFIG' ,'SET','notify-keyspace-events', 'KEA')
    