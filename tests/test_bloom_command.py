import pytest
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytests.conftest import resource_port_tracker

class TestBloomCommand(ValkeyBloomTestCaseBase):

    def verify_command_arity(self, command, expected_arity): 
        command_info = self.client.execute_command('COMMAND', 'INFO', command)
        actual_arity = command_info.get(command).get('arity')
        assert actual_arity == expected_arity, f"Arity mismatch for command '{command}'"

    def test_bloom_command_arity(self):
        self.verify_command_arity('BF.EXISTS', -1)
        self.verify_command_arity('BF.ADD', -1)
        self.verify_command_arity('BF.MEXISTS', -1)
        self.verify_command_arity('BF.MADD', -1)
        self.verify_command_arity('BF.CARD', -1)
        self.verify_command_arity('BF.RESERVE', -1)
        self.verify_command_arity('BF.INFO', -1)
        self.verify_command_arity('BF.INSERT', -1)

    def test_bloom_command_error(self):
        # test set up
        assert self.client.execute_command('BF.ADD key item') == 1
        assert self.client.execute_command('BF.RESERVE bf 0.01 1000') == b'OK'
        # non scaling filter
        assert self.client.execute_command('BF.RESERVE bf_non 0.01 2 NONSCALING') == b'OK'
        assert self.client.execute_command('BF.ADD bf_non 0') == 1
        assert self.client.execute_command('BF.ADD bf_non 1') == 1

        basic_error_test_cases = [
            # not found
            ('BF.INFO TEST404', 'not found'),
            # incorrect syntax and argument usage
            ('BF.ADD bf_non 2', 'non scaling filter is full'),
            ('bf.info key item', 'invalid information value'),
            ('bf.insert key CAPACITY 10000 ERROR 0.01 EXPANSION 0.99 NOCREATE NONSCALING ITEMS test1 test2 test3', 'bad expansion'),
            ('BF.INSERT KEY HELLO WORLD', 'unknown argument received'),
            ('BF.INSERT KEY error 2 ITEMS test1', '(0 < error rate range < 1)'),
            ('BF.INSERT TEST_LIMIT ERROR 0.99999999999999999 ITEMS ERROR_RATE', '(0 < error rate range < 1)'),
            ('BF.INSERT TEST_LIMIT CAPACITY 4394967295 ITEMS CAP', 'bad capacity'),
            ('BF.INSERT TEST_LIMIT CAPACITY 0 ITEMS CAP0', '(capacity should be larger than 0)'),
            ('BF.INSERT TEST_LIMIT EXPANSION 11 ITEMS EXPAN', 'bad expansion'),
            ('BF.INSERT TEST_NOCREATE NOCREATE ITEMS A B', 'not found'),
            ('BF.RESERVE KEY String 100', 'bad error rate'),
            ('BF.RESERVE KEY 0.99999999999999999 3000', '(0 < error rate range < 1)'),
            ('BF.RESERVE KEY 2 100', '(0 < error rate range < 1)'),
            ('BF.RESERVE KEY 0.01 String', 'bad capacity'),
            ('BF.RESERVE KEY 0.01 0.01', 'bad capacity'),
            ('BF.RESERVE KEY 0.01 -1', 'bad capacity'),
            ('BF.RESERVE KEY 0.01 4394967295', 'bad capacity'),
            ('BF.RESERVE bf 0.01 1000', 'item exists'),
            ('BF.RESERVE TEST_CAP 0.50 0', '(capacity should be larger than 0)'),

            # wrong number of arguments
            ('BF.ADD TEST', 'wrong number of arguments for \'BF.ADD\' command'),
            ('BF.ADD', 'wrong number of arguments for \'BF.ADD\' command'),
            ('BF.ADD HELLO TEST WORLD', 'wrong number of arguments for \'BF.ADD\' command'),
            ('BF.CARD KEY ITEM', 'wrong number of arguments for \'BF.CARD\' command'),
            ('bf.card', 'wrong number of arguments for \'BF.CARD\' command'),
            ('BF.EXISTS', 'wrong number of arguments for \'BF.EXISTS\' command'),
            ('bf.exists item', 'wrong number of arguments for \'BF.EXISTS\' command'),
            ('bf.exists key item hello', 'wrong number of arguments for \'BF.EXISTS\' command'),
            ('BF.INFO', 'wrong number of arguments for \'BF.INFO\' command'),
            ('bf.info key capacity size', 'wrong number of arguments for \'BF.INFO\' command'),
            ('BF.INSERT', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.INSERT KEY', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.INSERT KEY HELLO', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.INSERT MISS_ITEM EXPANSION 2', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.INSERT MISS_ITEM EXPANSION 2 ITEMS', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.INSERT MISS_VAL ERROR 0.5 EXPANSION', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.INSERT MISS_VAL ERROR 0.5 CAPACITY', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.INSERT MISS_VAL EXPANSION 2 EXPANSION', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.INSERT MISS_VAL EXPANSION 1 error', 'wrong number of arguments for \'BF.INSERT\' command'),
            ('BF.MADD', 'wrong number of arguments for \'BF.MADD\' command'),
            ('BF.MADD KEY', 'wrong number of arguments for \'BF.MADD\' command'),
            ('BF.MEXISTS', 'wrong number of arguments for \'BF.MEXISTS\' command'),
            ('BF.MEXISTS INFO', 'wrong number of arguments for \'BF.MEXISTS\' command'),
            ('BF.RESERVE', 'wrong number of arguments for \'BF.RESERVE\' command'),
            ('BF.RESERVE KEY', 'wrong number of arguments for \'BF.RESERVE\' command'),
            ('BF.RESERVE KEY SSS', 'wrong number of arguments for \'BF.RESERVE\' command'),
            ('BF.RESERVE TT1 0.01 1 NONSCALING test1 test2 test3', 'wrong number of arguments for \'BF.RESERVE\' command'),
            ('BF.RESERVE TT 0.01 1 NONSCALING EXPANSION 1', 'wrong number of arguments for \'BF.RESERVE\' command'),
        ]

        for test_case in basic_error_test_cases:
            cmd = test_case[0]
            expected_err_reply = test_case[1]
            self.verify_error_response(self.client, cmd, expected_err_reply)

    def test_bloom_command_behavior(self):
        basic_behavior_test_case = [
            ('BF.ADD key item', 1),
            ('BF.ADD key item', 0),
            ('BF.ADD key item1', 1),
            ('BF.EXISTS key item', 1),
            ('BF.EXISTS key item2', 0),
            ('BF.MADD key item item2', [0, 1]),
            ('BF.EXISTS key item', 1),
            ('BF.EXISTS key item2', 1),
            ('BF.EXISTS key item3', 0),
            ('BF.MADD hello world1 world2 world3', [1, 1, 1]),
            ('BF.MADD hello world1 world2 world3 world4', [0, 0, 0, 1]),
            ('BF.MEXISTS hello world5', [0]),
            ('BF.MADD hello world5', [1]),
            ('BF.MEXISTS hello world5 world6 world7', [1, 0, 0]),
            ('BF.INSERT TEST ITEMS ITEM', [1]),
            ('BF.INSERT TEST CAPACITY 1000 ITEMS ITEM', [0]),
            ('BF.INSERT TEST CAPACITY 200 error 0.50 ITEMS ITEM ITEM1 ITEM2', [0, 1, 1]),
            ('BF.INSERT TEST CAPACITY 300 ERROR 0.50 EXPANSION 1 ITEMS ITEM FOO', [0, 1]),
            ('BF.INSERT TEST ERROR 0.50 EXPANSION 3 NOCREATE items BOO', [1]), 
            ('BF.INSERT TEST ERROR 0.50 EXPANSION 1 NOCREATE NONSCALING items BOO', [0]),
            ('BF.INSERT TEST_EXPANSION EXPANSION 9 ITEMS ITEM', [1]),
            ('BF.INSERT TEST_CAPACITY CAPACITY 2000 ITEMS ITEM', [1]),
            ('BF.INSERT TEST_ITEMS ITEMS 1 2 3 EXPANSION 2', [1, 1, 1, 1, 0]),
            ('BF.INFO TEST Capacity', 100000),
            ('BF.INFO TEST ITEMS', 5),
            ('BF.INFO TEST filters', 1),
            ('bf.info TEST expansion', 2),
            ('BF.INFO TEST_EXPANSION EXPANSION', 9),
            ('BF.INFO TEST_CAPACITY CAPACITY', 2000),
            ('BF.CARD key', 3),
            ('BF.CARD hello', 5),
            ('BF.CARD TEST', 5),
            ('bf.card HELLO', 0),
            ('BF.RESERVE bf 0.01 1000', b'OK'),
            ('BF.RESERVE bf_exp 0.01 1000 EXPANSION 2', b'OK'),
            ('BF.RESERVE bf_non 0.01 1000 NONSCALING', b'OK'),
            ('bf.info bf_exp expansion', 2),
            ('BF.INFO bf_non expansion', None),
        ]

        for test_case in basic_behavior_test_case:
            cmd = test_case[0]
            expected_result = test_case[1]
            self.verify_command_success_reply(self.client, cmd, expected_result)

        # test bf.info
        assert self.client.execute_command('BF.RESERVE BF_INFO 0.50 2000 NONSCALING') == b'OK'
        bf_info = self.client.execute_command('BF.INFO BF_INFO')
        capacity_index = bf_info.index(b'Capacity') + 1
        filter_index = bf_info.index(b'Number of filters') + 1
        item_index = bf_info.index(b'Number of items inserted') + 1
        expansion_index = bf_info.index(b'Expansion rate') + 1
        assert bf_info[capacity_index] == self.client.execute_command('BF.INFO BF_INFO CAPACITY') == 2000
        assert bf_info[filter_index] == self.client.execute_command('BF.INFO BF_INFO FILTERS') == 1
        assert bf_info[item_index] == self.client.execute_command('BF.INFO BF_INFO ITEMS') == 0
        assert bf_info[expansion_index] == self.client.execute_command('BF.INFO BF_INFO EXPANSION') == None
