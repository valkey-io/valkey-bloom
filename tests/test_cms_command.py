#!/usr/bin/env python3

import pytest
from valkey import ResponseError
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

class TestCMSCommand(ValkeyBloomTestCaseBase):

    def test_cms_command_error(self):

        assert self.client.execute_command('CMS.INITBYDIM sketch 1000 5') == b'OK'

        basic_error_test_cases = [

            # incorrect syntax and argument usage
            ('CMS.INITBYDIM sketch 1000 5', 'item exists'),
            ('CMS.INITBYDIM newsketch abc 5', 'bad width'),
            ('CMS.INITBYDIM newsketch 1000 abc', 'bad depth'),
            ('CMS.INITBYDIM newsketch 0 5', 'bad width'),
            ('CMS.INITBYDIM newsketch 1000 0', 'bad depth'),
            ('CMS.INITBYDIM newsketch -1 5', 'bad width'),
            ('CMS.INITBYDIM newsketch 1000 -1', 'bad depth'),

            ('CMS.INITBYPROB newsketch abc 0.01', 'bad error rate'),
            ('CMS.INITBYPROB newsketch 0.01 abc', 'bad probability'),
            ('CMS.INITBYPROB newsketch 0 0.01', 'error rate should be between 0 and 1'),
            ('CMS.INITBYPROB newsketch 1 0.01', 'error rate should be between 0 and 1'),
            ('CMS.INITBYPROB newsketch 0.01 0', 'probability rate should be between 0 and 1'),
            ('CMS.INITBYPROB newsketch 0.01 1', 'probability rate should be between 0 and 1'),

            ('CMS.INCRBY sketch item abc', 'bad increment'),
            ('CMS.INCRBY sketch item -1', 'bad increment'),

            # wrong number of arguments
            ('CMS.INITBYDIM', "wrong number of arguments for 'CMS.INITBYDIM' command"),
            ('CMS.INITBYDIM key', "wrong number of arguments for 'CMS.INITBYDIM' command"),
            ('CMS.INITBYDIM key 1000', "wrong number of arguments for 'CMS.INITBYDIM' command"),
            ('CMS.INITBYDIM key 1000 5 extra', "wrong number of arguments for 'CMS.INITBYDIM' command"),

            ('CMS.INITBYPROB', "wrong number of arguments for 'CMS.INITBYPROB' command"),
            ('CMS.INITBYPROB key', "wrong number of arguments for 'CMS.INITBYPROB' command"),
            ('CMS.INITBYPROB key 0.01', "wrong number of arguments for 'CMS.INITBYPROB' command"),
            ('CMS.INITBYPROB key 0.01 0.01 extra', "wrong number of arguments for 'CMS.INITBYPROB' command"),

            ('CMS.INCRBY', "wrong number of arguments for 'CMS.INCRBY' command"),
            ('CMS.INCRBY key', "wrong number of arguments for 'CMS.INCRBY' command"),
            ('CMS.INCRBY key item', "wrong number of arguments for 'CMS.INCRBY' command"),
            ('CMS.INCRBY key item 1 item2', "wrong number of arguments for 'CMS.INCRBY' command"),

            ('CMS.QUERY', "wrong number of arguments for 'CMS.QUERY' command"),
            ('CMS.QUERY key', "wrong number of arguments for 'CMS.QUERY' command"),
            ('CMS.INFO', "wrong number of arguments for 'CMS.INFO' command"),
            ('CMS.INFO key WIDTH WIDTH', "wrong number of arguments for 'CMS.INFO' command"),
            ('CMS.INFO key NOTAPARAM', "invalid information value for 'CMS.INFO' command"),



         
        ]

        for test_case in basic_error_test_cases:
            cmd = test_case[0]
            expected_err_reply = test_case[1]
            self.verify_error_response(self.client, cmd, expected_err_reply)
