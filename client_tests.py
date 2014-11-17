#!/usr/bin/env python3
"""Test cases for the Client class."""
import time
import unittest
import ibapipy.data.contract as ibc
from ibclientpy.client import Client


TEST_ACCOUNT_NAME = 'XXXXXXX'

TEST_CONTRACT = ibc.Contract('cash', 'eur', 'usd', 'idealpro')

WAIT_SECONDS = 2


class ClientTests(unittest.TestCase):
    """Test cases for the Client class."""

    def test_constructor(self):
        client = Client()
        self.assertFalse(client.is_connected())

    def test_connect(self):
        client = Client()
        self.assertFalse(client.is_connected())
        client.connect()
        self.assertTrue(client.is_connected())
        client.disconnect()
        self.assertFalse(client.is_connected())

    def test_on_next_id(self):
        client = Client()
        self.assertEqual(-1, client.next_id)
        client.connect()
        self.assertNotEqual(-1, client.next_id)
        client.disconnect()
        self.assertFalse(client.is_connected())

    def test_accounts(self):
        result_times = []
        result_items = []
        def callback(account):
            result_times.append(time.time())
            result_items.append(account)
        client = Client()
        client.connect()
        client.req_account_updates(TEST_ACCOUNT_NAME, callback)
        elapsed_secs = 0
        while elapsed_secs < WAIT_SECONDS:
            if len(result_times) > 0:
                elapsed_secs = time.time() - result_times[-1]
                self.assertIsNotNone(result_items[-1])
            time.sleep(1)
        self.assertEqual(284, len(result_items))
        self.assertEqual(TEST_ACCOUNT_NAME.lower(),
                         result_items[-1].account_name.lower())
        client.disconnect()

    def test_contracts(self):
        result_times = []
        result_items = []
        def callback(contract):
            result_times.append(time.time())
            result_items.append(contract)
        client = Client()
        client.connect()
        client.req_contract(TEST_CONTRACT, callback)
        elapsed_secs = 0
        while elapsed_secs < WAIT_SECONDS:
            if len(result_times) > 0:
                elapsed_secs = time.time() - result_times[-1]
                self.assertIsNotNone(result_items[-1])
            time.sleep(1)
        self.assertEqual(1, len(result_items))
        self.assertEqual(TEST_CONTRACT.symbol.lower(),
                         result_items[-1].symbol.lower())
        client.disconnect()

if __name__ == '__main__':
    unittest.main()
