#!/usr/bin/env python3
"""Test cases for the Client class."""
import unittest
import ibapipy.data.contract as ibc
from multiprocessing import Queue
from ibclientpy.client import Client


TEST_ACCOUNT_NAME = 'XXXXXXX'

TEST_CONTRACT = ibc.Contract('cash', 'eur', 'usd', 'idealpro')

WAIT_SECONDS = 10


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

    def test_accounts(self):
        result_queue = Queue()
        def callback(account):
            result_queue.put(account)
        client = Client()
        client.connect()
        client.account_subscribe(TEST_ACCOUNT_NAME, callback)
        # The initial account update will generate a lot of incremental updates
        counter = 0
        while counter < 200:
            result = result_queue.get()
            counter += 1
            self.assertIsNotNone(result)
        client.disconnect()

    def test_contracts(self):
        result_queue = Queue()
        def callback(contract):
            result_queue.put(contract)
        client = Client()
        client.connect()
        client.contract_subscribe(TEST_CONTRACT, callback)
        result = result_queue.get()
        self.assertIsNotNone(result)
        client.disconnect()

if __name__ == '__main__':
    unittest.main()
