#!/usr/bin/env python3
"""Test cases for the Client class.

Login with user "edemo" password "demouser" for a demo account that can be used
for basic testing.

"""
import asyncio
import logging
import time
import unittest
import ibapipy.data.contract as ibc
import ibapipy.data.order as ibo
from asyncio import Future, Task
from ibclientpy.client import Client


# Enable debug mode for asyncio
#logging.basicConfig(level=logging.DEBUG)

# Contract to use for testing
TEST_CONTRACT = ibc.Contract('cash', 'eur', 'usd', 'idealpro')


class ClientTests(unittest.TestCase):
    """Test cases for the Client class."""

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    def test_constructor(self):
        return
        client = Client()
        self.assertFalse(client.is_connected())

    def test_connect(self):
        return
        client = Client(self.loop)
        self.assertFalse(client.is_connected())
        self.loop.run_until_complete(client.connect())
        self.assertTrue(client.is_connected())
        self.assertTrue(client.next_id > 0)
        self.loop.run_until_complete(client.disconnect())
        self.assertFalse(client.is_connected())

    def test_get_account_name(self):
        return
        client = Client(self.loop)
        self.loop.run_until_complete(client.connect())
        task = Task(client.get_account_name())
        self.loop.run_until_complete(task)
        self.assertIsNotNone(task.result())
        self.loop.run_until_complete(client.disconnect())
        account_name = task.result()
        self.assertTrue(len(account_name) > 0)

    def test_get_account(self):
        return
        # This can be a time-consuming test (sometimes IB takes a minute or so
        # to return all of the account data)
        client = Client(self.loop)
        self.loop.run_until_complete(client.connect())
        # We need the account name first
        task = Task(client.get_account_name())
        self.loop.run_until_complete(task)
        account_name = task.result()
        # Now get the full account
        task = Task(client.get_account(account_name))
        self.loop.run_until_complete(task)
        self.assertIsNotNone(task.result())
        self.loop.run_until_complete(client.disconnect())

    def test_get_contract(self):
        return
        client = Client(self.loop)
        self.loop.run_until_complete(client.connect())
        task = Task(client.get_contract(TEST_CONTRACT))
        self.loop.run_until_complete(task)
        self.assertIsNotNone(task.result())
        self.loop.run_until_complete(client.disconnect())

    def test_get_orders(self):
        return
        client = Client(self.loop)
        self.loop.run_until_complete(client.connect())
        # Place two test orders
        test_order = ibo.Order('buy', 110000, 'stp', aux_price=2)
        task = Task(client.place_order(TEST_CONTRACT, test_order))
        self.loop.run_until_complete(task)
        order_id1 = task.result()
        self.assertTrue(order_id1 > 0)
        task = Task(client.place_order(TEST_CONTRACT, test_order))
        self.loop.run_until_complete(task)
        order_id2 = task.result()
        self.assertTrue(order_id2 > 0)
        # Retrieve the open orders
        task = Task(client.get_orders())
        self.loop.run_until_complete(task)
        self.assertIsNotNone(task.result())
        self.assertEqual(2, len(task.result()))
        # Cancel the orders and disconnect
        task = Task(client.cancel_order(order_id1))
        self.loop.run_until_complete(task)
        task = Task(client.cancel_order(order_id2))
        self.loop.run_until_complete(task)
        self.loop.run_until_complete(client.disconnect())

    def test_get_history(self):
        return
        client = Client(self.loop)
        self.loop.run_until_complete(client.connect())
        historical_ticks = []
        @asyncio.coroutine
        def get_history():
            history = client.get_history(TEST_CONTRACT, '2015-01-15 10:00',
                                         '2015-01-15 11:00', 'US/Eastern')
            for item in history:
                remaining, ticks = yield from item
                historical_ticks.extend(ticks)
        self.loop.run_until_complete(get_history())
        self.loop.run_until_complete(client.disconnect())
        self.assertEqual(3600, len(historical_ticks))

    def test_get_ticks(self):
        client = Client(self.loop)
        self.loop.run_until_complete(client.connect())
        realtime_ticks = []
        @asyncio.coroutine
        def get_ticks():
            tick_count = 0
            ticks = client.get_ticks(TEST_CONTRACT)
            for item in ticks:
                tick = yield from item
                realtime_ticks.append(tick)
                tick_count += 1
                if tick_count == 5:
                    yield from client.cancel_ticks()
        self.loop.run_until_complete(get_ticks())
        self.loop.run_until_complete(client.disconnect())
        self.assertEqual(5, len(realtime_ticks))


if __name__ == '__main__':
    unittest.main()
