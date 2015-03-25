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
        client = Client()
        self.assertFalse(client.is_connected())

    def test_connect(self):
        client = Client(self.loop)
        self.assertFalse(client.is_connected())
        self.loop.run_until_complete(client.connect())
        self.assertTrue(client.is_connected())
        self.assertTrue(client.next_id > 0)
        self.loop.run_until_complete(client.disconnect())
        self.assertFalse(client.is_connected())

    def test_get_next_valid_id(self):
        client = Client(self.loop)
        self.loop.run_until_complete(client.connect())
        self.assertTrue(client.is_connected())
        self.assertTrue(client.next_id > 0)
        orig_id = client.next_id
        next_id = self.loop.run_until_complete(client.get_next_valid_id())
        self.assertEqual(orig_id, next_id)
        self.loop.run_until_complete(client.disconnect())

    def test_get_account_name(self):
        client = Client(self.loop)
        self.loop.run_until_complete(client.connect())
        task = Task(client.get_account_name())
        self.loop.run_until_complete(task)
        self.assertIsNotNone(task.result())
        self.loop.run_until_complete(client.disconnect())
        account_name = task.result()
        self.assertTrue(len(account_name) > 0)

    def test_get_account(self):
        # This can be a time-consuming test (sometimes IB takes a minute or so
        # to return all of the account data)
        client = Client(self.loop)
        self.loop.run_until_complete(client.connect())
        task = Task(client.get_account())
        self.loop.run_until_complete(task)
        self.assertIsNotNone(task.result())
        self.loop.run_until_complete(client.disconnect())

    def test_get_contract(self):
        client = Client(self.loop)
        self.loop.run_until_complete(client.connect())
        task = Task(client.get_contract(TEST_CONTRACT))
        self.loop.run_until_complete(task)
        self.assertIsNotNone(task.result())
        self.loop.run_until_complete(client.disconnect())

    def test_get_orders(self):
        client = Client(self.loop)
        self.loop.run_until_complete(client.connect())
        # Place two test orders
        buy_order = ibo.Order('buy', 110000, 'stp', aux_price=2)
        task = Task(client.place_order(TEST_CONTRACT, buy_order))
        self.loop.run_until_complete(task)
        buy_order_id = task.result()
        self.assertTrue(buy_order_id > 0)
        sell_order = ibo.Order('sell', 110000, 'stp', aux_price=0.2)
        task = Task(client.place_order(TEST_CONTRACT, sell_order))
        self.loop.run_until_complete(task)
        sell_order_id = task.result()
        self.assertTrue(sell_order_id > 0)
        # Retrieve the open orders matching our test ids
        task = Task(client.get_orders())
        self.loop.run_until_complete(task)
        self.assertIsNotNone(task.result())
        orders = [x for x in task.result()
                  if x.order_id in (buy_order_id, sell_order_id)]
        self.assertEqual(2, len(orders))
        # Cancel the orders and disconnect
        task = Task(client.cancel_order(buy_order_id))
        self.loop.run_until_complete(task)
        task = Task(client.cancel_order(sell_order_id))
        self.loop.run_until_complete(task)
        self.loop.run_until_complete(client.disconnect())

    def test_get_next_history_block(self):
        client = Client(self.loop)
        self.loop.run_until_complete(client.connect())
        historical_ticks = []
        @asyncio.coroutine
        def get_history():
            while True:
                blocks_left, ticks = yield from client.get_next_history_block(
                        TEST_CONTRACT, '2015-01-15 10:00', '2015-01-15 11:00',
                        'US/Eastern')
                if ticks is None:
                    break
                historical_ticks.extend(ticks)
        self.loop.run_until_complete(get_history())
        self.loop.run_until_complete(client.disconnect())
        self.assertEqual(3600, len(historical_ticks))

    def test_get_next_tick(self):
        client = Client(self.loop)
        self.loop.run_until_complete(client.connect())
        realtime_ticks = []
        @asyncio.coroutine
        def get_ticks():
            while True:
                tick = yield from client.get_next_tick(TEST_CONTRACT)
                if tick is None:
                    break
                realtime_ticks.append(tick)
                if len(realtime_ticks) == 3:
                    yield from client.cancel_ticks(TEST_CONTRACT)
        self.loop.run_until_complete(get_ticks())
        self.loop.run_until_complete(client.disconnect())
        self.assertTrue(len(realtime_ticks) >= 3)


if __name__ == '__main__':
    all_tests = unittest.TestSuite()
    all_tests.addTest(ClientTests('test_constructor'))
    all_tests.addTest(ClientTests('test_connect'))
    all_tests.addTest(ClientTests('test_get_next_valid_id'))
    all_tests.addTest(ClientTests('test_get_account_name'))
    all_tests.addTest(ClientTests('test_get_account'))
    all_tests.addTest(ClientTests('test_get_contract'))
    all_tests.addTest(ClientTests('test_get_orders'))
    all_tests.addTest(ClientTests('test_get_next_history_block'))
    all_tests.addTest(ClientTests('test_get_next_tick'))
    single_test = unittest.TestSuite()
    single_test.addTest(ClientTests('test_get_next_history_block'))
    unittest.TextTestRunner(verbosity=2).run(all_tests)
    #unittest.TextTestRunner(verbosity=2).run(single_test)

