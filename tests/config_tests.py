#!/usr/bin/env python
import redis
from test_util import TestUtil

class ConfigTests(TestUtil):

    def test_switch_config(self):
        self.start_redis_server(6380)
        self.start_redis_server(6381)
        self.start_proxy("tests/conf/timeout1.toml")

        r = redis.Redis(port=1530)
        response = r.execute_command("LOADCONFIG tests/conf/timeout1.toml")
        self.assertTrue(response)

        TestUtil.populate_redis_key(6380, "key1")
        self.assert_redis_key(1531, "key1")

        try:
            r.execute_command("SWITCHCONFIG")
            self.fail("Expected failure from SWITCHCONFIG")
        except redis.ResponseError, e:
            self.assertEqual(str(e), "The configs are the same!")

        # Same admin port, different listen_port.
        response = r.execute_command("LOADCONFIG tests/conf/swapconfig2.toml")
        self.assertTrue(response)

        TestUtil.populate_redis_key(6380, "key2")
        self.assert_redis_key(1531, "key2")

        try:
            response = r.execute_command("SWITCHCONFIG")
        except redis.ConnectionError, e:
            pass

        TestUtil.populate_redis_key(6380, "key3")
        self.assert_redis_key(1532, "key3")

        # Same admin port, same listener, different backend.
        r = redis.Redis(port=1530)
        response = r.execute_command("LOADCONFIG tests/conf/swapconfig3.toml")
        self.assertTrue(response)

        try:
            response = r.execute_command("SWITCHCONFIG")
        except redis.ConnectionError, e:
            pass
        TestUtil.populate_redis_key(6381, "key4")
        self.assert_redis_key(1532, "key4")


        # Different admin port, new pools.
        r = redis.Redis(port=1530)
        response = r.execute_command("LOADCONFIG tests/conf/swapconfig4.toml")
        self.assertTrue(response)

        try:
            response = r.execute_command("SWITCHCONFIG")
        except redis.ConnectionError, e:
            pass
        TestUtil.populate_redis_key(6381, "key5")
        self.assert_redis_key(1532, "key5")

        r = redis.Redis(port=1540)
        response = r.execute_command("LOADCONFIG tests/conf/swapconfig3.toml")
        self.assertTrue(response)