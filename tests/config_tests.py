#!/usr/bin/env python
import redis
import time
from test_util import TestUtil

class ConfigTests(TestUtil):

    def test_bad_backend_configs(self):
        # Verify that if backend does not have a host, it errors.
        proxy_proc = self.start_proxy("tests/conf/configsinglenohost.toml")
        self.assertEquals(proxy_proc.poll(), 1)

        # Verify that if backend has cluster hosts, it errors.
        proxy_proc = self.start_proxy("tests/conf/configsingleclusterhosts.toml")
        self.assertEquals(proxy_proc.poll(), 1)

        # Verify that if backend has a cluster name, it errors.
        proxy_proc = self.start_proxy("tests/conf/configsingleclustername.toml")
        self.assertEquals(proxy_proc.poll(), 1)

        # Verify that if cluster does not have cluster hosts, it errors.
        proxy_proc = self.start_proxy("tests/conf/configclusternohosts.toml")
        self.assertEquals(proxy_proc.poll(), 1)

        # Verify that if cluster does not have a cluster name, it errors.
        proxy_proc = self.start_proxy("tests/conf/configclusternoname.toml")
        self.assertEquals(proxy_proc.poll(), 1)

        # Verify that if cluster has a host, it errors.
        proxy_proc = self.start_proxy("tests/conf/configclusterwithhost.toml")
        self.assertEquals(proxy_proc.poll(), 1)


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
            self.assertEqual(str(e), "The loaded and staged configs are identical.")

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
        time.sleep(0.2)
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