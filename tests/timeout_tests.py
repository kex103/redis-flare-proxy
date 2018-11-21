#!/usr/bin/env python
import time
import socket
from test_util import TestUtil

class TimeoutTests(TestUtil):

    def test_single_backend_with_timeout(self):
        self.start_redis_server(6381)
        self.start_delayer(6380, 6381, 50)
        self.start_proxy("tests/conf/timeout1.toml")

        TestUtil.verify_redis_connection(1531)

    def test_single_backend_failed_timeout(self):
        self.start_redis_server(6381)
        self.start_delayer(6380, 6381, 110)
        self.start_proxy("tests/conf/timeout1.toml")

        TestUtil.verify_redis_error(1531, "ERROR: Not connected")

    def test_single_backend_ejected(self):
        self.start_redis_server(6381)
        self.start_delayer(6380, 6381, 2, 6382)
        self.start_proxy("tests/conf/retrylimit1.toml")

        TestUtil.verify_redis_connection(1531)

        # Set a long delay, so that all requests to the backend will time out.
        conn_to_delayer = socket.socket(socket.AF_INET)
        conn_to_delayer.connect(("0.0.0.0", 6382))
        conn_to_delayer.sendall("SETDELAY 401")
        # Verify that requests time out. After the 3rd failure, the backend is blacklisted.
        TestUtil.verify_redis_error(1531, "Proxy timed out")
        TestUtil.verify_redis_error(1531, "Proxy timed out")
        TestUtil.verify_redis_error(1531, "Proxy timed out")
        TestUtil.verify_redis_error(1531, "ERROR: Not connected")
        TestUtil.verify_redis_error(1531, "ERROR: Not connected")
        conn_to_delayer.sendall("BLOCK_NEW_CONNS 750")
        # Wait an extra long time, to test that the first ping times out. A second ping should be sent.
        time.sleep(4)

        # Set a short delay, so requests will stop timing out.
        conn_to_delayer.sendall("SETDELAY 2")
        # Wait a second, to give time for the proxy to detect that the backend is available again.
        time.sleep(2)
        # Verify that requests succeed now.
        TestUtil.verify_redis_connection(1531)
        TestUtil.verify_redis_connection(1531)
        TestUtil.verify_redis_connection(1531)
        
# test a backend responding with just a partial response and then failing to ever respond.
    def test_partial_response_timeout(self):
        # Test having a broken pipe.
        # First, spawn a proxy, then spawn a mock redis.
        # Have the mock redis cut off the connection partway through the redis response.
        # Verify that the proxy correctly gives a "Broken pipe" error.
        # Verify that subsequent requests get the same error.
        # Verify that the proxy recovers and works normally afterwards.
        pass

    def test_retry_connection(self):
        self.start_proxy("tests/conf/timeout1.toml")

        TestUtil.verify_redis_error(1531, "ERROR: Not connected")

        self.start_redis_server(6380)
        time.sleep(1) # TODO: Configure different retry frequencies besides 1 second

        TestUtil.verify_redis_connection(1531)

    def test_backend_ejection(self):
        # Test that auto backend host ejection works as expected.
        # 1. Verify that when a host fails enough times, all requests get shifted by the modula.
        # 2. Verify that when another host fails, all requests get shifted again.
        # 3. Verify that when that 2nd failed host recovers, requests shift back.
        # 4. Verify requests return to original sharding when the 1st failed host recovers.
        # TODO: Set up a test while a constant stream of redis requests occurs. We want to make sure the switch is clean.

        self.start_redis_server(6381)
        self.start_redis_server(6382)
        self.start_redis_server(6386)
        self.start_redis_server(6380)
        ports = [6381, 6382, 6386, 6380]
        self.start_delayer(incoming_port=6384, outgoing_port=6380, delay=1, admin_port=6385)
        self.start_delayer(incoming_port=6383, outgoing_port=6386, delay=1, admin_port=6387)
        self.start_proxy("tests/conf/multishardeject1.toml")
        TestUtil.verify_redis_connection(1531)

        TestUtil.populate_redis_key(1531, "key1")
        self.assert_redis_key(6381, "key1")
        TestUtil.populate_redis_key(1531, "key2")
        self.assert_redis_key(6382, "key2")
        TestUtil.populate_redis_key(1531, "key3")
        self.assert_redis_key(6386, "key3")
        TestUtil.populate_redis_key(1531, "key4")
        self.assert_redis_key(6380, "key4")
        TestUtil.populate_redis_key(1531, "key5")
        self.assert_redis_key(6381, "key5")
        TestUtil.populate_redis_key(1531, "key6")
        self.assert_redis_key(6382, "key6")

        TestUtil.flush_keys(ports)

        # Set a long delay, so that all requests to the backend will time out.
        conn_to_delayer1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn_to_delayer1.connect(("0.0.0.0", 6385))
        conn_to_delayer1.sendall("SETDELAY 400")

        # Send a request to the bad backend, to make proxy realize it is down.
        # TODO: One feature is have proxy ping health checks to backends periodically.
        try:
            TestUtil.populate_redis_key(1531, "key4")
            self.fail("Expected to time out")
        except:
            pass
        self.assert_redis_key(6380, "key4")

        TestUtil.populate_redis_key(1531, "key4")
        self.assert_redis_key(6381, "key4")
        TestUtil.populate_redis_key(1531, "key1")
        self.assert_redis_key(6382, "key1")
        TestUtil.populate_redis_key(1531, "key2")
        self.assert_redis_key(6381, "key2")
        TestUtil.populate_redis_key(1531, "key3")
        self.assert_redis_key(6386, "key3")
        TestUtil.populate_redis_key(1531, "key4")
        self.assert_redis_key(6381, "key4")
        TestUtil.populate_redis_key(1531, "key5")
        self.assert_redis_key(6386, "key5")
        TestUtil.populate_redis_key(1531, "key6")
        self.assert_redis_key(6382, "key6")

        TestUtil.flush_keys(ports)

        # Set a long delay, so that all requests to the backend will time out.
        conn_to_delayer2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn_to_delayer2.connect(("0.0.0.0", 6387))
        conn_to_delayer2.sendall("SETDELAY 401")
        try:
            TestUtil.populate_redis_key(1531, "key3")
            self.fail("Expected to time out")
        except:
            pass
        self.assert_redis_key(6386, "key3")

        TestUtil.populate_redis_key(1531, "key1")
        self.assert_redis_key(6381, "key1")
        TestUtil.populate_redis_key(1531, "key2")
        self.assert_redis_key(6382, "key2")
        TestUtil.populate_redis_key(1531, "key3")
        self.assert_redis_key(6381, "key3")
        # key4 can shift because it is originally mapped to a bad instance.
        TestUtil.populate_redis_key(1531, "key4")
        self.assert_redis_key(6382, "key4")
        TestUtil.populate_redis_key(1531, "key5")
        self.assert_redis_key(6381, "key5")
        TestUtil.populate_redis_key(1531, "key6")
        self.assert_redis_key(6382, "key6")

        TestUtil.flush_keys(ports)

        conn_to_delayer2.sendall("SETDELAY 1")
        time.sleep(1)

        TestUtil.populate_redis_key(1531, "key3")
        self.assert_redis_key(6386, "key3")
        TestUtil.populate_redis_key(1531, "key4")
        self.assert_redis_key(6381, "key4")
        TestUtil.populate_redis_key(1531, "key1")
        self.assert_redis_key(6382, "key1")
        TestUtil.populate_redis_key(1531, "key2")
        self.assert_redis_key(6381, "key2")
        TestUtil.populate_redis_key(1531, "key3")
        self.assert_redis_key(6386, "key3")
        TestUtil.populate_redis_key(1531, "key4")
        self.assert_redis_key(6381, "key4")
        TestUtil.populate_redis_key(1531, "key5")
        self.assert_redis_key(6386, "key5")
        TestUtil.populate_redis_key(1531, "key6")
        self.assert_redis_key(6382, "key6")

        TestUtil.flush_keys(ports)

        conn_to_delayer1.sendall("SETDELAY 1")
        time.sleep(1)

        TestUtil.populate_redis_key(1531, "key1")
        self.assert_redis_key(6381, "key1")
        TestUtil.populate_redis_key(1531, "key2")
        self.assert_redis_key(6382, "key2")
        TestUtil.populate_redis_key(1531, "key3")
        self.assert_redis_key(6386, "key3")
        TestUtil.populate_redis_key(1531, "key4")
        self.assert_redis_key(6380, "key4")
        TestUtil.populate_redis_key(1531, "key5")
        self.assert_redis_key(6381, "key5")
        TestUtil.populate_redis_key(1531, "key6")
        self.assert_redis_key(6382, "key6")
