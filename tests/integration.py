#!/usr/bin/env python
import redis
from subprocess import call
import subprocess
import os
import sys
import time
import unittest
import socket
from test_util import TestUtil

class TestRedFlareProxy(TestUtil):

    def test_single_backend_no_timeout(self):
        self.start_redis_server(6380)
        self.start_proxy("tests/conf/testconfig1.toml")

        TestUtil.verify_redis_connection(1531)

    def test_single_backend_with_timeout(self):
        self.start_redis_server(6381)
        self.start_delayer(6380, 6381, 50)
        self.start_proxy("tests/conf/timeout1.toml")

        TestUtil.verify_redis_connection(1531)

    def test_single_backend_failed_timeout(self):
        self.start_redis_server(6381)
        self.start_delayer(6380, 6381, 101)
        self.start_proxy("tests/conf/timeout1.toml")

        TestUtil.verify_redis_error(1531, "ERROR: Not connected")

    def test_single_backend_ejected(self):
        self.start_redis_server(6381)
        self.start_delayer(6380, 6381, 2, 6382)
        self.start_proxy("tests/conf/retrylimit1.toml")

        TestUtil.verify_redis_connection(1531)

        # Set a long delay, so that all requests to the backend will time out.
        conn_to_delayer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn_to_delayer.connect(("0.0.0.0", 6382))
        conn_to_delayer.sendall("SETDELAY 401")
        # Verify that requests time out. After the 3rd failure, the backend is blacklisted.
        TestUtil.verify_redis_error(1531, "Proxy timed out")
        TestUtil.verify_redis_error(1531, "Proxy timed out")
        TestUtil.verify_redis_error(1531, "Proxy timed out")
        TestUtil.verify_redis_error(1531, "ERROR: Not connected")
        TestUtil.verify_redis_error(1531, "ERROR: Not connected")
        conn_to_delayer.sendall("BLOCK_NEW_CONNS 750")
        time.sleep(1)

        # Set a short delay, so requests will stop timing out.
        conn_to_delayer.sendall("SETDELAY 2")
        # Wait a second, to give time for the proxy to detect that the backend is available again.
        time.sleep(1)
        # Verify that requests succeed now.
        TestUtil.verify_redis_connection(1531)
        TestUtil.verify_redis_connection(1531)
        TestUtil.verify_redis_connection(1531)

    def test_no_backend_failure(self):
        # Spawn a proxy with no backend. Verify that it complains about invalid config.
        self.start_proxy("tests/conf/nobackends.toml")
        TestUtil.verify_redis_error(1533, expect_conn_error=True)

        # Then spawn a proxy pointing to an invalid backend. Verify the redis error should be "Not connected"
        self.start_proxy("tests/conf/timeout1.toml")
        TestUtil.verify_redis_error(1531, "ERROR: Not connected")

# test a backend responding with just a partial response and then failing to ever respond.
    def test_partial_response_timeout(self):
        # Test having a broken pipe.
        # First, spawn a proxy, then spawn a mock redis.
        # Have the mock redis cut off the connection partway through the redis response.
        # Verify that the proxy correctly gives a "Broken pipe" error.
        # Verify that subsequent requests get the same error.
        # Verify that the proxy recovers and works normally afterwards.
        pass

# Test successful, multiple (4) backends, no timeout. verify that the sharding is correct.
    def test_multiple_backend_no_timeout(self):
        self.start_redis_server(6381)
        self.start_redis_server(6382)
        self.start_redis_server(6383)
        self.start_redis_server(6384)
        self.start_proxy("tests/conf/multishard1.toml")

        TestUtil.verify_redis_connection(1533)

        TestUtil.populate_redis_key(6381, "key1")
        self.assert_redis_key(1533, "key1")
        TestUtil.populate_redis_key(6382, "key2")
        self.assert_redis_key(1533, "key2")

    def test_multiple_backend_with_timeout(self):
        # TODO: Set delay at 1 at first, then verify timeout when delay set to 101.
        self.start_redis_server(6381)
        self.start_redis_server(6380)
        self.start_redis_server(6383)
        self.start_redis_server(6384)
        self.start_delayer(6382, 6380, 101)
        self.start_proxy("tests/conf/multishard2.toml")

        TestUtil.verify_redis_connection(1533)

        TestUtil.populate_redis_key(6384, "key1")
        TestUtil.verify_redis_error(1533, "ERROR: Not connected")

    def test_hashtags(self):
        self.start_redis_server(6381)
        self.start_redis_server(6382)
        self.start_redis_server(6383)
        self.start_redis_server(6384)
        ports = [6381, 6382, 6383, 6384]
        self.start_proxy("tests/conf/multishardtags1.toml")
        TestUtil.verify_redis_connection(1533)

        TestUtil.populate_redis_key(1533, "key1")
        self.assert_redis_key(6381, "key1")
        TestUtil.populate_redis_key(1533, "key4")
        self.assert_redis_key(6384, "key4")

        # Verify single hashtag doesn't work.
        TestUtil.populate_redis_key(1533, "/key4")
        self.assert_redis_key(6381, "/key4")
        TestUtil.populate_redis_key(1533, "key/4")
        self.assert_redis_key(6381, "key/4")
        TestUtil.populate_redis_key(1533, "key4/")
        self.assert_redis_key(6383, "key4/")

        # Verify that // corresponds to the same hash.
        TestUtil.populate_redis_key(1533, "key4//")
        self.assert_redis_key(6383, "key4//")
        TestUtil.populate_redis_key(1533, "key4///")
        self.assert_redis_key(6383, "key4///")
        TestUtil.populate_redis_key(1533, "//key4", "teste")
        self.assert_redis_key(6383, "//key4", "teste")

        # Verify that /4/ corresponds to the same hash.
        TestUtil.populate_redis_key(1533, "4", "/value534")
        self.assert_redis_key(6381, "4", "/value534")
        TestUtil.populate_redis_key(1533, "key/4/", "/value5")
        self.assert_redis_key(6381, "key/4/", "/value5")
        TestUtil.populate_redis_key(1533, "adaerr/4/", "/value2")
        self.assert_redis_key(6381, "adaerr/4/", "/value2")

        # TODO: Verify hashtag pairs.

        # TODO: Verify that more than 2 chars in a hashtag is invalid.

    def test_auth(self):
        # Start a server with auth.password required.
        self.start_redis_server(6381, password="password1")
        self.start_delayer(incoming_port=6380, outgoing_port=6381, delay=1, admin_port=6382)

        # 1. Verify that with config with the correct auth config can access the server.
        self.start_proxy("tests/conf/auth1.toml", tag=1)
        TestUtil.verify_redis_connection(1531)

        # 2. Verify that with right config, the server can be disconnected, reconnected, and will still work.
        conn_to_delayer1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn_to_delayer1.connect(("0.0.0.0", 6382))
        conn_to_delayer1.sendall("SETDELAY 400")

        TestUtil.verify_redis_error(1531, "Proxy timed out")
        TestUtil.verify_redis_error(1531, "ERROR: Not connected")

        conn_to_delayer1.sendall("SETDELAY 1")
        time.sleep(1.5)
        TestUtil.verify_redis_connection(1531)

        # 3. Verify that with the right config, the proxy can be reconfigured to the wrong one, and will not work.
        r = redis.Redis(port=1530)
        response = r.execute_command("LOADCONFIG tests/conf/auth2.toml")
        self.assertTrue(response)
        response = r.execute_command("SWITCHCONFIG")
        self.assertTrue(response)
        TestUtil.verify_redis_error(1531, "ERROR: Not connected")

        response = r.execute_command("SHUTDOWN")
        self.assertTrue(response);

        # 4. Verify that without config, the server is considered down (unauthorized)
        self.start_proxy("tests/conf/testconfig1.toml", tag=2)
        TestUtil.verify_redis_error(1531, "NOAUTH Authentication required.")

        # 5. Verify that with no config, the proxy can be reconfigured to the correct one,and will work.
        r = redis.Redis(port=1530)
        response = r.execute_command("LOADCONFIG tests/conf/auth1.toml")
        self.assertTrue(response)
        response = r.execute_command("SWITCHCONFIG")
        self.assertTrue(response)
        time.sleep(1)
        TestUtil.verify_redis_connection(1531)

    def test_db(self):
        self.start_redis_server(6380)
        self.start_redis_server(6382)
        delayer = self.start_delayer(incoming_port=6381, outgoing_port=6380, delay=0, admin_port=6383)
        ports = [6380, 6382]
        r1 = redis.Redis(port=6380)
        r2 = redis.Redis(port=6380, db=1)
        r3 = redis.Redis(port=6380, db=2)

        # 1. Verify that db is selected properly.
        self.start_proxy("tests/conf/db1.toml")

        TestUtil.populate_redis_key(1531, "key1")
        TestUtil.populate_redis_key(1531, "key2")
        TestUtil.populate_redis_key(1531, "key3")
        TestUtil.populate_redis_key(1531, "key4")
        TestUtil.populate_redis_key(1531, "key5")

        response = r1.execute_command("DBSIZE")
        self.assertEquals(response, 0)
        response = r2.execute_command("DBSIZE")
        self.assertEquals(response, 3)
        response = r3.execute_command("DBSIZE")
        self.assertEquals(response, 0)

        TestUtil.flush_keys(ports)

        # 2. Verify switching configs with a different db.
        r = redis.Redis(port=1530)
        response = r.execute_command("LOADCONFIG tests/conf/db2.toml")
        self.assertTrue(response)
        response = r.execute_command("SWITCHCONFIG")
        self.assertTrue(response)

        TestUtil.populate_redis_key(1531, "key1")
        TestUtil.populate_redis_key(1531, "key2")
        TestUtil.populate_redis_key(1531, "key3")
        TestUtil.populate_redis_key(1531, "key4")
        TestUtil.populate_redis_key(1531, "key5")

        response = r1.execute_command("DBSIZE")
        self.assertEquals(response, 0)
        response = r2.execute_command("DBSIZE")
        self.assertEquals(response, 0)
        response = r3.execute_command("DBSIZE")
        self.assertEquals(response, 3)

        TestUtil.flush_keys(ports)

        # 3. Verify that disconnecting and reconnecting results in same db being used.
        delayer.sendall("SETDELAY 400")
        TestUtil.verify_redis_error(1531, "Proxy timed out", key="key1")
        TestUtil.verify_redis_error(1531, "ERROR: Not connected", key="key1")
        delayer.sendall("SETDELAY 0")
        time.sleep(2)

        TestUtil.populate_redis_key(1531, "key1")
        TestUtil.populate_redis_key(1531, "key2")
        TestUtil.populate_redis_key(1531, "key3")
        TestUtil.populate_redis_key(1531, "key4")
        TestUtil.populate_redis_key(1531, "key5")

        response = r1.execute_command("DBSIZE")
        self.assertEquals(response, 0)
        response = r2.execute_command("DBSIZE")
        self.assertEquals(response, 0)
        response = r3.execute_command("DBSIZE")
        self.assertEquals(response, 3)

    def test_redis_commands(self):
        self.start_redis_server(6381)
        self.start_redis_server(6382)
        self.start_redis_server(6383)
        self.start_redis_server(6384)
        self.start_proxy("tests/conf/multishard1.toml")

        r = redis.Redis(port=1533, socket_timeout=1)

        # TODO: Give proper error response to unsupported command.
        #self.assertEquals(r.execute_command("FAKE_COMMAND key1"), 0)

        # Test keys commands
        self.assertEquals(r.execute_command("DEL key1"), 0)
        self.assertEquals(r.execute_command("DUMP key1"), None)
        self.assertEquals(r.execute_command("EXISTS key1"), False)
        self.assertEquals(r.execute_command("EXPIRE key1 1"), False)
        self.assertEquals(r.execute_command("EXPIREAT key1 1"), False)
        self.assertEquals(r.execute_command("PERSIST key1"), False)
        self.assertEquals(r.execute_command("PEXPIREAT key1 1"), False)
        self.assertEquals(r.execute_command("PEXPIREAT key1 1"), False)
        self.assertEquals(r.execute_command("PTTL key1"), -2)
        try:
            r.execute_command("RESTORE key1 1 a")
            self.fail("Expected response error did not occur")
        except redis.ResponseError:
            pass
        self.assertEquals(r.execute_command("SORT key1"), [])
        self.assertEquals(r.execute_command("TOUCH key1"), 0)
        self.assertEquals(r.execute_command("TTL key1"), -2)
        self.assertEquals(r.execute_command("TYPE key1"), 'none')
        self.assertEquals(r.execute_command("UNLINK key1"), 0)

        # Test strings commands
        self.assertEquals(r.execute_command("APPEND key1 der"), 3)
        self.assertEquals(r.execute_command("BITCOUNT key1"), 11)
        self.assertEquals(r.execute_command("BITFIELD key1"), [])
        self.assertEquals(r.execute_command("BITPOS key1 0"), 0)
        self.assertEquals(r.delete("key1"), 1)
        self.assertEquals(r.execute_command("DECR key1"), -1)
        self.assertEquals(r.execute_command("DECRBY key1", 2), -3)
        self.assertEquals(r.execute_command("GET key1"), '-3')
        self.assertEquals(r.execute_command("GETBIT key 0"), 0)
        self.assertEquals(r.execute_command("GETRANGE key1 0 -1"), '-3')
        self.assertEquals(r.execute_command("GETSET key1 4"), '-3')
        self.assertEquals(r.execute_command("INCR key1"), 5)
        self.assertEquals(r.execute_command("INCRBY key1 2"), 7)
        self.assertEquals(r.execute_command("INCRBYFLOAT key1 0.5"), '7.5')
        # TODO: Add support for mset/mget
        #self.assertEquals(r.execute_command("MGET key1 key2"), ['7.5', None])
        #self.assertEquals(r.execute_command("MSET key1 2 key2 four"), True)
        self.assertEquals(r.execute_command("PSETEX key1 10000000 value"), 'OK')
        self.assertEquals(r.execute_command("SET key1 value2"), 'OK')
        self.assertEquals(r.execute_command("SETBIT key1 1 1"), 1)
        self.assertEquals(r.execute_command("SETEX key1 10000 value3"), 'OK')
        self.assertEquals(r.execute_command("SETNX key3 value4"), 1)
        self.assertEquals(r.execute_command("SETRANGE key3 2 brooerr"), 9)
        self.assertEquals(r.execute_command("STRLEN key3"), 9)

        # Test hashes commands
        self.assertEquals(r.execute_command("HDEL key5 hkey1"), 0)
        self.assertEquals(r.execute_command("HEXISTS key5 hkey1"), False)
        self.assertEquals(r.execute_command("HGET key5 hkey1"), None)
        self.assertEquals(r.execute_command("HGETALL key5"), [])
        self.assertEquals(r.execute_command("HINCRBY key5 hkey1 1"), 1)
        self.assertEquals(r.execute_command("HINCRBYFLOAT key5 hkey1 0.5"), '1.5')
        self.assertEquals(r.execute_command("HKEYS key5 "), ['hkey1'])
        self.assertEquals(r.execute_command("HLEN key5"), 1)
        self.assertEquals(r.execute_command("HMGET key5 hkey1 hkey2"), ['1.5', None])
        self.assertEquals(r.execute_command("HMSET key5 hkey1 1 hkey2 value2"), 'OK')
        self.assertEquals(r.execute_command("HSCAN key5 0"), ['0', ['hkey1', '1', 'hkey2', 'value2']])
        self.assertEquals(r.execute_command("HSET key5 hkey3 value3"), 1)
        self.assertEquals(r.execute_command("HSETNX key5 hkey3 derp"), 0)
        self.assertEquals(r.execute_command("HSTRLEN key5 hkey1"), 1)
        self.assertEquals(r.execute_command("HVALS key5"), ['1', 'value2', 'value3'])

        # Test lists commands
        r.lpush('key6', 'value1', 'value2');
        self.assertEquals(r.execute_command("BLPOP key6 1"), ['key6', 'value2'])
        self.assertEquals(r.execute_command("BRPOP key6 1"), ['key6', 'value1'])
        #self.assertEquals(r.execute_command("BRPOPLPUSH key6"), 1)
        self.assertEquals(r.execute_command("LINDEX key6 0"), None)
        self.assertEquals(r.execute_command("LINSERT key6 BEFORE a b"), 0)
        self.assertEquals(r.execute_command("LLEN key6"), 0)
        self.assertEquals(r.execute_command("LPOP key6"), None)
        self.assertEquals(r.execute_command("LPUSH key6 value1"), 1)
        self.assertEquals(r.execute_command("LPUSHX key6 value2"), 2)
        self.assertEquals(r.execute_command("LRANGE key6 0 -1"), ['value2', 'value1'])
        self.assertEquals(r.execute_command("LREM key6 1 value1"), 1)
        self.assertEquals(r.execute_command("LSET key6 0 value3"), 'OK')
        self.assertEquals(r.execute_command("LTRIM key6 2 2"), 'OK')
        self.assertEquals(r.execute_command("RPOP key6"), None)
        #self.assertEquals(r.execute_command("RPOPLPUSH key6"), 1)
        self.assertEquals(r.execute_command("RPUSH key6 value4"), 1)
        self.assertEquals(r.execute_command("RPUSHX key6 value5"), 2)

        # Test sets commands
        self.assertEquals(r.execute_command("SADD key7 s1"), 1)
        self.assertEquals(r.execute_command("SCARD key7"), 1)
        #self.assertEquals(r.execute_command("SDIFF key7"), 1)
        #self.assertEquals(r.execute_command("SDIFFSTORE key7"), 1)
        #self.assertEquals(r.execute_command("SINTER key7"), 1)
        #self.assertEquals(r.execute_command("SINTERSTORE key7"), 1)
        self.assertEquals(r.execute_command("SISMEMBER key7 s1"), True)
        self.assertEquals(r.execute_command("SMEMBERS key7"), ['s1'])
        #self.assertEquals(r.execute_command("SMOVE key7"), 1)
        self.assertEquals(r.execute_command("SPOP key7"), 's1')
        self.assertEquals(r.execute_command("SRANDMEMBER key7"), None)
        self.assertEquals(r.execute_command("SREM key7 s2"), 0)
        self.assertEquals(r.execute_command("SSCAN key7 0 "), ['0', []])
        #self.assertEquals(r.execute_command("SUNION key7"), 1)
        #self.assertEquals(r.execute_command("SUNIONSTORE key7"), 1)

        # Test sorted set commands
        r.zadd('key8', value1=1, value2=2)
        self.assertEquals(r.execute_command("BZPOPMAX key8 1"), ['key8', 'value2', '2'])
        self.assertEquals(r.execute_command("BZPOPMIN key8 1"), ['key8', 'value1', '1'])
        self.assertEquals(r.execute_command("ZADD key8 3 value3"), 1)
        self.assertEquals(r.execute_command("ZCARD key8"), 1)
        self.assertEquals(r.execute_command("ZCOUNT key8 0 1"), 0)
        self.assertEquals(r.execute_command("ZINCRBY key8 2 value3"), '5')
        #self.assertEquals(r.execute_command("ZINTERSTORE key8"), 1)
        self.assertEquals(r.execute_command("ZLEXCOUNT key8 - +"), 1)
        self.assertEquals(r.execute_command("ZPOPMAX key8"), ['value3', '5'])
        self.assertEquals(r.execute_command("ZPOPMIN key8"), [])
        self.assertEquals(r.execute_command("ZRANGE key8 0 -1"), [])
        self.assertEquals(r.execute_command("ZRANGEBYLEX key8 - +"), [])
        self.assertEquals(r.execute_command("ZRANGEBYSCORE key8 0 1"), [])
        self.assertEquals(r.execute_command("ZRANK key8 value3"), None)
        self.assertEquals(r.execute_command("ZREM key8 value1"), 0)
        self.assertEquals(r.execute_command("ZREMRANGEBYLEX key8 - +"), 0)
        self.assertEquals(r.execute_command("ZREMRANGEBYRANK key8 0 -1"), 0)
        self.assertEquals(r.execute_command("ZREMRANGEBYSCORE key8 0 1"), 0)
        self.assertEquals(r.execute_command("ZREVRANGE key8 0 -1"), [])
        self.assertEquals(r.execute_command("ZREVRANGEBYLEX key8 - +"), [])
        self.assertEquals(r.execute_command("ZREVRANGEBYSCORE key8 1 0"), [])
        self.assertEquals(r.execute_command("ZREVRANK key8 value1"), None)
        self.assertEquals(r.execute_command("ZSCAN key8 0"), ['0', []])
        self.assertEquals(r.execute_command("ZSCORE key8 value"), None)
        #self.assertEquals(r.execute_command("ZUNIONSTORE key8"), 1)

        # Test hyperloglog commands
        self.assertEquals(r.execute_command("PFADD key9 blar"), 1)
        self.assertEquals(r.execute_command("PFCOUNT key9"), 1)
        #self.assertEquals(r.execute_command("PFMERGE key9 key10"), 1)

        # No support for pub/sub commands

        # No support for discard/exec/multi/watch

        # Test cluster commands
        # See cluster tests.

        # Test geo commands
        self.assertEquals(r.execute_command("GEOADD key10 13.361389 38.115556 Palermo 15.087269 37.502669 Catania"), 2)
        self.assertEquals(r.execute_command("GEODIST key10 Palermo Catania"), "166274.1516")
        self.assertEquals(r.execute_command("GEOHASH key10 Palermo Catania"), ['sqc8b49rny0', 'sqdtr74hyu0'])
        self.assertEquals(r.execute_command("GEOPOS key10"), [])
        self.assertEquals(r.execute_command("GEORADIUS key10 15 37 100 km"), ['Catania'])
        self.assertEquals(r.execute_command("GEORADIUSBYMEMBER key10 Palermo 1 km"), ['Palermo'])

        # No support for server commands

        # Test streams commands
        # Not implemented yet.

        # No support for transactions commands

        # Test scripting commands
        # TODO: Implement scripting for single keys.
        #self.assertEquals(r.execute_command("EVAL key10"), 1)
        #self.assertEquals(r.execute_command("EVALSHA key10"), 1)
        #self.assertEquals(r.execute_command("SCRIPT DEBUG key10"), 1)
        #self.assertEquals(r.execute_command("SCRIPT EXISTS key10"), 1)
        #self.assertEquals(r.execute_command("SCRIPT FLUSH key10"), 1)
        #self.assertEquals(r.execute_command("SCRIPT KILL key10"), 1)
        #self.assertEquals(r.execute_command("SCRIPT LOAD key10"), 1)

        # Test connection commands
        # Not supported
        # auth
        # echo
        # ping
        # quit
        # select
        # swapdb

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

    def test_conhashing(self):
        pass
        # Test that consistent hashing works as expected
        # 1. Test that hashing is similar to twemproxy. (4 backends)
        # 2. Verify that when one backend is ejected (with auto_eject_host), all requests to other backends remain, but requests to the failed one gets moved.
        # 3. Verify the same happens when another backend is ejected. Verify it's the same as twemproxy.
        # 4. Verify recover of 2nd
        # 5. Verify recovery of first.
        # TODO: Set up a test while a constant stream of redis requests occurs. We want to make sure the switch is clean.

# Test cluster backends, no timeout. Verify sharding is correct.
    def test_cluster_no_timeout(self):
        # use redis cluster configs
        # place runtime configs in test/tmp folder
        # make surre ports 7000-7002 and 17000-17002 are open.
        # start up redis servers
        # start up redflareproxy
        # check sharding.
        self.start_redis_cluster_server(7000)
        self.start_redis_cluster_server(7001)
        self.start_redis_cluster_server(7002)
        self.initialize_redis_cluster([7000, 7001, 7002])
        self.start_proxy("tests/conf/cluster1.toml")

        TestUtil.verify_redis_connection(1533)

        TestUtil.populate_redis_key(1533, "key1")
        self.assert_redis_key(1533, "key1")

        TestUtil.populate_redis_key(1533, "key2")
        self.assert_redis_key(1533, "key2")

    def test_retry_connection(self):
        self.start_proxy("tests/conf/timeout1.toml")

        TestUtil.verify_redis_error(1531, "ERROR: Not connected")

        self.start_redis_server(6380)
        time.sleep(1) # TODO: Configure different retry frequencies besides 1 second

        TestUtil.verify_redis_connection(1531)

    def test_admin(self):
        self.start_proxy("tests/conf/timeout1.toml")

        r = redis.Redis(port=1530, decode_responses=True)
        response = r.execute_command("INFO")
        self.assertEqual(response.get('__raw__'), ["DERP"]);

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

# Test multiple backends, 
if __name__ == "__main__":
    TestRedFlareProxy.setupClass()
    unittest.main()