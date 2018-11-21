#!/usr/bin/env python
import redis
from test_util import TestUtil

class CommandTests(TestUtil):

    def test_script_commands(self):
        self.start_redis_server(6381)
        self.start_redis_server(6382)
        self.start_redis_server(6383)
        self.start_redis_server(6384)
        self.start_proxy("tests/conf/multishard1.toml")

        r = redis.Redis(port=1533, socket_timeout=1)

        # Test scripting commands
        script = "return redis.call('set',KEYS[1],ARGV[1])"
        # execute_command doesn't seem to work with eval properly as a single string.
        self.assertEquals(r.eval(script, 1, 'key10', 'value10'), 'OK')
        self.assertEquals(r.get('key10'), 'value10')

        # Verify scripts with more than 1 keys are rejected.
        script = "return 3"
        try:
            self.assertEquals(r.eval(script, 2, 'key10', 'key11'), True)
            self.fail("Expected response error did not occur")
        except redis.ResponseError, e:
            self.assertEquals(str(e), "ERROR: Scripts must have 1 key")

        # Verify scripts with multi lines
        script = "local a = redis.call('set',KEYS[1],ARGV[1])\r\nreturn 3"
        self.assertEquals(r.eval(script, 1, 'key10', 'value11'), 3)
        self.assertEquals(r.get('key10'), 'value11')

        # Remaining commands are unimplemented.
        #script = "local a = redis.call('set',KEYS[1],ARGV[1])\r\nreturn 3"
        #self.assertEquals(r.execute_command("SCRIPT LOAD \"{}\"".format(script)), 'cb4332')
        #self.assertEquals(r.execute_command("EVALSHA cb4332 key10 value12"), 3)
        #self.assertEquals(r.get('key10'), 'value12')
        #self.assertEquals(r.execute_command("SCRIPT DEBUG key10"), 1)
        #self.assertEquals(r.execute_command("SCRIPT EXISTS key10"), 1)
        #self.assertEquals(r.execute_command("SCRIPT FLUSH key10"), 1)
        #self.assertEquals(r.execute_command("SCRIPT KILL key10"), 1)

    def test_redis_commands(self):
        self.start_redis_server(6381)
        self.start_redis_server(6382)
        self.start_redis_server(6383)
        self.start_redis_server(6384)
        self.start_proxy("tests/conf/multishard1.toml")

        r = redis.Redis(port=1533, socket_timeout=1)

        # TODO: Give proper error response to unsupported command.
        try:
            r.execute_command("FAKE_COMMAND key1")
            self.fail("Expected response error did not occur")
        except redis.ResponseError, e:
            self.assertEquals(str(e), "ERROR: Unsupported command")

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

        # Test connection commands
        # Not supported
        # auth
        # echo
        # ping
        # quit
        # select
        # swapdb