#!/usr/bin/env python
import time
import socket
import redis
from test_util import TestUtil

class StatsTests(TestUtil):

    def test_stats(self):
        self.start_redis_server(6381)
        self.start_delayer(6380, 6381, 50)
        self.start_proxy("tests/conf/timeout1.toml")

        TestUtil.verify_redis_connection(1531)
        r = redis.Redis(port=1530, socket_timeout=1)
        response = r.execute_command("STATS")
        self.assertEqual(
            response,
            """Stats:
accepted_clients: 1
client_connections: 0
requests: 1
responses: 1
send_client_bytes: 5
recv_client_bytes: 27
send_backend_bytes: 33
recv_backend_bytes: 12"""
        );


        TestUtil.populate_redis_key(1531, "key2")
        response = r.execute_command("STATS")
        self.assertEqual(
            response,
            """Stats:
accepted_clients: 2
client_connections: 0
requests: 2
responses: 2
send_client_bytes: 10
recv_client_bytes: 61
send_backend_bytes: 67
recv_backend_bytes: 17"""
        );

