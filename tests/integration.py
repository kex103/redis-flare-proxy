#!/usr/bin/env python
import redis
from subprocess import call
import subprocess
import os
import sys
import time
import unittest
import socket

def get_script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))

def kill_redis_server(port):
    try:
        r = redis.Redis(port=port)
        if r.ping():
            r.shutdown()
            print("Killed redis server at port: {}".format(port))
            return
    except redis.exceptions.ConnectionError, e:
        pass

def build_rustproxy():
    if call(["cargo", "build"]) != 0:
        raise AssertionError('Failed to compile RustProxy with cargo')

def verify_redis_connection(port, key="test_key"):
    try:
        r = redis.Redis(port=port, socket_timeout=1)
        return_value = r.get(key)
        if return_value or return_value == None:
            return True
        raise AssertionError("Get key {} from port {} not successful!".format(key, port));
    except redis.exceptions.ConnectionError, e:
        raise AssertionError("Failed to connect to port: {}".format(port))

def populate_redis_key(port, key="test_key", data="value"):
    try:
        r = redis.Redis(port=port, socket_timeout=1)
        return_value = r.set(key, data)
        if return_value:
            return True
        raise AssertionError("Set key {} at port {} not successful!".format(key, port));
    except redis.exceptions.ConnectionError, e:
        raise AssertionError("Failed to connect to port: {}".format(port))

def expect_redis_key(port, key="test_key", data="value"):
    try:
        r = redis.Redis(port=port, socket_timeout=1)
        return_value = r.get(key)
        return return_value == data
    except redis.exceptions.ConnectionError, e:
        raise AssertionError("Failed to connect to port: {}".format(port))

def verify_redis_error(port, message):
    try:
        r = redis.Redis(port=port, socket_timeout=1)
        r.get("hi")
        raise AssertionError("Expected error '{}' did not occur.".format(message))
    except redis.exceptions.ResponseError, e:
        if str(e) != message:
            raise AssertionError("Expected '{}', received '{}' instead".format(message, str(e)))

class TestRustProxy(unittest.TestCase):
    subprocesses = []
    proxy_admin_ports = []

    @classmethod
    def setupClass(self):
        build_rustproxy()

    def setUp(self):
        log_file = "tests/log/{}.log".format(self.id())
        try:
            os.remove(log_file)
        except:
            pass
        files = os.listdir("tests/tmp")
        for f in files:
            if not os.path.isdir(f) and ".conf" in f:
                os.remove("tests/tmp/{}".format(f))

        kill_redis_server(6380)
        kill_redis_server(6381)
        kill_redis_server(6382)
        kill_redis_server(6383)
        kill_redis_server(6384)
        kill_redis_server(7000)
        kill_redis_server(7001)
        kill_redis_server(7002)

    def tearDown(self):
        for proxy_admin_port in self.proxy_admin_ports:
            try:
                r = redis.Redis(port=proxy_admin_port)
                r.shutdown()
            except e:
                pass

        for subprocess in self.subprocesses:
            subprocess.kill()

    def start_redis_server(self, port):
        FNULL = open(os.devnull, 'w')
        process = subprocess.Popen(["redis-server", "--port", "{}".format(port)], stdout=FNULL, stderr=subprocess.STDOUT)

        time.sleep(1.0)
        r = redis.Redis(port=port)
        if not r.ping():
            raise AssertionError('Redis server is unavailable at port: {}. Stopping test.'.format(port))
        self.subprocesses.append(process)

    def start_redis_cluster_server(self, port):
        FNULL = open(os.devnull, 'w')
        process = subprocess.Popen(["redis-server", "tests/conf/redis-cluster{}.conf".format(port)], stdout=FNULL, stderr=subprocess.STDOUT)

        time.sleep(1.0)
        r = redis.Redis(port=port)
        if not r.ping():
            raise AssertionError('Redis cluster server is unavailable at port: {}. Stopping test.'.format(port))
        self.subprocesses.append(process)

    def initialize_redis_cluster(self, ports):
        first_port = None
        for port in ports:
            if first_port == None:
                first_port = port
            else:
                r = redis.Redis(port=port)
                if not r.execute_command('CLUSTER MEET 127.0.0.1 {}'.format(first_port)):
                    raise AssertionError('Redis cluster server {} failed to meet {}. Stopping test.'.format(port, first_port))

        # set slots now.
        slots_per_instance = 16383 / len(ports)
        for i in range (1, 16383):
            port_index = i / slots_per_instance
            r = redis.Redis(port=ports[port_index])
            if not r.execute_command('CLUSTER ADDSLOTS {}'.format(i)):
                raise AssertionError('Redis cluster server {} failed to add slot {}. Stopping test.'.format(ports[port_index], i))
        time.sleep(1.0);

    def start_rustproxy(self, config_path):
        log_out = open("tests/log/{}.stdout".format(self._testMethodName), 'w')
        args = ["-c{}".format(
            config_path),
            "-l DEBUG"]
        env = os.environ.copy()
        env['RUST_BACKTRACE'] = '1'
        process = subprocess.Popen(["cargo", "run", "--bin", "rustproxy", "--"] + args, stdout=log_out, stderr=subprocess.STDOUT, env=env)
        time.sleep(1);
        self.subprocesses.append(process)
        # Get the port name to remove.
        self.proxy_admin_ports.append(1530)

    def start_delayer(self, incoming_port, outgoing_port, delay, admin_port=None):
        log_out = open("tests/log/{}.delayer.stdout".format(self._testMethodName), 'w')
        FNULL = open(os.devnull, 'w')
        process = subprocess.Popen(
            ["python", "tests/delayed_responder.py", str(incoming_port), str(outgoing_port), str(delay), str(admin_port)],
            stdout=log_out
        )
        self.subprocesses.append(process)


    def test_single_backend_no_timeout(self):
        self.start_redis_server(6380)
        self.start_rustproxy("tests/conf/testconfig1.toml")

        verify_redis_connection(1531)

    def test_single_backend_with_timeout(self):
        self.start_redis_server(6381)
        self.start_delayer(6380, 6381, 50)
        self.start_rustproxy("tests/conf/timeout1.toml")

        verify_redis_connection(1531)

    def test_single_backend_failed_timeout(self):
        self.start_redis_server(6381)
        self.start_delayer(6380, 6381, 101)
        self.start_rustproxy("tests/conf/timeout1.toml")

        verify_redis_error(1531, "RustProxy timed out")

    def test_single_backend_ejected(self):
        self.start_redis_server(6381)
        self.start_delayer(6380, 6381, 2, 6382)
        self.start_rustproxy("tests/conf/retrylimit1.toml")

        verify_redis_connection(1531)

        # Set a long delay, so that all requests to the backend will time out.
        conn_to_delayer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn_to_delayer.connect(("0.0.0.0", 6382))
        conn_to_delayer.sendall("SETDELAY 401")
        # Verify that requests time out. After the 3rd failure, the backend is blacklisted.
        verify_redis_error(1531, "RustProxy timed out")
        verify_redis_error(1531, "RustProxy timed out")
        verify_redis_error(1531, "RustProxy timed out")
        verify_redis_error(1531, "ERROR: Not connected")
        verify_redis_error(1531, "ERROR: Not connected")
        conn_to_delayer.sendall("BLOCK_NEW_CONNS 750")
        time.sleep(1)
        # TODO: Have delayer stop listening to all server sockets for a bit.

        # Set a short delay, so requests will stop timing out.
        conn_to_delayer.sendall("SETDELAY 2")
        # Wait a second, to give time for the proxy to detect that the backend is available again.
        time.sleep(1)
        # Verify that requests succeed now.
        verify_redis_connection(1531)
        verify_redis_connection(1531)
        verify_redis_connection(1531)

# unsuccessful, no backends, no timeout.

# test a backend responding with just a partial response and then failing to ever respond.
    def test_partial_response_timeout(self):
        pass

# Test unsuccessful, single backend that is down, no timeout.

# Test successful basic, single backend, yes timeout.



# Test successful, multiple (4) backends, no timeout. verify that the sharding is correct.
    def test_multiple_backend_no_timeout(self):
        self.start_redis_server(6381)
        self.start_redis_server(6382)
        self.start_redis_server(6383)
        self.start_redis_server(6384)
        self.start_rustproxy("tests/conf/multishard1.toml")

        verify_redis_connection(1533)

        populate_redis_key(6381, "key1")
        self.assertTrue(expect_redis_key(1533, "key1"))
        populate_redis_key(6384, "key2")
        self.assertTrue(expect_redis_key(1533, "key2"))

    def test_multiple_backend_with_timeout(self):
        self.start_redis_server(6381)
        self.start_redis_server(6382)
        self.start_redis_server(6383)
        self.start_redis_server(6380)
        self.start_delayer(6384, 6380, 101)
        self.start_rustproxy("tests/conf/multishard2.toml")

        verify_redis_connection(1533)

        populate_redis_key(6384, "key1")
        verify_redis_error(1533, "RustProxy timed out")

# Test cluster backends, no timeout. Verify sharding is correct.
    def test_cluster_no_timeout(self):
        # use redis cluster configs
        # place runtime configs in test/tmp folder
        # make surre ports 7000-7002 and 17000-17002 are open.
        # start up redis servers
        # start up rustproxy
        # check sharding.
        self.start_redis_cluster_server(7000)
        self.start_redis_cluster_server(7001)
        self.start_redis_cluster_server(7002)
        self.initialize_redis_cluster([7000, 7001, 7002])
        self.start_rustproxy("tests/conf/cluster1.toml")

        verify_redis_connection(1533)

        populate_redis_key(1533, "key1")
        self.assertTrue(expect_redis_key(1533, "key1"))

        populate_redis_key(1533, "key2")
        self.assertTrue(expect_redis_key(1533, "key2"))

    def test_retry_connection(self):
        self.start_rustproxy("tests/conf/timeout1.toml")

        verify_redis_error(1531, "ERROR: Not connected")

        self.start_redis_server(6380)
        time.sleep(0.5)

        verify_redis_connection(1531)

    def test_admin(self):
        self.start_rustproxy("tests/conf/timeout1.toml")

        r = redis.Redis(port=1530, decode_responses=True)
        response = r.execute_command("INFO")
        self.assertEqual(response.get('__raw__'), ["DERP"]);

    def test_switch_config(self):
        self.start_redis_server(6380)
        self.start_redis_server(6381)
        self.start_rustproxy("tests/conf/timeout1.toml")

        r = redis.Redis(port=1530)
        response = r.execute_command("LOADCONFIG tests/conf/timeout1.toml")
        self.assertTrue(response)

        populate_redis_key(6380, "key1")
        self.assertTrue(expect_redis_key(1531, "key1"))

        try:
            r.execute_command("SWITCHCONFIG")
            self.fail("Expected failure from SWITCHCONFIG")
        except redis.ResponseError, e:
            self.assertEqual(str(e), "The configs are the same!")

        # Same admin port, different listen_port.
        response = r.execute_command("LOADCONFIG tests/conf/swapconfig2.toml")
        self.assertTrue(response)

        populate_redis_key(6380, "key2")
        self.assertTrue(expect_redis_key(1531, "key2"))

        try:
            response = r.execute_command("SWITCHCONFIG")
        except redis.ConnectionError, e:
            pass

        populate_redis_key(6380, "key3")
        self.assertTrue(expect_redis_key(1532, "key3"))

        # Same admin port, same listener, different backend.
        r = redis.Redis(port=1530)
        response = r.execute_command("LOADCONFIG tests/conf/swapconfig3.toml")
        self.assertTrue(response)

        try:
            response = r.execute_command("SWITCHCONFIG")
        except redis.ConnectionError, e:
            pass
        populate_redis_key(6381, "key4")
        self.assertTrue(expect_redis_key(1532, "key4"))


        # Different admin port, new pools.
        r = redis.Redis(port=1530)
        response = r.execute_command("LOADCONFIG tests/conf/swapconfig4.toml")
        self.assertTrue(response)

        try:
            response = r.execute_command("SWITCHCONFIG")
        except redis.ConnectionError, e:
            pass
        populate_redis_key(6381, "key5")
        self.assertTrue(expect_redis_key(1532, "key5"))

        r = redis.Redis(port=1540)
        response = r.execute_command("LOADCONFIG tests/conf/swapconfig3.toml")
        self.assertTrue(response)

# Test multiple backends, 
if __name__ == "__main__":
    build_rustproxy()
    unittest.main()