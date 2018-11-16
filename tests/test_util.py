#!/usr/bin/env python
import redis
from subprocess import call
import subprocess
import os
import sys
import time
import unittest
import socket

class TestUtil(unittest.TestCase):
    subprocesses = []
    proxy_admin_ports = []

    @classmethod
    def setupClass(self):
        TestUtil.build_proxy()

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

        # TODO: Verify that ports aren't being used.
       # kill_redis_server(6380)
        ##kill_redis_server(6381)
     #   kill_redis_server(6382)
      #  kill_redis_server(6383)
       # kill_redis_server(6384)
        #kill_redis_server(7000)
      #  kill_redis_server(7001)
       # kill_redis_server(7002)

    def tearDown(self):
        for proxy_admin_port in self.proxy_admin_ports:
            try:
                r = redis.Redis(port=proxy_admin_port)
                r.shutdown()
            except:
                pass

        for subprocess in self.subprocesses:
            try:
                subprocess.kill()
            except OSError:
                pass

    @staticmethod
    def build_proxy(args=[]):
        if call(["cargo", "build"] + args) != 0:
            raise AssertionError('Failed to compile RedFlareProxy with cargo')

    @staticmethod
    def kill_redis_server(port):
        try:
            r = redis.Redis(port=port)
            if r.ping():
                r.shutdown()
                print("Killed redis server at port: {}".format(port))
                return
        except redis.exceptions.ResponseError, e:
            # TODO: kill the instance
            pass
        except redis.exceptions.ConnectionError, e:
            pass

    @staticmethod
    def verify_redis_connection(port, key="test_key"):
        try:
            r = redis.Redis(port=port, socket_timeout=1)
            return_value = r.get(key)
            if return_value or return_value == None:
                return True
            raise AssertionError("Get key {} from port {} not successful!".format(key, port));
        except redis.exceptions.ConnectionError, e:
            raise AssertionError("Failed to connect to port: {}".format(port))

    @staticmethod
    def populate_redis_key(port, key="test_key", data="value"):
        try:
            r = redis.Redis(port=port, socket_timeout=1)
            return_value = r.set(key, data)
            if return_value:
                return True
            raise AssertionError("Set key {} at port {} not successful!".format(key, port));
        except redis.exceptions.ConnectionError, e:
            raise AssertionError("Failed to connect to port: {}".format(port))

    @staticmethod
    def expect_redis_key(port, key="test_key", data="value"):
        # if data is None, any value is fine, so long as the key exists in the given port.
        try:
            r = redis.Redis(port=port, socket_timeout=1)
            return_value = r.get(key)
            if return_value == None:
                return False
            return data==None or return_value == data
        except redis.exceptions.ConnectionError, e:
            raise AssertionError("Failed to connect to port: {}".format(port))

    @staticmethod
    def verify_redis_error(port, message=None, expect_conn_error=False, key="hi"):
        try:
            r = redis.Redis(port=port, socket_timeout=1)
            r.get(key)
            raise AssertionError("Expected error '{}' did not occur.".format(message))
        except redis.exceptions.ResponseError, e:
            if str(e) != message:
                raise AssertionError("Expected '{}', received '{}' instead".format(message, str(e)))
        except redis.ConnectionError, e:
            if not expect_conn_error:
                raise e

    @staticmethod
    def flush_keys(redis_ports):
        for redis_port in redis_ports:
            r = redis.Redis(port=redis_port, socket_timeout=1)
            r.flushall()

    # Function to help determine shardng to write tests.
    @staticmethod
    def determine_backend(key, ports):
        already_found = False
        for port in ports:
            if expect_redis_key(port, key, data=None):
                if already_found:
                    print("ERR: DUPLICATE KEY: {} FOUND AT PORT: {}\n".format(key, port))
                else:
                    print("KEY: {} FOUND AT PORT: {}\n".format(key, port))
                already_found = True
                break
        if not already_found:
            print("ERR: FAILED TO FIND KEY {} AT ANY PORT".format(key))

    def assert_redis_key(self, port, key="test_key", data="value"):
        self.assertTrue(TestUtil.expect_redis_key(port, key, data))

    def start_redis_server(self, port, password=None, config_path=None):
        FNULL = open(os.devnull, 'w')
        command = ["redis-server"]
        if config_path:
            command.append(str(config_path))
        if password:
            command = command + ["--requirepass", str(password)]
        command = command + ["--port", str(port)]
        process = subprocess.Popen(command, stdout=FNULL, stderr=subprocess.STDOUT)
        self.subprocesses.append(process)

        r = redis.Redis(port=port, password=password)
        attempts_remaining = 4
        while attempts_remaining:
            try:
                if not r.ping():
                    raise AssertionError('Redis server is unavailable at port: {}. Stopping test.'.format(port))
                return process
            except redis.ConnectionError:
                attempts_remaining = attempts_remaining - 1
                time.sleep(0.1)
        raise AssertionError('Redis server did not start at port: {}'.format(port))

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

    def start_proxy(self, config_path, tag=""):
        log_file = "tests/log/{}{}.stdout".format(self._testMethodName, tag)
        log_out = open(log_file, 'w')
        args = ["-c{}".format(
            config_path),
            "-l DEBUG"]
        env = os.environ.copy()
        env['RUST_BACKTRACE'] = '1'
        process = subprocess.Popen(["cargo", "run", "--bin", "redflareproxy", "--"] + args, stdout=log_out, stderr=subprocess.STDOUT, env=env)
        time.sleep(0.5); # TODO: wait until proxy initializes.
        self.subprocesses.append(process)
        # TODO: Get the port name to remove.
        self.proxy_admin_ports.append(1530)
        return process

    def start_delayer(self, incoming_port, outgoing_port, delay, admin_port=6400):
        log_out = open("tests/log/{}.delayer{}.stdout".format(self._testMethodName, incoming_port), 'w')
        FNULL = open(os.devnull, 'w')
        process = subprocess.Popen(
            ["python", "tests/delayed_responder.py", str(incoming_port), str(outgoing_port), str(delay), str(admin_port)],
            stdout=log_out
        )
        self.subprocesses.append(process)

        attempts_remaining = 3
        while attempts_remaining:
            try:
                admin_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                admin_conn.connect(("0.0.0.0", admin_port))
                return admin_conn
            except socket.error:
                time.sleep(0.1)
                attempts_remaining = attempts_remaining - 1
        raise AssertionError('Unable to connect to delayer admin_port {} after starting'.format(admin_port))