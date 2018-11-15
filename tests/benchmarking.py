#!/usr/bin/env python
import redis
from subprocess import call
from threading import Timer
import subprocess
import os
import sys
import time
import unittest

def kill_redis_server(port):
    try:
        r = redis.Redis(port=port)
        if r.ping():
            r.shutdown()
            print("Killed redis server at port: {}".format(port))
            return
    except redis.exceptions.ConnectionError, e:
        pass

class BenchmarkProxy(unittest.TestCase):

    subprocesses = []
    proxy_admin_ports = []

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
        self.build_proxy()

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
            except:
                pass

    def build_proxy(self):
        if call(["cargo", "build", "--release"]) != 0:
            raise AssertionError('Failed to compile RedFlareProxy with cargo')

    def start_redis_server(self, port):
        FNULL = open(os.devnull, 'w')
        process = subprocess.Popen(["redis-server", "--port", "{}".format(port)], stdout=FNULL, stderr=subprocess.STDOUT)

        time.sleep(1.0)
        r = redis.Redis(port=port)
        if not r.ping():
            raise AssertionError('Redis server is unavailable at port: {}. Stopping test.'.format(port))
        self.subprocesses.append(process)

    def start_proxy(self, config_path):
        log_file = "tests/log/{}.log".format(self.id())
        log_out = open("./tests/log/{}.log.stdout".format(self._testMethodName), 'w')
        args = ["-c {}".format(config_path),
            "-l ERROR"]
        env = os.environ.copy()
        env['RUST_BACKTRACE'] = '1'
        process = subprocess.Popen(["cargo", "run", "--bin", "redflareproxy", "--release", "--"] + args, stdout=log_out, stderr=subprocess.STDOUT, env=env)
        time.sleep(1);
        self.subprocesses.append(process)
        # Get the port name to remove.
        self.proxy_admin_ports.append(1530)

    def start_nutcracker(self, config_path):
        log_out = open("./tests/log/{}.log.stdout".format(self._testMethodName), 'w')
        process = subprocess.Popen(["./tests/bin/nutcracker", "-c", config_path, "-v", "0"], stdout=log_out, stderr=subprocess.STDOUT)
        self.subprocesses.append(process)

    def start_benchmarker(self, port, mock_port):
        log_out = open("./tests/log/{}.bencher.log".format(self._testMethodName), 'w')
        env = os.environ.copy()
        #env['RUST_LOG'] = 'DEBUG'
        process = subprocess.Popen(["cargo", "run", "--bin", "redflare-benchmark", "--release", "--", "-p", "{}".format(port), "-n", "{}".format(mock_port)], stdout=log_out, stderr=subprocess.STDOUT, env=env)
        self.subprocesses.append(process)
        return process

    def start_proxy_with_profiling(self, config_path):
        log_file = "tests/log/{}.log".format(self.id())
        log_out = open("./tests/log/{}.log.stdout".format(self._testMethodName), 'w')
        args = ["-c {}".format(config_path),
            "-l ERROR"]
        env = os.environ.copy()
        env['RUST_BACKTRACE'] = '1'
        process = subprocess.Popen(["cargo", "profiler", "callgrind", "--bin", "redflareproxy", "--release", "--"] + args, stdout=log_out, stderr=subprocess.STDOUT, env=env)
        time.sleep(1);
        self.subprocesses.append(process)
        # Get the port name to remove.
        self.proxy_admin_ports.append(1530)


    def run_redis_benchmark(self, port):
        log_out = open("tests/log/{}.log.stdout2".format(self._testMethodName), 'w')
        proc = subprocess.Popen(["redis-benchmark", "-p", "{}".format(port), "-t", "get,set", "-n", "600000"], stdout=log_out, stderr=subprocess.STDOUT)
        kill_proc = lambda p: self.kill_process_for_timeout(p)
        timer = Timer(25, kill_proc, [proc])
        timer.start()
        try:
            proc.wait()
        finally:
            timer.cancel()

    def kill_process_for_timeout(self, process):
        process.kill()
        self.fail("Spawned process failed to complete within the expected time. This means the performance is abysmally slow, or, more likely, the proxy failed to respond")

    def test_benchmark_nutcracker(self):
        kill_redis_server(6380)
        self.start_redis_server(6380)

        self.start_nutcracker("/home/kxiao/rustproxy/tests/conf/timeout1.yaml")
        self.run_redis_benchmark(1531)


    def test_benchmark_raw(self):
        kill_redis_server(6380)
        self.start_redis_server(6380)
        self.run_redis_benchmark(6380)

    def test_benchmark_single_backend(self):
        kill_redis_server(6380)
        self.start_redis_server(6380)

        self.start_proxy("/home/kxiao/rustproxy/tests/conf/timeout1.toml")
        self.run_redis_benchmark(1531)

    def test_benchmark_single_backend_with_profiling(self):
        kill_redis_server(6380)
        proc = self.start_benchmarker(1531, 6380)

        self.start_proxy_with_profiling("/home/kxiao/rustproxy/tests/conf/timeout1.toml")
        proc.wait()

    def test_benchmark_single_pool(self):
        kill_redis_server(6380)
        proc = self.start_benchmarker(1531, 6380)
        self.start_proxy("/home/kxiao/rustproxy/tests/conf/timeout1.toml")
        proc.wait()

    def test_benchmark_nutcracker_single_pool(self):
        kill_redis_server(6380)
        proc = self.start_benchmarker(1531, 6380)
        self.start_nutcracker("/home/kxiao/rustproxy/tests/conf/timeout1.yaml")
        proc.wait()

    def test_benchmark_failure_pool(self):
        pass

    def test_benchmark_multiple_pools(self):
        pass

    def test_benchmark_post_switch_config(self):
        pass

if __name__ == "__main__":
    unittest.main()