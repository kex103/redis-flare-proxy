#!/usr/bin/env python
import redis
from subprocess import call
from threading import Timer
import subprocess
import os
import sys
import time
import unittest
from test_util import TestUtil

class BenchmarkProxy(TestUtil):

    @classmethod
    def setupClass(self):
        TestUtil.build_proxy(['--release'])

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
        process = subprocess.Popen(["cargo", "run", "--release", "--bin", "redflare-benchmark", "--", "-p", "{}".format(port), "-n", "{}".format(mock_port)], stdout=log_out, stderr=subprocess.STDOUT, env=env)
        self.subprocesses.append(process)
        return process

    def start_proxy_with_profiling(self, config_path):
        log_file = "tests/log/{}.log".format(self.id())
        log_out = open("./tests/log/{}.log.stdout".format(self._testMethodName), 'w')
        args = [
            "-c {}".format(config_path),
            "-l DEBUG",
            "-o kex.out"
        ]
        env = os.environ.copy()
        env['RUST_BACKTRACE'] = '1'
        process = subprocess.Popen(["cargo", "profiler", "callgrind", "--release", "--bin", "target/release/redflareproxy", "--"] + args, stdout=log_out, stderr=subprocess.STDOUT, env=env)
        self.subprocesses.append(process)
        # Get the port name to remove.
        self.proxy_admin_ports.append(1530)

        attempts_remaining = 10
        while attempts_remaining:
            try:
                r = redis.Redis(port=1531)
                r.get("key")
                return process
            except redis.ConnectionError:
                time.sleep(0.5)
                attempts_remaining = attempts_remaining - 1
                continue
        raise AssertionError("Timed out waiting for proxy to start.")

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
        self.start_redis_server(6380)

        self.start_nutcracker(TestUtil.script_dir() + "/conf/timeout1.yaml")
        self.run_redis_benchmark(1531)


    def test_benchmark_raw_redis(self):
        self.start_redis_server(6380)
        self.run_redis_benchmark(6380)

    def test_benchmark_single_backend(self):
        self.start_redis_server(6380)

        self.start_proxy(TestUtil.script_dir() + "/conf/timeout1.toml")
        self.run_redis_benchmark(1531)

    def test_benchmark_single_backend_with_profiling(self):
        proc = self.start_benchmarker(1531, 6380)

        proxy_proc = self.start_proxy_with_profiling(TestUtil.script_dir() + "/conf/timeout1.toml")
        proc.wait()
        r = redis.Redis(port=1530)
        r.execute_command("SHUTDOWN")
        proxy_proc.wait()

    def test_benchmark_single_pool(self):
        proc = self.start_benchmarker(1531, 6380)
        self.start_proxy(TestUtil.script_dir() + "/conf/timeout1.toml")
        proc.wait()

    def test_benchmark_nutcracker_single_pool(self):
        proc = self.start_benchmarker(1531, 6380)
        self.start_nutcracker(TestUtil.script_dir() + "/conf/timeout1.yaml")
        proc.wait()

    def test_benchmark_failure_pool(self):
        pass

    def test_benchmark_multiple_pools(self):
        pass

    def test_benchmark_post_switch_config(self):
        pass

if __name__ == "__main__":
    BenchmarkProxy.setupClass()
    unittest.main()