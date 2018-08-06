/*#[macro_use]
extern crate log;
extern crate env_logger;
extern crate redis;
extern crate time;

use redis::Commands;
use redis::RedisError;
use std::process::Command;
use redis::RedisResult;
use std::io::Error as IOError;
use std::process::Output;
use std::process::Stdio;
use std::error::Error;

macro_rules! test {
    ($expr:expr) => (match $expr {
        Ok(val) => val,
        Err(err) => {
            println!("TEST ERROR: {:?}", err);
            assert!(false);
            return;
        }
    })
}

macro_rules! fail {
    ($expr:expr) => (match $expr {
        Ok(_) => {
            println!("Received Ok instead of expected error! ");
            assert!(false);
            return;
        },
        Err(err) => err,
    })
}

use std::{thread};
use std::time::Duration;

#[test]
fn test_starting() {
    println!("ABOUT TO START");
    setup_redis(6380);
    println!("VERIFYING");

    verify_redis_connection(6380, "");
    println!("going to set up proxy!");

    setup_proxy(String::from("tests/conf/testconfig1.toml"));
    println!("setting up proxy!");
    verify_redis_connection(1531, "");

    kill_process(6380);
    kill_process(1530);
}

#[test]
fn test_sharding() {
    // Uses a config that has 10 different shards.
    // Verify that redis commands go to the correct shards.
    setup_redis(6384);
    setup_redis(6381);
    setup_redis(6382);
    setup_redis(6383);

    verify_redis_connection(6384, "");
    verify_redis_connection(6381, "");
    verify_redis_connection(6382, "");
    verify_redis_connection(6383, "");

    setup_proxy(String::from("tests/conf/multishard1.toml"));

    verify_redis_connection(1533, "");
    verify_same_redis_backend(1533, 6384, "abc5");
    verify_same_redis_backend(1533, 6381, "a");
    verify_same_redis_backend(1533, 6382, "ab");
    verify_same_redis_backend(1533, 6383, "");

    kill_process(6384);
    kill_process(6381);
    kill_process(6382);
    kill_process(6383);
    kill_process(1532);
}

#[test]
fn test_timeouts() {
    kill_process(6390);
    setup_redis(6391);
    verify_redis_connection(6391, "");
    setup_delay(6390, 6391, 95);
    verify_redis_connection(6390, "");
    setup_proxy(String::from("tests/conf/timeout1.toml"));
    verify_redis_connection(1542, "");

    kill_process(6392);
    setup_redis(6393);
    verify_redis_connection(6393, "");
    setup_delay(6392, 6393, 105);
    verify_redis_connection(6392, "");
    setup_proxy(String::from("tests/conf/timeout2.toml"));
    verify_timeout(1544, "");

    kill_process(6390);
    kill_process(6391);
    kill_process(6392);
    kill_process(6393);
    kill_process(1541);
    kill_process(1543);
    // Have multiple pools.
    // Verify that 
    //assert_eq!(0, 1);
}

#[test]
fn test_failure_limits() {
    kill_process(6394);
    setup_redis(6395);
    setup_delay(6394, 6395, 100);
    verify_redis_connection(6394, "");
    setup_proxy(String::from("tests/conf/retrylimit1.toml"));
    verify_timeout(1546, "");
    verify_timeout(1546, "");
    verify_timeout(1546, "");
    kill_process(6394);
    kill_process(6395);;
    kill_process(1545);

}

#[test]
fn test_auto_eject_hosts() {
    // Test with 1 host, fail it, and see that there are no hosts.
    // Test with 3 hosts, fail it, and see that the sharding has been modified.
}

fn test_switchconfig() {
    // switch config from pool 1: with 3 destinations. to pool 2: which has 2 different destinations.
    // Switch config from pool 1: with 3 a, b, c, to pool 3: which has destinations a, b, d.
    // Switch config from pool 1: with 3 a, b, c, to pool 4: still a, b, c, but with some other stat.
    // Remove a pool.
    // Add a pool.
}

fn test_redisdb() {
    // Set redis_db to a value. Verify that proxy connects to that value.
    // Attempt to use select on the connections. 
}

fn test_redisauth() {
    // Test setting up a redis server with authentication required.
}

fn test_hashtags() {
    // Test setting hash tags.
}

fn test_serverlimit() {

}

fn test_auto_eject_hosts() {

}

fn test_server_weights() {
}

fn test_serverretrytimeout() {}

fn verify_redis_connection(port: usize, suffix: &'static str) {
    println!("Verifying redis connection: {}", port);
    let destination = format!("redis://127.0.0.1:{}/", port);
    let client = test!(redis::Client::open(destination.as_str()));
    let con = test!(client.get_connection());

    let key = format!("test{}{}", port, suffix);
    let test_key = key.as_str();

    let result : Option<String> = test!(con.get(test_key));
    assert_eq!(result, None);

    let msg = format!("{}:{}", port, suffix);
    let full_msg = msg.as_str();

    let result2 : bool = test!(con.set(test_key, full_msg));
    assert_eq!(result2, true);
    let result3 : String = test!(con.get(test_key));
    assert_eq!(result3, full_msg);
    let result4 : bool = test!(con.del(test_key));
    assert_eq!(result4, true);
}

fn verify_timeout(port: usize, suffix: &'static str) {
    println!("Verifying redis connection: {}", port);
    let destination = format!("redis://127.0.0.1:{}/", port);
    let client = test!(redis::Client::open(destination.as_str()));
    let con = test!(client.get_connection());

    let key = format!("test{}{}", port, suffix);
    let test_key = key.as_str();

    let result = fail!(con.get(test_key) as RedisResult<Option<String>>);
    println!("Failing result: {:?}, {:?} {:?}", result, result.description(), result.cause());
    assert_eq!(result.description(), "An error was signalled by the server");
}

fn verify_blackout(port:usize, suffix: &'static str) {
    println!("Verifying redis connection: {}", port);
    let destination = format!("redis://127.0.0.1:{}/", port);
    let client = test!(redis::Client::open(destination.as_str()));
    let con = test!(client.get_connection());

    let key = format!("test{}{}", port, suffix);
    let test_key = key.as_str();

    let result = fail!(con.get(test_key) as RedisResult<Option<String>>);
    println!("Failing result: {:?}, {:?} {:?}", result, result.description(), result.cause());

}

fn verify_same_redis_backend(port1: usize, port2: usize, suffix: &'static str) {
    let destination1 = format!("redis://127.0.0.1:{}/", port1);
    let destination2 = format!("redis://127.0.0.1:{}/", port2);
    let client1 = test!(redis::Client::open(destination1.as_str()));
    let client2 = test!(redis::Client::open(destination2.as_str()));
    let con1 = test!(client1.get_connection());
    let con2 = test!(client2.get_connection());

    let key = format!("test{}{}", port1, suffix);
    let test_key = key.as_str();

    let result : Option<String> = test!(con1.get(test_key));
    assert_eq!(result, None);
    let result : Option<String> = test!(con2.get(test_key));
    assert_eq!(result, None);

    let msg = format!("{}:{}", port1, suffix);
    let full_msg = msg.as_str();

    let result2 : bool = test!(con1.set(test_key, full_msg));
    assert_eq!(result2, true);
    let result3 : Option<String> = test!(con2.get(test_key));
    assert_eq!(result3.unwrap(), full_msg);
    let result4 : bool = test!(con2.del(test_key));
    assert_eq!(result4, true);

    let result : Option<String> = test!(con1.get(test_key));
    assert_eq!(result, None);
    let result : Option<String> = test!(con2.get(test_key));
    assert_eq!(result, None);
}

fn kill_process(port: usize) -> Result<Output, IOError> {
    // TODO: Need to actually check the output.
    let result = try!(Command::new("lsof")
        .arg(format!("-i:{}", port))
        .arg("-t")
        .arg("-sTCP:LISTEN")
        .output());
    let mut pid = String::from_utf8(result.stdout).unwrap();
    pid.pop();
    println!("PID: {:?}", pid);
    let result2 = Command::new("kill")
        .arg("-TERM")
        .arg(format!("{}", pid)).output();
    println!("RESULT: {:?}", result2);
    result2
}

fn setup_redis(port: usize) {
    kill_process(port);
    thread::sleep(Duration::from_millis(100));
    let status = Command::new("redis-server")
                            .arg("--port")
                            .arg(format!("{}", port))
                            .stdout(Stdio::null())
                            .spawn();

    let ten_millis = Duration::from_millis(100);
    thread::sleep(ten_millis);
    verify_redis_connection(port, "");
}

fn setup_proxy(config: String) {
    kill_process(1530);
    kill_process(1532);
    thread::sleep(Duration::from_millis(3000));
    println!("Setting up proxy: {:?}", time::now());
    let status = Command::new("cargo")
                            .arg("run")
                            .arg("--")
                            .arg("-c")
                            .arg(format!("{}", config))
                            .arg("-l")
                            .arg("DEBUG")
                            .arg("-o")
                            .arg(format!("tests/log/{}.log", config))
                            //.stdout(Stdio::null())
                            .spawn();

    thread::sleep(Duration::from_millis(5000));
    // TODO: build as first step. Then spawn process to run. That way we don't have to wait long for it to start up.
}

fn setup_delay(port1: usize, port2: usize, timeout: usize) {
    kill_process(port1);
    println!("Setting up delayer: {} to {} for {} ms", port1, port2, timeout);
    let mut current_dir = std::env::current_dir().unwrap();
    current_dir.push(std::path::PathBuf::from("tests/delayed_responder.py"));
    println!("{}", current_dir.to_str().unwrap());
    let status = Command::new("python")
                            .arg(format!("{}", current_dir.to_str().unwrap()))
                            .arg(format!("{}", port1))
                            .arg(format!("{}", port2))
                            .arg(format!("{}", timeout))
                            .spawn();
    thread::sleep(Duration::from_millis(500));
}*/