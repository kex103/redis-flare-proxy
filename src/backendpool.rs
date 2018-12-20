use backend::write_to_stream;
use redflareproxy::PoolTokenValue;
use redflareproxy::ClientTokenValue;
use backend::SingleBackend;
use redflareproxy::ClientToken;
use redflareproxy::Client;
use redflareproxy::ProxyError;
use redisprotocol::extract_redis_command;
use hash::hash;
use redflareproxy::BackendToken;
use redflareproxy::PoolToken;
use config::{Distribution, BackendPoolConfig};
use backend::{Backend};
use redisprotocol::{extract_key, RedisError};

use mio::*;
use mio::tcp::{TcpListener};
use std::string::String;
use std::io::{BufRead};
use hashbrown::HashMap;
use conhash::*;
use conhash::Node;

use rand::thread_rng;
use rand::Rng;

use std::cell::RefCell;
use std::rc::Rc;

use std::io::BufReader;

#[derive(Clone)]
struct IndexNode {
    index: usize,
}
impl Node for IndexNode {
    fn name(&self) -> String {
        format!("{:?}", self.index)
    }
}

pub struct BackendPool {
    pub token: PoolToken,
    pub config: BackendPoolConfig,
    pub name: String,
    // Cache list of backend tokens. Used for sharding purposes.

    // index corresponding to the first backend associated with this pool.
    pub first_backend_index: usize,
    pub num_backends: usize,

    pub listen_socket: Option<TcpListener>,
}

impl BackendPool {
    pub fn new(pool_name: String, pool_token: PoolToken, config: BackendPoolConfig, first_backend_index: usize) -> BackendPool {
        debug!("PoolToken: {:?} for pool: {:?}", pool_token, pool_name);
        BackendPool {
            name: pool_name,
            token: pool_token,
            num_backends: config.servers.len(),
            config: config,
            first_backend_index: first_backend_index,
            listen_socket: None,
        }
    }

    /*
        Attempts to establish the pool by binding to the listening socket, and registering to the event poll.
        If this process fails, an error is returned.
    */
    pub fn connect(&mut self, poll_registry: &mut Poll) -> Result<(), ProxyError> {
        // Setup the server socket
        let addr = self.config.listen;
        let server_socket = match TcpListener::bind(&addr) {
            Ok(soc) => soc,
            Err(err) => {
                return Err(ProxyError::PoolBindSocketFailure(addr, err));
            }
        };

        debug!("Setup backend listener: {:?}", self.token);
        match poll_registry.register(&server_socket, self.token, Ready::readable(), PollOpt::edge()) {
            Ok(_) => {}
            Err(err) => {
                return Err(ProxyError::PoolPollFailure(err));
            }
        };
        self.listen_socket = Some(server_socket);
        return Ok(());
    }

    pub fn accept_client_connection(
        &mut self,
        poll: &Rc<RefCell<Poll>>,
        next_client_token_value: &mut ClientTokenValue,
        clients: &mut HashMap<ClientTokenValue, (Client, PoolTokenValue)>,
    ) {
        loop {
            match self.listen_socket {
                Some(ref mut listener) => {
                    loop {
                        let mut stream = match listener.accept() {
                            Ok(s) => s.0,
                            Err(e) => {
                                if e.kind() == std::io::ErrorKind::WouldBlock {
                                    return;
                                }
                                panic!("Failed for some reason {:?}", e);
                            }
                        };
                        let client_token = Token(*next_client_token_value);
                        *next_client_token_value += 1;
                        match poll.borrow_mut().register(&stream, client_token, Ready::readable(), PollOpt::edge()) {
                            Ok(_) => {
                                clients.insert(client_token.0, (BufReader::new(stream), self.token.0));
                                info!("Backend Connection accepted: client {:?}", client_token);
                            }
                            Err(err) => {
                                error!("Failed to register client token to poll: {:?}", err);
                            }
                        };
                    }
                }
                None => {
                    error!("Listen socket is no more when accepting!");
                    return
                }
            }
        }
    }
}

// Based on the given command, determine which Backend to use, if any.
pub fn shard<'a>(
    config: &BackendPoolConfig,
    backends: &'a mut [Backend],
    command: &[u8]) -> Result<&'a mut Backend, RedisError> {
    let key = match extract_key(command) {
        Ok(key) => key,
        Err(err) => {
            return Err(err);
        }
    };
    let tag = get_tag(key, &config.hash_tag);

    // TODO: Also, we want to cache the shardmap building if possible.

    // How does the ConsistentHashing library work?
    if config.distribution == Distribution::Ketama {
        let mut consistent_hash = conhash::ConsistentHash::new();
        let mut i = 0;
        for backend in backends.iter() {
            // 40 is pulled to match twemproxy's ketama.
            consistent_hash.add(&IndexNode{index: i}, backend.weight * 40);
            //consistent_hash.add(&TokenNode {token: i.clone()}, backend.weight);
            // TODO: Determine # of replicas. Most likely it's just the weight.
            // TODO: Check if there's any discrepancy between this key and twemproxy.
            i += 1;
        }
        let hashed_index = match consistent_hash.get(tag) {
            Some(n) => n.index,
            None => {
                return Err(RedisError::NoBackend);
            }
        };
        match backends.get_mut(hashed_index) {
            Some(b) => {
                return Ok(b);
            }
            None => {
                error!("Consistent hashing hashed to a nonexistent backend! Index: {}. This should never happen. Please contact author.", hashed_index);
                return Err(RedisError::NoBackend);
            }
        }
    }

    // Get total size:
    let mut total_weight = 0;
    for ref mut backend in backends.iter_mut() {
        if !config.auto_eject_hosts || backend.is_available() {
            total_weight += backend.weight;
        }
    }

    let shard: Result<usize, ProxyError> = match config.distribution {
        Distribution::Modula => Ok(hash(&config.hash_function, &tag) % total_weight), // Should be using key, not command.
        Distribution::Random => Ok(thread_rng().gen_range(0, total_weight - 1)),
        _ => panic!("Impossible to hit this with ketama!"),
    };
    match shard {
        Ok(shard_no) => {
            debug!("Sharding command tag to be {}", shard_no);
            {
                for backend in backends.iter_mut() {
                    if config.auto_eject_hosts && !backend.is_available() {
                        continue;
                    }
                    if shard_no >= total_weight - 1 {
                        return Ok(backend);
                    }
                    total_weight -= backend.weight;
                }
            }
        }
        Err(error) => debug!("Received {:?} while sharding!", error),
    }
    Err(RedisError::NoBackend)
}

#[cfg(test)]
use init_logging;
#[test]
fn test_hashtag() {
    init_logging();
    assert_eq!(get_tag(b"/derr/der", &"/".to_string()), b"derr");
    assert_eq!(get_tag(b"derr/der", &"/".to_string()), b"derr/der");
    assert_eq!(get_tag(b"derr<der>", &"<>".to_string()), b"der");
    assert_eq!(get_tag(b"der/r/der", &"//".to_string()), b"r");
    assert_eq!(get_tag(b"dberadearb", &"ab".to_string()), b"dear");
}

fn get_tag<'a>(key: &'a [u8], tags: &String) -> &'a [u8] {
    if tags.len() == 0 {
        return key;
    }
    let ref mut chars = tags.chars();
    let a = chars.next().unwrap();
    let b = match chars.next() {
        Some(b) => b,
        None => a
    };
    let mut parsing_tag = false;
    let mut beginning = 0;
    let mut index = 0;
    for &cha in key {
        if !parsing_tag && cha == a as u8 {
            parsing_tag = true;
            beginning = index + 1;
        }
        else if parsing_tag && cha == b as u8 {
            return key.get(beginning..index).unwrap();
        }
        index += 1;
    }
    return key;
}

pub fn handle_timeout(
    backend: &mut Backend,
    backend_token: BackendToken,
    clients: &mut HashMap<usize, (Client, usize)>,
    cluster_backends: &mut Vec<(SingleBackend, usize)>,
) {
    if backend.handle_timeout(backend_token, clients, cluster_backends) {
        mark_backend_down(backend, backend_token, clients, cluster_backends);
    }
}

fn mark_backend_down(
    backend: &mut Backend,
    token: BackendToken,
    clients: &mut HashMap<usize, (Client, usize)>,
    cluster_backends: &mut Vec<(SingleBackend, usize)>,
) {
    backend.handle_backend_failure(token, clients, cluster_backends);
    //if self.config.auto_eject_hosts {
        //self.rebuild_pool_sharding();
    //}
}

pub fn handle_client_readable(
    client_stream: &mut Client,
    pool_config: &BackendPoolConfig,
    client_token: ClientToken,
    backends: &mut [Backend],
    cluster_backends: &mut Vec<(SingleBackend, usize)>,
) -> bool {
    debug!("Handling client: {:?}", &client_token);

    // 1. Pull command from client.
    let buf_len = {
        let (buf_len, err_resp) = {
            let buf = match client_stream.fill_buf() {
                Ok(b) => b,
                Err(_err) => { &[] }
            };
            if buf.len() == 0 {
                // mark client as closed.
                (0, None)
            }
            else {
                debug!("Read from client:\n{:?}", std::str::from_utf8(buf));
                let mut err_resp: Option<&[u8]> = None;
                let client_request = match extract_redis_command(buf) {
                    Ok(r) => r,
                    Err(err) => {
                        debug!("Invalid redis protocol: {:?}", err);
                        err_resp = Some(b"-ERROR: Invalid redis protocol[");
                        b""
                    }
                };
                debug!("Extracted from client:\n{:?}", std::str::from_utf8(&client_request));


                // TODO: see if redis cluster needs a different token.
                // Perhaps the API should be.. write_backend(message_string, token)
                // A cluster needs... access to all possible connections.. ?
                // Or it has its own.
                match shard(pool_config, backends, &client_request) {
                    Ok(backend) => {
                        match backend.write_message(&client_request, client_token, cluster_backends) {
                            Ok(_) => {}
                            Err(err) => {
                                debug!("Backend could not be written to. Received error: {}", err);
                                err_resp = Some(b"-ERROR: Not connected\r\n");
                            }
                        };
                    }
                    Err(RedisError::NoBackend) => {
                        err_resp = Some(b"-ERROR: No backend\r\n");
                    }
                    Err(RedisError::UnsupportedCommand) => {
                        err_resp = Some(b"-ERROR: Unsupported command\r\n");
                    }
                    Err(RedisError::InvalidScript) => {
                        err_resp = Some(b"-ERROR: Scripts must have 1 key\r\n");
                    }
                    Err(_reason) => {
                        debug!("Failed to shard: reason: {:?}", _reason);
                        err_resp = Some(b"-ERROR: Unknown proxy error");
                    }
                }
                (client_request.len(), err_resp)
            }
        };
        client_stream.consume(buf_len);


        match err_resp {
            None => {}
            Some(resp) => {
                debug!("Wrote to client error: {:?}: {:?}", client_token, std::str::from_utf8(resp));
                // Should check the written usize, and should retry if it's not all written.
                // Also, with an error, ErrorKind::Interrupted, the error is non-fatal, and the write attempt can be re-attempted.
                // Twemproxy tries forever if interrupted is received, or wouldblock, or EAGAIN
                if write_to_stream(client_stream, resp).is_err() {
                    return false;
                }
            }
        }
        debug!("All done handling client! {:?}", buf_len);
        buf_len

    };
    if buf_len == 0 {
        return false;
    }
    return true;
}