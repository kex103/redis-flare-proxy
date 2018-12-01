use redflareproxy::PoolTokenValue;
use redflareproxy::ClientTokenValue;
use backend::SingleBackend;
use redflareproxy::ClientToken;
use redflareproxy::FIRST_SOCKET_INDEX;
use redflareproxy::Client;
use redflareproxy::ProxyError;
use redisprotocol::extract_redis_command2;
use hash::hash;
use redflareproxy::BackendToken;
use redflareproxy::PoolToken;
use config::{Distribution, BackendPoolConfig};
use backend::{Backend};
use redisprotocol::{extract_key, RedisError};

use mio::*;
use mio::tcp::{TcpListener};
use std::string::String;
use std::io::{Write, BufRead};
use std::time::{Instant};
use hashbrown::HashMap;
use conhash::*;
use conhash::Node;

use rand::thread_rng;
use rand::Rng;

use std::cell::RefCell;
use std::rc::Rc;

use bufreader::BufReader;


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
    pub fn new(name: String, pool_token: PoolToken, config: BackendPoolConfig, first_backend_index: usize) -> BackendPool {
        debug!("Pool token: {:?}", pool_token);
        debug!("Pool name: {:?}", name);
        BackendPool {
            name: name,
            token: pool_token,
            num_backends: config.servers.len(),
            config: config,
            first_backend_index: first_backend_index,
            listen_socket: None,
        }
    }

    pub fn connect(
        &mut self,
        poll_registry: &mut Poll,
    ) {
        // Setup the server socket
        let addr = match self.config.listen.parse() {
            Ok(addr) => addr,
            Err(err) => {
                panic!("Failed to parse backendpool address {:?}: {:?}", self.config.listen, err);
            }
        };
        let server_socket = match TcpListener::bind(&addr) {
            Ok(soc) => soc,
            Err(err) => {
                panic!("Unable to bind to {:?}: {:?}", addr, err);
            }
        };

        debug!("Setup backend listener: {:?}", self.token);
        poll_registry.register(&server_socket, self.token, Ready::readable(), PollOpt::edge()).unwrap();
        self.listen_socket = Some(server_socket);
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
// We support Ketama, Modula, and Random.
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
        let hashed_index = consistent_hash.get(tag).unwrap().index;
        return Ok(backends.get_mut(hashed_index).unwrap());
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
    Err(RedisError::Unknown)
}

#[cfg(test)]
use cluster_backend::init_logging;
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
    num_pools: usize,
    num_backends: usize,
) {
    if backend.handle_timeout(backend_token, clients, cluster_backends, num_pools, num_backends) {
        mark_backend_down(backend, backend_token, clients, cluster_backends, num_pools, num_backends);
    }
}

fn mark_backend_down(
    backend: &mut Backend,
    token: BackendToken,
    clients: &mut HashMap<usize, (Client, usize)>,
    cluster_backends: &mut Vec<(SingleBackend, usize)>,
    num_pools: usize,
    num_backends: usize,
) {
    backend.handle_backend_failure(token, clients, cluster_backends, num_pools, num_backends);
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
    num_backends: usize,
) -> Result<bool, RedisError> {
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
                let client_request = try!(extract_redis_command2(buf));
                debug!("Extracted from client:\n{:?}", std::str::from_utf8(&client_request));


                // TODO: see if redis cluster needs a different token.
                // Perhaps the API should be.. write_backend(message_string, token)
                // A cluster needs... access to all possible connections.. ?
                // Or it has its own.
                let mut err_resp: Option<&[u8]> = None;
                match shard(pool_config, backends, &client_request) {
                    Ok(backend) => {
                        if !backend.write_message(&client_request, client_token, cluster_backends, num_backends) {
                            err_resp = Some(b"-ERROR: Not connected\r\n");
                        }
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
                    }
                }
                (client_request.len(), err_resp)
            }
        };
        client_stream.consume(buf_len);


        match err_resp {
            Some(resp) => {
                debug!("Wrote to client error: {:?}: {:?}", client_token, std::str::from_utf8(resp));
                let _ = client_stream.get_mut().write(resp);
            }
            None => {
            }
        }
        debug!("All done handling client! {:?}", buf_len);
        buf_len

    };
    if buf_len == 0 {
        return Ok(false);
    }
    return Ok(true);
}