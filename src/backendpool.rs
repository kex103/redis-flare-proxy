use redisprotocol::extract_redis_command;
use hash::hash;
use redflareproxy::BackendToken;
use redflareproxy::PoolToken;
use redflareproxy::{Subscriber};
use redflareproxy::{generate_backend_token, generate_client_token};
use config::{Distribution, BackendPoolConfig};
use backend::{Backend};
use redisprotocol::{extract_key, RedisError};

use mio::*;
use mio::tcp::{TcpListener, TcpStream};
use std::collections::{VecDeque};
use std::string::String;
use std::io::{Write, BufRead};
use std::time::{Instant};
use hashbrown::HashMap;
use conhash::*;
use conhash::Node;

use rand::thread_rng;
use rand::Rng;

use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;

use bufreader::BufReader;

#[derive(Clone)]
struct TokenNode {
    token: Token,
}

impl Node for TokenNode {
    fn name(&self) -> String {
        format!("{:?}", self.token)
    }
}

pub struct BackendPool {
    pub token: Token,
    pub config: BackendPoolConfig,
    pub name: String,
    // Cache list of backend tokens. Used for sharding purposes.
    pub backends: VecDeque<Token>,
    pub backend_map: HashMap<Token, Backend>,
    pub all_backend_tokens: HashMap<Token, Token>,
    pub client_sockets: HashMap<Token, BufReader<TcpStream>>,
    pub listen_socket: Option<TcpListener>,
}

impl BackendPool {
    pub fn new(name: String, pool_token: Token, config: BackendPoolConfig) -> BackendPool {
        debug!("Pool token: {:?}", pool_token);
        debug!("Pool name: {:?}", name);
        BackendPool {
            name: name,
            token: pool_token,
            config: config,
            backends: VecDeque::new(),
            backend_map: HashMap::new(),
            all_backend_tokens: HashMap::new(),
            client_sockets: HashMap::new(),
            listen_socket: None,
        }
    }

    pub fn connect(
        &mut self,
        backend_tokens_registry: &Rc<RefCell<HashMap<BackendToken, PoolToken>>>,
        next_socket_index: &Rc<Cell<usize>>,
        poll_registry: &Rc<RefCell<Poll>>,
        subscribers_registry: &Rc<RefCell<HashMap<Token, Subscriber>>>,
    ) {
        // Initialize backends.
        for backend_config in self.config.servers.clone() {
            let backend_token = generate_backend_token(next_socket_index, backend_tokens_registry, self.token);
            let (backend, all_backend_tokens) = Backend::new(
                backend_config,
                backend_token.clone(),
                backend_tokens_registry,
                subscribers_registry,
                poll_registry,
                next_socket_index,
                self.config.timeout,
                self.config.failure_limit,
                self.config.retry_timeout,
                self as &mut BackendPool,
            );
            self.backend_map.insert(backend_token.clone(), backend);
            for extra_token in all_backend_tokens {
                self.all_backend_tokens.insert(extra_token, backend_token.clone());
            }
            self.backends.push_back(backend_token);
        }

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
        poll_registry.borrow_mut().register(&server_socket, self.token, Ready::readable(), PollOpt::edge()).unwrap();
        subscribers_registry.borrow_mut().insert(self.token, Subscriber::PoolListener);
        self.listen_socket = Some(server_socket);

        // Connect backends.
        for (_, backend) in &mut self.backend_map {
            backend.connect();
        }
    }

    pub fn get_backend(&mut self, token: Token) -> &mut Backend {
        if self.backend_map.contains_key(&token) {
            return self.backend_map.get_mut(&token).unwrap();
        }
        let backend_token = self.all_backend_tokens.get(&token).unwrap();
        return self.backend_map.get_mut(backend_token).unwrap();
    }

    pub fn handle_timeout(
        &mut self,
        backend_token: Token,
        target_timestamp: Instant
    ) {
        if self.get_backend(backend_token).handle_timeout(backend_token, target_timestamp) {
            self.mark_backend_down(backend_token);
        }
    }

    fn mark_backend_down(
        &mut self,
        token: Token,
    ) {
        self.get_backend(token).handle_backend_failure(token);
        if self.config.auto_eject_hosts {
            self.rebuild_pool_sharding();
        }
    }

    fn rebuild_pool_sharding(&mut self) {
        // TODO: Implement this.
    }

    pub fn accept_client_connection(
        &mut self,
        next_socket_index: &Rc<Cell<usize>>,
        subscribers: &mut HashMap<Token, Subscriber>,
        poll: &Rc<RefCell<Poll>>,
        pool_token: Token
    ) {
        let socket = {    
            match self.listen_socket {
                Some(ref mut listener) => {
                    let socket = match listener.accept() {
                        Ok((socket, _)) => socket,
                        Err(err) => {
                            debug!("No more pending connections: {:?}", err);
                            return
                        }
                    };
                    socket
                }
                None => {
                    error!("Listen socket is no more when accepting!");
                    return
                }
            }
        };
        let client_token = generate_client_token(next_socket_index);
        match poll.borrow_mut().register(&socket, client_token, Ready::readable(), PollOpt::edge()) {
            Ok(_) => {

                self.client_sockets.insert(client_token, BufReader::new(socket));
                subscribers.insert(client_token, Subscriber::PoolClient(pool_token));
                info!("Backend Connection accepted: client {:?}", client_token);
            }
            Err(err) => {
                error!("Failed to register client token to poll: {:?}", err);
            }
        };

        // Try again to see if there were multiple pending client connections.
        // TODO: Should try using the TcpListener.incoming() iterator instead of being recursive?
        self.accept_client_connection(next_socket_index, subscribers, poll, pool_token);
    }

    pub fn handle_client_readable(
        &mut self,
        client_token: Token
    ) -> Result<(), RedisError> {
        debug!("Handling client: {:?}", &client_token);

        // 1. Pull command from client.
        let buf_len = {
            match self.client_sockets.get_mut(&client_token) {
                Some(stream) => {
                    let (buf_len, err_resp) = {
                        let buf = match stream.fill_buf() {
                            Ok(b) => b,
                            Err(_err) => { &[] }
                        };
                        if buf.len() == 0 {
                            // mark client as closed.
                            (0, None)
                        }
                        else {
                            debug!("Read from client:\n{:?}", std::str::from_utf8(buf));
                            let client_request = try!(extract_redis_command(buf));
                            debug!("Read from client:\n{:?}", std::str::from_utf8(&client_request));


                            // TODO: see if redis cluster needs a different token.
                            // Perhaps the API should be.. write_backend(message_string, token)
                            // A cluster needs... access to all possible connections.. ?
                            // Or it has its own.
                            let mut err_resp = None;
                            match shard(&self.config, &self.backends, &mut self.backend_map, &client_request) {
                                Ok(backend) => {
                                    if !backend.write_message(&client_request, client_token) {
                                        err_resp = Some("-ERROR: Not connected\r\n".to_owned());
                                    }
                                }
                                Err(RedisError::NoBackend) => {
                                    err_resp = Some("-ERROR: No backend\r\n".to_owned());
                                }
                                Err(RedisError::UnsupportedCommand) => {
                                    err_resp = Some("-ERROR: Unsupported command\r\n".to_owned());
                                }
                                Err(RedisError::InvalidScript) => {
                                    err_resp = Some("-ERROR: Scripts must have 1 key\r\n".to_owned());
                                }
                                Err(_reason) => {
                                    debug!("Failed to shard: reason: {:?}", _reason);
                                }
                            }
                            (client_request.len(), err_resp)
                        }
                    };
                    stream.consume(buf_len);


                    match err_resp {
                        Some(resp) => {
                            debug!("Wrote to client {:?}: {:?}", client_token, resp);
                            let _ = stream.get_mut().write(&resp.into_bytes()[..]);
                        }
                        None => {
                        }
                    }
                    debug!("All done handling client!");
                    buf_len


                }
                None => {
                    error!("Handle client called for expired socket!");
                    return Ok(());
                }
            }
        };
        if buf_len == 0 {
            self.client_sockets.remove(&client_token);
            return Ok(());
        }
        return Ok(());
    }

    // Fired when a reconnect is desired.
    pub fn handle_reconnect(
        &mut self,
        backend_token: Token
    ) {
        self.get_backend(backend_token).connect();
        info!("Attempted to reconnect: Backend {:?}", &backend_token);
    }
}

// Based on the given command, determine which Backend to use, if any.
// We support Ketama, Modula, and Random.
pub fn shard<'a>(
    config: &BackendPoolConfig,
    backends: &VecDeque<Token>,
    backend_map: &'a mut HashMap<Token, Backend>,
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
        for i in backends.iter() {
            let backend = backend_map.get_mut(i).unwrap();
            // 40 is pulled to match twemproxy's ketama.
            consistent_hash.add(&TokenNode {token: i.clone()}, backend.weight * 40);
            //consistent_hash.add(&TokenNode {token: i.clone()}, backend.weight);
            // TODO: Determine # of replicas. Most likely it's just the weight.
            // TODO: Check if there's any discrepancy between this key and twemproxy.
        }
        let hashed_token = consistent_hash.get(tag).unwrap().token;
        return Ok(backend_map.get_mut(&hashed_token).unwrap());
    }

    // Get total size:
    let mut total_weight = 0;
    for i in backends.iter() {
        let backend = backend_map.get_mut(i).unwrap();
        if !config.auto_eject_hosts || backend.is_available() {
            total_weight += backend.weight;
        }
    }

    let shard: Result<usize, String> = match config.distribution {
        Distribution::Modula => Ok(hash(&config.hash_function, &tag) % total_weight), // Should be using key, not command.
        Distribution::Random => Ok(thread_rng().gen_range(0, total_weight - 1)),
        _ => panic!("Impossible to hit this with ketama!"),
    };
    let mut answer = None;
    match shard {
        Ok(shard_no) => {
            debug!("Sharding command tag to be {}", shard_no);
            {
                let ref backends = backends;
                for backend_token in backends.iter() {
                    let backendbogus = backend_map.get_mut(backend_token).unwrap();
                    if config.auto_eject_hosts && !backendbogus.is_available() {
                        continue;
                    }
                    if shard_no >= total_weight - 1 {
                        answer = Some(backend_token.clone());
                        break;
                    }
                    total_weight -= backendbogus.weight;
                }
            }
            match answer {
                Some(backend_token) => return Ok(backend_map.get_mut(&backend_token).unwrap()),
                None => panic!("Overflowed when determining shard!"),
            }
        }
        Err(error) => debug!("Received {} while sharding!", error),
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

pub fn _parse_redis_response(stream: &mut BufReader<TcpStream>) -> &str {
    let buf = stream.fill_buf().unwrap();
    match buf.get(0).unwrap() {
        // $
        36u8 => {
            // Find the end. parse the integer.
            // Then parse X bytes from next end.
            // Expect the end.
        }
        // *
        42u8 => {}
        _ => {}
    };
    ""
}