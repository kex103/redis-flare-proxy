use rustproxy::BackendToken;
use rustproxy::PoolToken;
use rustproxy::{StreamType, Subscriber};
use rustproxy::{generate_backend_token, generate_client_token};
use config::{Distribution, BackendPoolConfig};
use backend::{Backend};
use redisprotocol::{determine_modula_shard, extract_key};

use mio::*;
use mio::tcp::{TcpListener, TcpStream};
use std::collections::{VecDeque, HashMap};
use std::string::String;
use std::io::{Read, Write, BufRead};
use std::time::{Instant};
use bufstream::BufStream;

use conhash::*;
use conhash::Node;

use rand::thread_rng;
use rand::Rng;

use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;

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
    pub client_sockets: HashMap<Token, BufStream<TcpStream>>,
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
        written_sockets: &mut VecDeque<(Token, StreamType)>,
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
                written_sockets,
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

    // Based on the given command, determine which Backend to use, if any.
    // We support Ketama, Modula, and Random.
    pub fn shard(&mut self, command: &String) -> Option<&mut Backend> {
        let key = extract_key(command).unwrap();
        let tag = get_tag(key, &self.config.hash_tag);

        // TODO: Also, we want to cache the shardmap building if possible.

        // How does the ConsistentHashing library work?
        if self.config.distribution == Distribution::Ketama {
            let mut consistent_hash = conhash::ConsistentHash::new();
            for i in self.backends.iter() {
                let backend = self.backend_map.get_mut(i).unwrap();
                // 40 is pulled to match twemproxy's ketama.
                consistent_hash.add(&TokenNode {token: i.clone()}, backend.weight * 40);
                //consistent_hash.add(&TokenNode {token: i.clone()}, backend.weight);
                // TODO: Determine # of replicas. Most likely it's just the weight.
                // TODO: Check if there's any discrepancy between this key and twemproxy.
            }
            let hashed_token = consistent_hash.get(tag.as_bytes()).unwrap().token;
            return self.backend_map.get_mut(&hashed_token);
        }

        // Get total size:
        let mut total_weight = 0;
        for i in self.backends.iter() {
            let backend = self.backend_map.get_mut(i).unwrap();
            if !self.config.auto_eject_hosts || backend.is_available() {
                total_weight += backend.weight;
            }
        }

        let shard = match self.config.distribution {
            Distribution::Modula => determine_modula_shard(&tag, total_weight), // Should be using key, not command.
            Distribution::Random => Ok(thread_rng().gen_range(0, total_weight - 1)),
            _ => panic!("Impossible to hit this with ketama!"),
        };
        let mut answer = None;
        match shard {
            Ok(shard_no) => {
                debug!("Sharding command tag to be {}", shard_no);
                {
                    let ref backends = self.backends;
                    let ref mut backend_map = self.backend_map;
                    for backend_token in backends.iter() {
                        let backendbogus = backend_map.get_mut(backend_token).unwrap();
                        if self.config.auto_eject_hosts && !backendbogus.is_available() {
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
                    Some(backend_token) => return self.backend_map.get_mut(&backend_token),
                    None => panic!("Overflowed when determining shard!"),
                }
            }
            Err(error) => debug!("Received {} while sharding!", error),
        }
        None
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

                self.client_sockets.insert(client_token, BufStream::new(socket));
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
        written_sockets: &mut VecDeque<(Token, StreamType)>,
        client_token: Token
    ) {
        debug!("Handling client: {:?}", &client_token);

        // 1. Pull command from client.
        let client_request = {
            let mut buf = String::new();
            match self.client_sockets.get_mut(&client_token) {
                Some(stream) => {
                    let result = stream.read_to_string(&mut buf);
                    debug!("Read from client:\n{} ({})", buf, buf.len());
                    match result {
                        Ok(size) => {
                            debug!("Length: {}", size);
                            if size == 0 {
                                let _ = stream.read_to_string(&mut buf);
                                debug!("Read again: {}", buf);
                            }
                        }
                        Err(reason) => {
                            if buf.len() == 0 {
                                error!("Error: {}, {}", reason, buf);
                            }
                        }
                    }
                }
                None => {
                    error!("Handle client called for expired socket!");
                }
            }
            buf
        };
        // 1b. Handle client closing if it was just an empty message.
        if client_request.len() == 0 {
            self.client_sockets.remove(&client_token);
            // TODO: Clean up references to client token in rustproxy?
            info!("Closing client socket");
            return;
        }

        // TODO: see if redis cluster needs a different token.
        // Perhaps the API should be.. write_backend(message_string, token)
        // A cluster needs... access to all possible connections.. ?
        // Or it has its own.
        let success = {
            let backend = self.shard(&client_request).unwrap();
            backend.write_message(client_request, client_token)
        };

        if !success {
            let err_resp = "-ERROR: Not connected\r\n".to_owned();
            self.write_to_client(client_token, err_resp.to_owned(), written_sockets);
        }
        debug!("All done handling client!");
    }

    // Fired when a reconnect is desired.
    pub fn handle_reconnect(
        &mut self,
        backend_token: Token
    ) {
        self.get_backend(backend_token).connect();
        info!("Attempted to reconnect: Backend {:?}", &backend_token);
    }

    fn write_to_client(&mut self, client_token: Token, message: String, written_sockets: &mut VecDeque<(Token, StreamType)>,) {
        match self.client_sockets.get_mut(&client_token) {
            Some(stream) => {
                debug!("Wrote to client {:?}: {:?}", client_token, message);
                let _ = stream.write(&message.into_bytes()[..]);
                written_sockets.push_back((client_token.clone(), StreamType::PoolClient));
            }
            _ => panic!("Found listener instead of stream!"),
        }
    }
}

#[cfg(test)]
use cluster_backend::init_logging;
#[test]
fn test_hashtag() {
    init_logging();
    assert_eq!(get_tag("/derr/der".to_string(), &"/".to_string()), "derr".to_string());
    assert_eq!(get_tag("derr/der".to_string(), &"/".to_string()), "derr/der".to_string());
    assert_eq!(get_tag("derr<der>".to_string(), &"<>".to_string()), "der".to_string());
    assert_eq!(get_tag("der/r/der".to_string(), &"//".to_string()), "r".to_string());
    assert_eq!(get_tag("dberadearb".to_string(), &"ab".to_string()), "dear".to_string());
}

fn get_tag(command: String, tags: &String) -> String {
    if tags.len() == 0 {
        return command;
    }
    let ref mut chars = tags.chars();
    let a = chars.next().unwrap();
    let b = match chars.next() {
        Some(b) => b,
        None => a
    };
    let mut parsing_tag = false;
    let mut output = String::new();
    for cha in command.chars() {
        if !parsing_tag && cha == a {
            parsing_tag = true;
            continue;
        }
        if parsing_tag && cha == b {
            return output;
        }
        if parsing_tag {
            output.push(cha);
            continue;
        }
    }
    return command;
}

pub fn parse_redis_response(stream: &mut BufStream<TcpStream>) -> String {
    let mut string = String::new();
    let _ = stream.read_line(&mut string);
    match string.chars().next() {
        Some('$') => {
            if Some('-') == {
                let mut chars = string.chars();
                chars.next();
                chars.next()
            } {
                return string;
            }
            let bytes = {
                let mut next_line = String::from(&string[1..]);
                next_line.pop();
                next_line.pop();
                match next_line.parse::<usize>() {
                    Ok(int) => int,
                    Err(err) => {
                        error!("Could not parse array response length: {} {} {}", string, err, next_line);
                        0
                    }
                }
            };
            let mut buf = vec![0; bytes + 2];
            let _ = stream.read_exact(&mut buf);
            match String::from_utf8(buf) {
                Ok(result) => string.push_str(&result),
                Err(err) => error!("Could not parse from utf8 buffer: {}", err),
            }
        }
        Some('*') => {
            let lines = {
                let mut next_line = String::from(&string[1..]);
                next_line.pop();
                next_line.pop();
                match next_line.parse::<usize>() {
                    Ok(int) => int,
                    Err(err) => {
                        error!("Could not parse array response length: {} {} {}", string, err, next_line);
                        0
                    }
                }
            };
            for _ in 0..lines {
                let next_line = parse_redis_response(stream);
                string.push_str(&next_line);
            }
        }
        _ => {}
    }
    string
}