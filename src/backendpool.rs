use client::BufferedClient;
use stats::Stats;
use std::collections::VecDeque;
use backend::{write_to_client};
use bufreader::BufReader;
use redflareproxy::PoolTokenValue;
use redflareproxy::ClientTokenValue;
use backend::SingleBackend;
use redflareproxy::ClientToken;
use client::{Client};
use redflareproxy::ProxyError;
use redisprotocol::extract_redis_command;
use hash::hash;
use redflareproxy::BackendToken;
use redflareproxy::PoolToken;
use config::{Distribution, BackendPoolConfig};
use backend::{Backend};
use redisprotocol::{extract_key, RedisError, KeyPos};
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
    enable_advanced_commands: bool,
    pub name: String,

    // Cache list of backend tokens. Used for sharding purposes.
    pub cached_backend_shards: Rc<RefCell<Option<Vec<usize>>>>,

    // index corresponding to the first backend associated with this pool.
    pub first_backend_index: usize,
    pub num_backends: usize,

    pub listen_socket: Option<TcpListener>,
}

impl BackendPool {
    pub fn new(pool_name: String, pool_token: PoolToken, config: BackendPoolConfig, enable_advanced_commands: bool, first_backend_index: usize) -> BackendPool {
        debug!("PoolToken: {:?} for pool: {:?}", pool_token, pool_name);
        BackendPool {
            name: pool_name,
            token: pool_token,
            num_backends: config.servers.len(),
            config: config,
            enable_advanced_commands: enable_advanced_commands,
            first_backend_index: first_backend_index,
            listen_socket: None,
            cached_backend_shards: Rc::new(RefCell::new(None)),
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

        if self.config.warm_sockets {
            // There's an issue where new TcpStreams take 10-15 ms longer to be accepted. It appears to be due to some kind
            // of collection resizing, as it occurs at 64, 128, 256 connections.
            // This attempts to warm through 1024 sockets. It seems to work despite not connecting to anything.
            let mut temp_collection = Vec::with_capacity(1024);
            for _i in 0..1024 {
                let a = "0.0.0.0:0".parse().unwrap();
                temp_collection.push(mio::net::TcpStream::connect(&a));
            }
        }

        return Ok(());
    }

    pub fn accept_client_connection(
        &mut self,
        poll: &Rc<RefCell<Poll>>,
        next_client_token_value: &mut ClientTokenValue,
        clients: &mut HashMap<ClientTokenValue, (BufferedClient, PoolTokenValue)>,
        stats: &mut Stats,
    ) {
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
                            clients.insert(client_token.0, (BufReader::new(Client::new(stream)), self.token.0));
                            stats.accepted_clients += 1;
                            debug!("Backend Connection accepted: client {:?}", client_token);
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

// Based on the given command, determine which Backend to use, if any.
pub fn shard<'a>(
    cached_backend_shards: &mut Option<Vec<usize>>,
    config: &BackendPoolConfig,
    backends: &'a mut [Backend],
    key: &[u8]) -> Result<&'a mut Backend, RedisError> {
    let tag = get_tag(key, &config.hash_tag);

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

    if cached_backend_shards.is_none() {
        // Get total size:
        let mut total_weight = 0;
        for ref mut backend in backends.iter_mut() {
            if !config.auto_eject_hosts || backend.is_available() {
                total_weight += backend.weight;
            }
        }

        // mapping of weight to backend index.
        let mut mapping : Vec<usize> = Vec::with_capacity(total_weight);

        let mut index = 0;
        let mut backend_index = 0;
        for ref mut backend in backends.iter_mut() {
            if !config.auto_eject_hosts || backend.is_available() {
                for _i in index..index+backend.weight {
                    mapping.push(backend_index);
                }
                index += backend.weight;
            }
            backend_index += 1;
        }
        *cached_backend_shards = Some(mapping);
    }


    let total_weight = match cached_backend_shards {
        Some(mapping) => mapping.len(),
        None => { panic!("No cached backend mapping"); }
    };

    let shard: Result<usize, ProxyError> = match config.distribution {
        Distribution::Modula => Ok(hash(&config.hash_function, &tag) % total_weight), // Should be using key, not command.
        Distribution::Random => Ok(thread_rng().gen_range(0, total_weight - 1)),
        _ => panic!("Impossible to hit this with ketama!"),
    };
    match shard {
        Ok(shard_no) => {
            debug!("Sharding command tag to be {}", shard_no);
            {
                let backend_index = match cached_backend_shards {
                    Some(mapping) => mapping.get(shard_no).unwrap(),
                    None => { panic!("No cached backend mapping when getting backend"); }
                };
                debug!("Now got index: {:?}", backend_index);
                return Ok(backends.get_mut(*backend_index).unwrap());
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
    let a = match chars.next() {
        Some(a) => a,
        None => { panic!("Shouldn't be possible to hit, get_tag"); }
    };
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
            return match key.get(beginning..index) {
                Some(res) => res,
                None => { panic!("Shouldn't be possible to hit, get_tag exceeded bounds"); }
            };
        }
        index += 1;
    }
    return key;
}

pub fn handle_timeout(
    backend: &mut Backend,
    backend_token: BackendToken,
    clients: &mut HashMap<usize, (BufferedClient, usize)>,
    cluster_backends: &mut Vec<(SingleBackend, usize)>,
    completed_clients: &mut VecDeque<ClientTokenValue>,
    stats: &mut Stats,
) {
    if backend.handle_timeout(backend_token, clients, cluster_backends, completed_clients, stats) {
        mark_backend_down(backend, backend_token, clients, cluster_backends, completed_clients, stats);
    }
}

fn mark_backend_down(
    backend: &mut Backend,
    token: BackendToken,
    clients: &mut HashMap<usize, (BufferedClient, usize)>,
    cluster_backends: &mut Vec<(SingleBackend, usize)>,
    completed_clients: &mut VecDeque<ClientTokenValue>,
    stats: &mut Stats,
) {
    backend.handle_backend_failure(token, clients, cluster_backends, completed_clients, stats);
    //if self.config.auto_eject_hosts {
        //self.rebuild_pool_sharding();
    //}
}

pub fn handle_client_readable(
    backend_pool: &mut BackendPool,
    client: &mut BufferedClient,
    client_token: ClientToken,
    backends: &mut [Backend],
    cluster_backends: &mut Vec<(SingleBackend, usize)>,
    completed_clients: &mut VecDeque<ClientTokenValue>,
    stats: &mut Stats,
) -> bool {
    debug!("Handling client: {:?}", &client_token);

    // 1. Pull command from client.
    let buf_len = loop {
        let mut id = 0;
        let instant = std::time::Instant::now();
        let (buf_len, err_resp, more_buf) = {
            let buf = if client.fill_buf().is_ok() {
                    &client.buf[client.pos..client.cap]
                }
                else {
                    &[]
            };
            if buf.len() == 0 {
                // mark client as closed.
                (0, None, false) // Nothing. Mark it as closed. Mark as nothing?
            }
            else {
                debug!("Read from client:\n{:?}", std::str::from_utf8(buf));
                let mut err_resp: Option<&[u8]> = None;
                let (client_request, consumed_len): (&[u8], usize) = match extract_redis_command(buf) {
                    Ok(r) => (r, r.len()),
                    Err(err) => {
                        debug!("Invalid redis protocol: {:?}", err);
                        err_resp = Some(b"-ERROR: Invalid redis protocol\r\n");
                        (b"", buf.len())
                    }
                };
                debug!("Extracted from client:\n{:?}", std::str::from_utf8(&client_request));
                if client_request.len() > 0 {
                    stats.requests += 1;
                    match extract_key(&client_request) {
                        Ok(KeyPos::Single(key)) => {
                            let backend = shard(
                                &mut backend_pool.cached_backend_shards.borrow_mut(),
                                &mut backend_pool.config,
                                backends,
                                key
                            ).unwrap();
                            match backend.write_message(
                                &client_request,
                                client_token,
                                cluster_backends,
                                (instant, id),
                                stats
                            ) {
                                Ok(_) => {}
                                Err(err) => {
                                    debug!("Backend could not be written to. Received error: {}", err);
                                    err_resp = Some(b"-ERROR: Not connected\r\n");
                                }
                            };
                        }
                        Ok(KeyPos::Multi(vec)) => {
                            if !backend_pool.enable_advanced_commands {
                                err_resp = Some(b"-ProxyError: Advanced commands are currently disabled. They can be enabled by setting 'enable_advanced_commands' to true in the proxy config\r\n");
                            } else {
                                client.inner.pending_response = Vec::new();
                                client.inner.pending_count = vec.len();
                                for key in vec.iter() {
                                    id += 1;
                                    client.inner.pending_response.push(Vec::new());

                                    let backend = shard(
                                        &mut backend_pool.cached_backend_shards.borrow_mut(),
                                        &mut backend_pool.config,
                                        backends,
                                        key
                                    ).unwrap();
                                    let mut split_msg : Vec<u8> = Vec::with_capacity(25 + key.len());
                                    split_msg.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$");
                                    split_msg.extend_from_slice(&key.len().to_string().as_bytes());
                                    split_msg.extend_from_slice(b"\r\n");
                                    split_msg.extend_from_slice(key);
                                    split_msg.extend_from_slice(b"\r\n");

                                    match backend.write_message(
                                        &split_msg,
                                        client_token,
                                        cluster_backends,
                                        (instant, id),
                                        stats
                                    ) {
                                        Ok(_) => {}
                                        Err(err) => {
                                            debug!("Backend could not be written to when splitting. Received error: {}", err);
                                            let resp = b"-ERROR: Not connected\r\n";
                                            if write_to_client(
                                                &mut client.inner,
                                                &client_token.0,
                                                resp,
                                                (instant, id),
                                                completed_clients,
                                                stats
                                            ).is_err() {
                                                return false;
                                            };
                                        }
                                    };
                                }
                            }
                        }
                        Ok(KeyPos::MultiSet(vec)) => {
                            if !backend_pool.enable_advanced_commands {
                                err_resp = Some(b"-ProxyError: Advanced commands are currently disabled. They can be enabled by setting 'enable_advanced_commands' to true in the proxy config\r\n");
                            } else {
                                client.inner.pending_response = Vec::new();
                                client.inner.pending_count = vec.len();
                                for (key, args) in vec.iter() {
                                    id += 1;
                                    client.inner.pending_response.push(Vec::new());

                                    let backend = shard(
                                        &mut backend_pool.cached_backend_shards.borrow_mut(),
                                        &mut backend_pool.config,
                                        backends,
                                        key
                                    ).unwrap();
                                    let mut split_msg : Vec<u8> = Vec::with_capacity(35 + key.len() + args.len());
                                    split_msg.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$");
                                    split_msg.extend_from_slice(&key.len().to_string().as_bytes());
                                    split_msg.extend_from_slice(b"\r\n");
                                    split_msg.extend_from_slice(key);
                                    split_msg.extend_from_slice(b"\r\n$");
                                    split_msg.extend_from_slice(&args.len().to_string().as_bytes());
                                    split_msg.extend_from_slice(b"\r\n");
                                    split_msg.extend_from_slice(args);
                                    split_msg.extend_from_slice(b"\r\n");

                                    match backend.write_message(
                                        &split_msg,
                                        client_token,
                                        cluster_backends,
                                        (instant, id),
                                        stats
                                    ) {
                                        Ok(_) => {}
                                        Err(err) => {
                                            debug!("Backend could not be written to when splitting. Received error: {}", err);
                                            let resp = b"-ERROR: Not connected\r\n".to_vec();
                                            if write_to_client(
                                                &mut client.inner,
                                                &client_token.0,
                                                &resp,
                                                (instant, id),
                                                completed_clients,
                                                stats
                                            ).is_err() {
                                                return false;
                                            };
                                        }
                                    };
                                }
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
                        Err(RedisError::MissingArgsMget) => {
                            err_resp = Some(b"-wrong number of arguments for 'mget' command\r\n");
                        }
                        Err(RedisError::MissingArgsMset) => {
                            err_resp = Some(b"-wrong number of arguments for 'mset' command\r\n");
                        }
                        Err(RedisError::WrongArgsMset) => {
                            err_resp = Some(b"-wrong number of arguments for MSET\r\n");
                        }
                        Err(_reason) => {
                            debug!("Failed to shard: reason: {:?}", _reason);
                            err_resp = Some(b"-ERROR: Unknown proxy error\r\n");
                        }
                    };
                }
                let more_buf = buf.len() > client_request.len() && client.inner.pending_count == 0;
                (consumed_len, err_resp, more_buf)
            }
        };
        client.consume(buf_len);
        stats.recv_client_bytes += buf_len;


        match err_resp {
            None => {}
            Some(resp) => {
                debug!("Wrote to client error: {:?}: {:?}", client_token, std::str::from_utf8(resp));
                // Should check the written usize, and should retry if it's not all written.
                // Also, with an error, ErrorKind::Interrupted, the error is non-fatal, and the write attempt can be re-attempted.
                // Twemproxy tries forever if interrupted is received, or wouldblock, or EAGAIN
                if write_to_client(
                    client.get_mut(),
                    &client_token.0,
                    resp,
                    (instant, id),
                    completed_clients,
                    stats
                ).is_err() {
                    return false;
                };
            }
        }
        debug!("All done handling client! {:?}", buf_len);
        if more_buf {
            continue;
        } else {
            break buf_len;
        }

    };
    if buf_len == 0 {
        return false;
    }
    return true;
}