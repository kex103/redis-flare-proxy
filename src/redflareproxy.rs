use backend::SingleBackend;
use backendpool::handle_timeout;
use backendpool::handle_client_readable;
use config::BackendConfig;
use bufreader::BufReader;
use mio::net::TcpStream;
use backend::Backend;
use backend::BackendEnum;
use admin;
use config::{RedFlareProxyConfig, BackendPoolConfig, load_config};
use backendpool;
use backendpool::BackendPool;
use mio::*;
use mio::unix::{UnixReady};
use std::mem;
use std::cell::{RefCell};
use std::rc::Rc;

use hashbrown::HashMap;

// For admin reqs.
use backend::parse_redis_command;
use toml;

// Reserved Token space.
pub const NULL_TOKEN: Token = Token(0);
pub const ADMIN_LISTENER: Token = Token(1);

// Pool Listeners
pub const FIRST_SOCKET_INDEX: usize = 10;

// Backend conns
// from FIRST_SOCKET_INDEX + backendpools.len

// Backend conns timeouts

// backend conns retry?

// Client conns.

pub const FIRST_CLUSTER_BACKEND_INDEX: usize = 1000000000;
// Cluster clients... start from reverse to end?

pub type BackendToken = Token;
pub type PoolToken = Token;
pub type ClientToken = Token;

pub type Client = BufReader<TcpStream>;

#[derive(Clone, Copy, Debug)]
enum SubType {
    Timeout,
    RequestTimeout,
    PoolServer,
    PoolListener,
    PoolClient,
    ClusterServer,
    AdminListener,
    AdminClient,
}

#[derive(Debug)]
pub enum ProxyError {
    InvalidConfig(String),
    PollFailure(String),
}

// High-level struct that contains everything for a redflareproxy instance.
pub struct RedFlareProxy {
    // This may just get integrated back into RedFlareProxy.
    pub admin: admin::AdminPort,

    // Configs
    pub config: RedFlareProxyConfig,
    pub staged_config: Option<RedFlareProxyConfig>,

    // Child structs.
    pub backendpools: Vec<BackendPool>,
    backends: Vec<Backend>,
    cluster_backends: Vec<(SingleBackend, usize)>,

    // Whenever a client closes, we reregister the last client to it.
    clients: Vec<(Client, usize)>,

    // Registry...
    poll: Rc<RefCell<Poll>>,
    running: bool,
}
impl RedFlareProxy {
    pub fn new(config_path: String) -> Result<RedFlareProxy, ProxyError> {
        let config = try!(load_config(config_path));
        let poll = match Poll::new() {
            Ok(poll) => Rc::new(RefCell::new(poll)),
            Err(error) => {
                return Err(ProxyError::PollFailure(format!("Failed to init poll: {:?}", error)));
            }
        };
        let admin = admin::AdminPort::new(config.admin.clone(), &poll.borrow());

        let num_pools = config.pools.len();

        let mut num_backends = 0;
        for (_, pool_config) in &config.pools {
            num_backends += pool_config.servers.len();
        }

        let mut redflareproxy = RedFlareProxy {
            admin: admin,
            backendpools: Vec::with_capacity(num_pools),
            backends: Vec::with_capacity(num_backends),
            cluster_backends: Vec::new(),
            clients: Vec::new(),
            config: config,
            staged_config: None,
            poll: poll,
            running: true,
        };
        // Populate backend pools.
        let pools_config = redflareproxy.config.pools.clone();
        let mut next_backend_token_value = FIRST_SOCKET_INDEX + num_pools;
        let mut pool_token_value = FIRST_SOCKET_INDEX;
        for (pool_name, pool_config) in pools_config {
            init_backend_pool(
                &mut redflareproxy.backendpools,
                &mut redflareproxy.backends,
                &pool_name,
                &pool_config,
                &mut redflareproxy.cluster_backends,
                &mut next_backend_token_value,
                pool_token_value,
                &mut redflareproxy.poll,
                num_backends,

            );
            pool_token_value += 1;
        }
        debug!("Initialized redflareproxy");

        Ok(redflareproxy)
    }

    pub fn switch_config(&mut self) -> Result<(), ProxyError> {
        if self.staged_config.is_none() {
            return Err(ProxyError::InvalidConfig("No staged config".to_owned()));
        }
        // Check that configs aren't the same.
        {
            match self.staged_config {
                Some(ref staged_config) => {
                    if staged_config == &self.config {
                        return Err(ProxyError::InvalidConfig("The configs are the same!".to_owned()));
                    }
                }
                None => {}
            }
        }
        let staged_config = mem::replace(&mut self.staged_config, None);
        self.config = staged_config.unwrap();

        // Replace admin.
        if self.config.admin != self.admin.config {
            let admin = admin::AdminPort::new(self.config.admin.clone(), &self.poll.borrow());
            self.admin = admin; // TODO: what to do with old admin?
        }

        let mut existing_clients: HashMap<String, Vec<Client>> = HashMap::new();
        for (client, pool_token_value) in self.clients.drain(0..) {
            // check listen socket of pool_token_value.
            let pool_index = pool_token_value - FIRST_SOCKET_INDEX;
            let listen_socket = self.backendpools.get_mut(pool_index).unwrap().config.listen.clone();
            if existing_clients.contains_key(&listen_socket) {
                existing_clients.get_mut(&listen_socket).unwrap().push(client);
            } else {
                let mut clients = Vec::new();
                clients.push(client);
                existing_clients.insert(listen_socket, clients);
            }
        }

            let (new_backends, new_clients) = {
                    let mut expired_pools = Vec::new();
                    let mut remaining_pools = HashMap::new();
                    let mut expired_backends: Vec<Backend> = Vec::new();
                    let mut remaining_backends = HashMap::new();
                    let mut backends_iter = self.backends.drain(0..);
                    for pool in self.backendpools.drain(0..) {
                                let mut should_keep = false;
                                {
                                    let ref config = pool.config;
                                    for (_, p_config) in self.config.pools.iter() {
                                        if p_config == config {
                                            should_keep = true;
                                            break;
                                        }
                                    }
                                }
                                if !should_keep {
                                    let num_backends = pool.num_backends;
                                    expired_pools.push(pool);
                                    for _ in 0..num_backends {
                                        expired_backends.push(backends_iter.next().unwrap());
                                    }
                                } else {
                                    let num_backends = pool.num_backends;
                                    let first_backend_index = pool.first_backend_index;
                                    remaining_pools.insert(pool.config.clone(), pool);
                                    for i in 0..num_backends {
                                        remaining_backends.insert(first_backend_index + i, backends_iter.next().unwrap());
                                    }
                                }
                    }
                    for _pool in expired_pools {
                        // dont need to clean up anything, i believe.
                    }

                    // now, try to remake.
                let num_pools = self.config.pools.len();
                let mut new_backendpools = Vec::with_capacity(num_pools);
                let mut num_backends = 0;
                for (_, pool_config) in &self.config.pools {
                    num_backends += pool_config.servers.len();
                }
                let mut new_backends = Vec::with_capacity(num_backends);
                let mut new_clients: Vec<(Client, usize)> = Vec::new();
                let mut new_cluster_backends: Vec<(SingleBackend, usize)> = Vec::new();
                // TODO: Implement cluster switching.

                let pools_config = self.config.pools.clone();
                let mut pool_token_value = FIRST_SOCKET_INDEX;
                let mut next_backend_token_value = FIRST_SOCKET_INDEX + num_pools;
                let mut next_client_token_value = FIRST_SOCKET_INDEX + num_pools + 3*num_backends;
                for (pool_name, pool_config) in pools_config {
                    // check if pool_config exists in remaining_pools. if it does, reregister it to the correct token.
                    match remaining_pools.remove(&pool_config) {
                        Some(mut pool) => {
                            // regregister pool token.
                            pool.token = Token(pool_token_value);
                            match pool.listen_socket {
                                Some(ref s) => {
                                    let _ = self.poll.borrow_mut().reregister(s, Token(pool_token_value), Ready::readable(), PollOpt::edge()).unwrap();
                                }
                                None => {}
                            }
                            // rename to the right name.
                            pool.name = pool_name.clone();

                            // move the existing backends.
                            let num_backends = pool.num_backends;
                            let first_backend_index = pool.first_backend_index;
                            for i in (first_backend_index..first_backend_index+num_backends).rev() {
                                let mut backend = remaining_backends.remove(&i).unwrap();
                                let _ = backend.reregister_token(Token(i), num_backends);

                                // also, rename pool token.
                                backend.change_pool_token(pool_token_value);
                                new_backends.push(backend);
                            }

                            pool.first_backend_index = next_backend_token_value;
                            next_backend_token_value += num_backends;

                            new_backendpools.push(pool);
                        }
                        None => {
                            init_backend_pool(
                                &mut new_backendpools,
                                &mut new_backends,
                                &pool_name,
                                &pool_config,
                                &mut new_cluster_backends,
                                &mut next_backend_token_value,
                                pool_token_value,
                                &mut self.poll,
                                num_backends,
                            );
                        }
                    }
                    match existing_clients.remove(&pool_config.listen) {
                        Some(mut clients) => {
                            for mut client in clients.drain(0..) {
                                let _ = self.poll.borrow_mut().reregister(client.get_ref(), Token(next_client_token_value), Ready::readable() | Ready::writable(), PollOpt::edge());
                                next_client_token_value += 1;
                                new_clients.push((client, pool_token_value));
                            }
                        }
                        None => {}
                    }

                    pool_token_value += 1;
                }

            self.backendpools = new_backendpools;
            (new_backends, new_clients)
            };


            self.backends = new_backends;

            self.clients = new_clients;
        Ok(())
    }

    pub fn run(&mut self) {
        let mut events = Events::with_capacity(1024);
        while self.running {
            match self.poll.borrow_mut().poll(&mut events, None) {
                Ok(_poll_size) => {}
                Err(error) => {
                    panic!("Error polling. Shutting down: {:?}", error);
                }
            };
            for event in events.iter() {
                debug!("Event detected: {:?} {:?}", &event.token(), event.readiness());
                self.handle_event(&event);
            }
        }
    }

    fn handle_event(&mut self, event: &Event) {
        let token = event.token();
        debug!("Event: {:?}", token);
        if event.readiness().contains(UnixReady::error()) {
            // TODO: Don't want to do mark backend down for client connections.
            /* Why does the errror occur? How does a backend socket just error? Timeout? Is this on establishing connection?*/
            // TODO: We want to make sure these tokens that fail are actualy backend tokens. It could be something else, like timers.
            let subscriber = self.identify_token(token);
            match subscriber {
                SubType::PoolServer => {
                    let token_id = token.0 - FIRST_SOCKET_INDEX - self.backendpools.len();
                    let num_pools = self.backendpools.len();
                    let num_backends = self.backends.len();
                    let backend = match self.backends.get_mut(token_id) {
                        Some(backend) => backend,
                        None => {
                            error!("Unable to find backend from token: {:?}", token);
                            return;
                        }
                    };
                    backend.handle_backend_failure(token, &mut self.clients, &mut self.cluster_backends, num_pools, num_backends);
                    return;
                }
                SubType::PoolClient => {
                    let num_pools = self.backendpools.len();
                    let num_backends = self.backends.len();
                    let client_index = convert_token_to_client_index(num_pools, num_backends, token.0);
                    {
                    let (client, _) = self.clients.get_mut(client_index).unwrap();
                    error!("Received client error: {:?} {:?}", client.get_ref().take_error(), token);
                    }

                    error!("Removed client because of error: {:?}", token);
                                        // remove client.
                    self.clients.swap_remove(client_index);
                    match self.clients.get_mut(client_index) {
                        None => {}
                        Some((c, _)) => {
                            self.poll.borrow_mut().reregister(c.get_ref(), token, Ready::readable(), PollOpt::edge()).unwrap();
                        }
                    }
                    return;
                }
                other => {
                    error!("Received other error: {:?} {:?}", other, token);
                }
            }
        }
        let subscriber = self.identify_token(token);

        match subscriber {
            SubType::Timeout => {
                debug!("RetryTimeout {:?}", token);
                let num_pools = self.backendpools.len();
                let num_backends = self.backends.len();
                let token_id = token.0 - FIRST_SOCKET_INDEX - num_pools - num_backends;
                match self.backends.get_mut(token_id) {
                    Some(backend) => {
                        backend.connect(&mut self.cluster_backends, num_backends)
                    }
                    None => error!("HashMap says it has token but it really doesn't! {:?}",token),
                }
            }
            SubType::RequestTimeout => {
                debug!("RequestTimeout {:?})", token);
                let num_pools = self.backendpools.len();
                let num_backends = self.backends.len();
                let backend_token = Token(token.0 - num_backends);
                let token_id = token.0 - FIRST_SOCKET_INDEX - num_pools - 2*num_backends;
                match self.backends.get_mut(token_id) {
                    Some(backend) => {
                        handle_timeout(backend, backend_token, &mut self.clients, &mut self.cluster_backends, num_pools, num_backends);
                    }
                    None => error!("HashMap says it has token but it really doesn't! {:?}", token),
                }
            }
            SubType::PoolListener => {
                debug!("PoolListener {:?}", token);
                let token_id = token.0 - FIRST_SOCKET_INDEX;
                let num_pools = self.backendpools.len();
                let num_backends = self.backends.len();
                match self.backendpools.get_mut(token_id) {
                    Some(pool) => pool.accept_client_connection(&self.poll, &mut self.clients, num_pools, num_backends),
                    None => error!("HashMap says it has token but it really doesn't!"),
                }
            }
            SubType::PoolClient => {
                debug!("PoolClient {:?}", token);
                let num_pools = self.backendpools.len();
                let num_backends = self.backends.len();
                let client_index = convert_token_to_client_index(num_pools, num_backends, token.0);
                let res = match self.clients.get_mut(client_index) {
                    Some((client, pool_token_value)) => {
                        let pool_index = *pool_token_value - FIRST_SOCKET_INDEX;
                        let pool_config = &self.backendpools.get(pool_index).unwrap().config;
                        let start_backend_index = self.backendpools.get(pool_index).unwrap().first_backend_index - FIRST_SOCKET_INDEX - num_pools;
                        let last_index = start_backend_index + self.backendpools.get(pool_index).unwrap().num_backends;
                        debug!("KEX: Start index: {:?} last_index: {:?}", start_backend_index, last_index);
                        let res = handle_client_readable(client, &pool_config, token, self.backends.get_mut(start_backend_index..last_index).unwrap(), &mut self.cluster_backends, num_backends);
                        res.unwrap()
                    }
                    None => {
                        error!("No client available? Index: {:?}", client_index);
                        return;
                    }
                };
                if res != true {
                    // remove client.
                    error!("Removed client because it expired: {:?}", token);
                    self.clients.swap_remove(client_index);
                    let num_clients = self.clients.len();
                    match self.clients.get_mut(client_index) {
                        None => {}
                        Some((c, pool_token_value)) => {
                            self.poll.borrow_mut().reregister(c.get_ref(), token, Ready::readable(), PollOpt::edge()).unwrap();
                            let pool_index = *pool_token_value - FIRST_SOCKET_INDEX;
                            let pool = self.backendpools.get(pool_index).unwrap();
                            let start_backend_index = pool.first_backend_index - FIRST_SOCKET_INDEX - num_pools;
                            let last_index = start_backend_index + pool.num_backends;
                            let old_client_token_value = num_clients + FIRST_SOCKET_INDEX + num_pools + 3*num_backends;
                            let new_client_token_value = token.0;
                            if new_client_token_value != old_client_token_value {
                                for backend in self.backends.get_mut(start_backend_index..last_index).unwrap().iter_mut() {
                                    match backend.single {
                                        BackendEnum::Single(ref mut b) => {
                                            for (ref mut element, _) in b.queue.iter_mut() {
                                                if element.0 == old_client_token_value {
                                                    element.0 = new_client_token_value;
                                                }
                                            }
                                        }
                                        _ => {
                                            panic!("not implemented for cluster yet");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // go thorugh pool of the client_index, and go through the queue,replacing all references of old client_index with new one.

                }
            }
            SubType::PoolServer => {
                debug!("PoolServer {:?}", token);
                let num_pools = self.backendpools.len();
                let num_backends = self.backends.len();
                let backend_index = convert_token_to_backend_index(num_pools, token.0);
                let mut next_cluster_token_value = FIRST_CLUSTER_BACKEND_INDEX + self.cluster_backends.len();
                match self.backends.get_mut(backend_index) {
                    Some(b) => b.handle_backend_response(token, &mut self.clients, &mut next_cluster_token_value, &mut self.cluster_backends, self.backendpools.len(), num_backends),
                    None => error!("HashMap says it has token but it really doesn't!"),
                }
            }
            SubType::ClusterServer => {
                debug!("ClusterServer {:?}", token);
                let num_pools = self.backendpools.len();
                let num_backends = self.backends.len();
                let cluster_index = convert_token_to_cluster_index(token.0);
                let pool_token_value = self.cluster_backends.get(cluster_index).unwrap().1;
                let backend_index = convert_token_to_backend_index(num_pools, pool_token_value);
                let mut next_cluster_token_value = FIRST_CLUSTER_BACKEND_INDEX + self.cluster_backends.len();
                self.backends.get_mut(backend_index).unwrap().handle_backend_response(token, &mut self.clients, &mut next_cluster_token_value, &mut self.cluster_backends, num_pools, num_backends);
            }
            SubType::AdminClient => {
                debug!("AdminClient {:?}", token);
                self.handle_client_socket(token);
            }
            SubType::AdminListener => {
                debug!("AdminListener {:?}", token);
                self.admin.accept_client_connection(2, &mut self.poll.borrow_mut());
            }
        }
        return;
    }

    pub fn get_current_config(&self) -> RedFlareProxyConfig {
        self.config.clone()
    }
    
    pub fn get_staged_config(&self) -> Option<RedFlareProxyConfig> {
        self.staged_config.clone()
    }

    fn handle_client_socket(&mut self, token: ClientToken) {
        let mut switching_config = false;
        let request = {
            let client_stream = match self.admin.client_sockets.get_mut(&token) {
                Some(stream) => stream,
                None => {
                    error!("AdminClient {:?} triggered an event, but it is no longer stored.", token);
                    return;
                }
            };
            parse_redis_command(client_stream)
        };
        debug!("RECEIVED COMMAND: {}", request);
        let mut lines = request.lines();
        let current_line = lines.next();
        let res = match current_line {
            None => {
                error!("AdminClient socket has nothing, when something was expected.");
                return;
            }
            Some("INFO") => {
                "DERP".to_owned()
            }
            Some("PING") => {
                "PONG".to_owned()
            }
            Some("LOADCONFIG") => {
                let next_line = lines.next();
                if next_line.is_none() {
                    "Missing filepath argument!".to_owned()
                } else {
                    let argument = next_line.unwrap();
                    let config = load_config(argument.to_owned()).unwrap();
                    self.staged_config = Some(config);
                    argument.to_owned()
                }
            }
            Some("SHUTDOWN") => {
                self.running = false;
               "OK".to_owned()
            }
            Some("STAGEDCONFIG") => {
                let staged_config = self.get_staged_config();
                if staged_config.is_none() {
                    "No config staged.".to_owned()
                } else {
                    toml::to_string(&staged_config).unwrap()
                }
            }
            Some("CONFIGINFO") => {
                toml::to_string(&self.get_current_config()).unwrap()
            }
            Some("SWITCHCONFIG") => {
                // TODO: Need to lose reference to the stream, OR
                // best is to orphan it. and respond OK.
                switching_config = true;
                // need to respond to socket later.switch_config(redflareproxy
                "OK".to_owned()
            }
            Some(unknown_command) => {
                debug!("Unknown command: {}", unknown_command);
                "Unknown command".to_owned()
            }
        };
        if !switching_config {
            let mut response = String::new();
            response.push_str("+");
            response.push_str(res.as_str());
            response.push_str("\r\n");
            debug!("RESPONSE: {}", &response);
            self.admin.write_to_client(token, response);
        }
        if switching_config {
            let result = {
                self.switch_config()
            };
            match result {
                Ok(_) => {
                    let response = "+OK\r\n".to_owned();
                    self.admin.write_to_client(token, response);

                }
                Err(ProxyError::InvalidConfig(message)) => {
                    let mut response = String::new();
                    response.push_str("-");
                    response.push_str(&message);
                    response.push_str("\r\n");
                    self.admin.write_to_client(token, response);
                }
                Err(_) => {
                    let mut response = String::new();
                    response.push_str("-Unknown admin error\r\n");
                    self.admin.write_to_client(token, response);
                }
            }
        }
    }

    fn identify_token(&mut self, token: Token) -> SubType {
        let num_pools = self.backendpools.len();
        let num_backends = self.backends.len();
        let ref value = token.0;
        if *value == 1 {
            return SubType::AdminListener;
        }
        if *value > 1 && *value < FIRST_SOCKET_INDEX {
            return SubType::AdminClient;
        }
        if *value >= FIRST_SOCKET_INDEX && *value < FIRST_SOCKET_INDEX + num_pools {
            return SubType::PoolListener;
        }
        if *value >= FIRST_SOCKET_INDEX + num_pools && *value < FIRST_SOCKET_INDEX + num_pools + num_backends {
            return SubType::PoolServer;
        }
        if *value >= FIRST_SOCKET_INDEX + num_pools + num_backends && *value < FIRST_SOCKET_INDEX + num_pools + 2*num_backends {
            return SubType::Timeout;
        }
        if *value >= FIRST_SOCKET_INDEX + num_pools + 2*num_backends && *value < FIRST_SOCKET_INDEX + num_pools + 3*num_backends {
            return SubType::RequestTimeout;
        }
        if *value >= FIRST_CLUSTER_BACKEND_INDEX {
            return SubType::ClusterServer;
        }
        return SubType::PoolClient;
        // TODO panic!("Need to identify between AdminClient");
    }
}

pub fn convert_token_to_client_index(num_pools: usize, num_backends: usize, token_value: usize) -> usize {
    return token_value - FIRST_SOCKET_INDEX - num_pools - 3*num_backends;
}

pub fn convert_token_to_backend_index(num_pools: usize, token_value: usize) -> usize {
    return token_value - FIRST_SOCKET_INDEX - num_pools;
}
pub fn convert_token_to_timeout_index(num_pools: usize, num_backends: usize, token_value: usize) -> usize {
    return token_value - FIRST_SOCKET_INDEX - num_pools - num_backends;
}
pub fn convert_token_to_requesttimeout_index(num_pools: usize, num_backends: usize, token_value: usize) -> usize {
    return token_value - FIRST_SOCKET_INDEX - num_pools - 2*num_backends;
}
pub fn convert_token_to_cluster_index(token_value: usize) -> usize {
    return token_value - FIRST_CLUSTER_BACKEND_INDEX;
}

fn init_backend_pool(
    backendpools: &mut Vec<BackendPool>,
    backends: &mut Vec<Backend>,
    pool_name: &String,
    pool_config: &BackendPoolConfig,
    cluster_backends: &mut Vec<(SingleBackend, usize)>,
    next_backend_token_value: &mut usize,
    pool_token_value: usize,
    poll: &Rc<RefCell<Poll>>,
    num_backends: usize,
) {
    let pool_token = Token(pool_token_value);
    let mut pool = backendpool::BackendPool::new(pool_name.clone(), pool_token, pool_config.clone(), *next_backend_token_value);

    let mut backend_token_value = *next_backend_token_value;

    *next_backend_token_value += pool_config.servers.len();
    
    pool.connect(&mut poll.borrow_mut());


    for backend_config in pool_config.servers.clone() {
        let backend = init_backend(backend_config, pool_config, cluster_backends, pool_token_value, backend_token_value, poll, num_backends);
        backends.push(backend);
        backend_token_value += 1;
        debug!("KEX: init_backend_pool Token_value now at {:?} backends len at {:?}", backend_token_value, backends.len());
    }

    backendpools.push(pool);
}

pub fn init_backend(
    backend_config: BackendConfig,
    pool_config: &BackendPoolConfig,
    cluster_backends: &mut Vec<(SingleBackend, usize)>,
    pool_token_value: usize,
    backend_token_value: usize,
    poll_registry: &Rc<RefCell<Poll>>,
    num_backends: usize,
) -> Backend {
    // Initialize backends.
    let backend_token = Token(backend_token_value);
    let mut next_cluster_token_value = FIRST_CLUSTER_BACKEND_INDEX + cluster_backends.len();
    let (mut backend, _all_backend_tokens) = Backend::new(
        backend_config,
        backend_token,
        cluster_backends,
        poll_registry,
        &mut next_cluster_token_value,
        pool_config.timeout,
        pool_config.failure_limit,
        pool_config.retry_timeout,
        pool_token_value,
    );
    backend.connect(cluster_backends, num_backends);
    return backend;
}
