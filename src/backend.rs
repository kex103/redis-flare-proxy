use client::BufferedClient;
use stats::Stats;
use redflareproxy::ClientTokenValue;
use redisprotocol::WriteError;
use redflareproxy::PoolTokenValue;
use std::net::SocketAddr;
use hashbrown::HashMap;
use redflareproxy::ClientToken;
use redflareproxy::BackendToken;
use client::Client;
use bufreader::BufReader;
use redflareproxy::{NULL_TOKEN};
use config::BackendConfig;
use mio::*;
use mio_more::timer::{Timer, Builder};
use mio::tcp::{TcpStream};
use std::collections::{VecDeque};
use std::string::String;
use std::io::{Read, Write, BufRead};
use std::time::Duration;
use std::time::Instant;
use std::cell::RefCell;
use std::rc::Rc;
use cluster_backend::{ClusterBackend};
use redisprotocol::extract_redis_command;
use redisprotocol::RedisError;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BackendStatus {
    READY,
    CONNECTED,
    DISCONNECTED,
    CONNECTING,
    LOADING,
}

pub enum BackendEnum {
    Single(SingleBackend),
    Cluster(ClusterBackend),
}

pub struct Backend {
    pub weight: usize,
    pub single: BackendEnum,
}
impl Backend {
    pub fn new(
        config: BackendConfig,
        token: BackendToken,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
        poll_registry: &Rc<RefCell<Poll>>,
        next_cluster_token_value: &mut usize,
        timeout: usize,
        failure_limit: usize,
        retry_timeout: usize,
        pool_token: PoolTokenValue,
        num_backends: usize,
        cached_backend_shards: &Rc<RefCell<Option<Vec<usize>>>>,
    ) -> (Backend, Vec<Token>) {
        let weight = config.weight;
        let (backend, all_backend_tokens) = match config.use_cluster {
            false => {
                // The config should be validated to have a host when not using cluster. See load_config.
                let host = config.host.unwrap().clone();
                let (backend, tokens) = SingleBackend::new(
                    config,
                    host,
                    token,
                    poll_registry,
                    timeout,
                    failure_limit,
                    retry_timeout,
                    pool_token,
                    num_backends,
                    cached_backend_shards,
                );
                (BackendEnum::Single(backend), tokens)
            }
            true => {
                let (backend, tokens) = ClusterBackend::new(
                    config,
                    token,
                    cluster_backends,
                    poll_registry,
                    next_cluster_token_value,
                    timeout,
                    failure_limit,
                    retry_timeout,
                    pool_token,
                    num_backends,
                    cached_backend_shards,
                );
                (BackendEnum::Cluster(backend), tokens)
            }
        };
        (Backend {
            single: backend,
            weight: weight,
        }, all_backend_tokens)
    }

    pub fn reregister_token(&mut self, new_token: BackendToken, cluster_backends: &mut Vec<(SingleBackend, usize)>, new_num_backends: usize) -> Result<(), std::io::Error> {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.reregister_token(new_token, new_num_backends),
            BackendEnum::Cluster(ref mut backend) => backend.reregister_token(new_token, cluster_backends, new_num_backends),
        }
    }

    pub fn change_pool_token(&mut self, new_token_value: PoolTokenValue) {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.change_pool_token(new_token_value),
            BackendEnum::Cluster(ref mut backend) => backend.change_pool_token(new_token_value),
        }
    }

    pub fn is_available(&self) -> bool {
        match self.single {
            BackendEnum::Single(ref backend) => backend.is_available(),
            BackendEnum::Cluster(ref backend) => backend.is_available(),
        }
    }

    pub fn init_connection(&mut self, cluster_backends: &mut Vec<(SingleBackend, usize)>) {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.init_connection(),
            BackendEnum::Cluster(ref mut backend) => backend.init_connection(cluster_backends),
        }
    }

    pub fn handle_timeout(
        &mut self,
        token: Token,
        clients: &mut HashMap<usize, (BufferedClient, usize)>,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
        completed_clients: &mut VecDeque<ClientTokenValue>,
        stats: &mut Stats,
    ) -> bool {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.handle_timeout(clients, completed_clients, stats),
            BackendEnum::Cluster(ref mut backend) => {
                backend.handle_timeout(
                    token,
                    clients,
                    cluster_backends,
                    completed_clients,
                    stats
                )
            }
        }
    }

    /*
        Attempts to write a message to the backend.
        @param message: Message to be written.
        @param client_token: Token of the client that originated the message. NULL_TOKEN means that the message came
                             from the proxy.
        @param cluster_backends: Access to all cluster backends, in case this backend is a ClusterBackend.
        @param request_id: Unique identifier of the request, determined by time and id. Id will always be 0 for normal
                           requests. Multikey requests are split into many requests, with each one having an id of > 0.
    */
    pub fn write_message(
        &mut self,
        message: &[u8],
        client_token: ClientToken,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
        request_id: (Instant, usize),
        stats: &mut Stats,
    ) -> Result<(), WriteError> {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.write_message(message, client_token, request_id, stats),
            BackendEnum::Cluster(ref mut backend) => {
                backend.write_message(
                    message,
                    client_token,
                    cluster_backends,
                    request_id,
                    stats,
                )
            }
        }
    }

    pub fn handle_backend_response(
        &mut self,
        token: BackendToken,
        clients: &mut HashMap<usize, (BufferedClient, usize)>,
        next_cluster_token_value: &mut usize,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
        completed_clients: &mut VecDeque<ClientTokenValue>,
        stats: &mut Stats,
    ) {
        match self.single {
            BackendEnum::Single(ref mut backend) => {
                let mut resp_handler = |_response: &[u8]| -> () {};
                backend.handle_backend_response(clients, &mut resp_handler, completed_clients, stats);
            }
            BackendEnum::Cluster(ref mut backend) => backend.handle_backend_response(token, clients, next_cluster_token_value, cluster_backends, completed_clients, stats),
        };
    }

    pub fn handle_backend_failure(
        &mut self,
        token: Token,
        clients: &mut HashMap<usize, (BufferedClient, usize)>,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
        completed_clients: &mut VecDeque<ClientTokenValue>,
        stats: &mut Stats,
    ) {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.handle_backend_failure(clients, completed_clients, stats),
            BackendEnum::Cluster(ref mut backend) => backend.handle_backend_failure(token, clients, cluster_backends, completed_clients, stats),
        }
    }
}

pub struct SingleBackend {
    token: BackendToken,
    status: BackendStatus,
    pub weight: usize,
    host: SocketAddr,
    pub queue: VecDeque<(ClientToken, Instant, usize)>,
    failure_limit: usize,
    retry_timeout: usize,
    failure_count: usize,
    config: BackendConfig,
    pool_token: usize,
    poll_registry: Rc<RefCell<Poll>>,
    socket: Option<BufReader<TcpStream>>,
    timer: Option<Timer<Instant>>,
    retry_timer: Option<Timer<Instant>>,
    pub timeout: usize,
    waiting_for_auth_resp: bool,
    waiting_for_db_resp: bool,
    waiting_for_ping_resp: bool,
    pub num_backends: usize,
    cached_backend_shards: Rc<RefCell<Option<Vec<usize>>>>,
}
impl SingleBackend {
    pub fn new(
        config: BackendConfig,
        host: SocketAddr,
        token: BackendToken,
        poll_registry: &Rc<RefCell<Poll>>,
        timeout: usize,
        failure_limit: usize,
        retry_timeout: usize,
        pool_token: usize,
        num_backends: usize,
        cached_backend_shards: &Rc<RefCell<Option<Vec<usize>>>>,
    ) -> (SingleBackend, Vec<Token>) {
        debug!("Initialized Backend: token: {:?}", token);
        // TODO: Configure message queue size per backend.
        let backend = SingleBackend {
            host : host,
            token : token,
            queue: VecDeque::with_capacity(4096),
            status: BackendStatus::DISCONNECTED,
            timeout: timeout,
            poll_registry: Rc::clone(poll_registry),
            failure_limit: failure_limit,
            retry_timeout: retry_timeout,
            failure_count: 0,
            weight: config.weight,
            config: config,
            pool_token: pool_token,
            socket: None,
            timer: None,
            retry_timer: None,
            waiting_for_auth_resp: false,
            waiting_for_db_resp: false,
            waiting_for_ping_resp: false,
            num_backends: num_backends,
            cached_backend_shards: Rc::clone(cached_backend_shards),
        };
        (backend, Vec::new())
    }

    pub fn reregister_token(&mut self, new_token: BackendToken, new_num_backends: usize) -> Result<(), std::io::Error> {
        self.num_backends = new_num_backends;
        match self.socket {
            Some(ref s) => {
                self.token = new_token;
                try!(self.poll_registry.borrow_mut().reregister(s.get_ref(), new_token, Ready::readable() | Ready::writable(), PollOpt::edge()));
            }
            None => {}
        }
        match self.retry_timer {
            Some(ref t) => {
                try!(self.poll_registry.borrow_mut().reregister(t, Token(new_token.0 + self.num_backends), Ready::readable(), PollOpt::edge()));
            }
            None => {}
        }
        match self.timer {
            Some(ref t) => {
                try!(self.poll_registry.borrow_mut().reregister(t, Token(new_token.0 + 2* self.num_backends), Ready::readable(), PollOpt::edge()));
            }
            None => {}
        }
        return Ok(());
    }

    pub fn change_pool_token(&mut self, new_token_value: PoolTokenValue) {
        self.pool_token = new_token_value;
    }

    pub fn is_available(&self) -> bool {
        return self.status == BackendStatus::READY;
    }

    pub fn init_connection(&mut self) {
        match self.connect() {
            Ok(a) => a,
            Err(err) => {
                debug!("Failed to establish connection due to {:?}", err);
                change_state(&mut self.status, BackendStatus::DISCONNECTED);
                *self.cached_backend_shards.borrow_mut() = None;
                self.set_retry_timer();
            }
        }
    }

    pub fn connect(&mut self) -> Result<(), std::io::Error> {
        if self.status == BackendStatus::READY || self.status == BackendStatus::CONNECTED {
            debug!("Trying to connect when already connected!");
            return Ok(());
        }

        // Setup the server socket
        let socket = try!(TcpStream::connect(&self.host));
        debug!("New socket to {}: {:?}", self.host, socket);

        try!(self.poll_registry.borrow_mut().register(&socket, self.token, Ready::readable() | Ready::writable(), PollOpt::edge()));
        debug!("Registered backend: {:?}", &self.token);
        self.socket = Some(BufReader::new(socket));

        change_state(&mut self.status, BackendStatus::CONNECTING);
        return Ok(());
    }

    // Callback after initializing a connection.
    fn handle_connection(&mut self, stats: &mut Stats,) {
        let mut wait_for_resp = false;

        // TODO: Cache the string pushing to config initialization.
        if self.config.auth != String::new() {
            let mut request = String::with_capacity(14 + self.config.auth.len());
            request.push_str("*2\r\n$4\r\nAUTH\r\n$");
            request.push_str(&self.config.auth.len().to_string());
            request.push_str("\r\n");
            request.push_str(&self.config.auth);
            request.push_str("\r\n");
            if self.write_to_backend_stream(NULL_TOKEN, &request.as_bytes(), (Instant::now(), 0), stats).is_err() {
                change_state(&mut self.status, BackendStatus::DISCONNECTED);
                self.socket = None;
                return;
            }
            self.waiting_for_auth_resp = true;
            wait_for_resp = true;
        }

        if self.config.db != 0 {
            let mut request = String::with_capacity(14 + self.config.auth.len());
            request.push_str("*2\r\n$6\r\nSELECT\r\n$");
            request.push_str(&self.config.db.to_string().len().to_string());
            request.push_str("\r\n");
            request.push_str(&self.config.db.to_string());
            request.push_str("\r\n");
            if self.write_to_backend_stream(NULL_TOKEN, &request.as_bytes(), (Instant::now(), 0), stats).is_err() {
                change_state(&mut self.status, BackendStatus::DISCONNECTED);
                self.socket = None;
                return;
            }
            self.waiting_for_db_resp = true;
            wait_for_resp = true;
        }

        if self.timeout != 0 {
            if self.write_to_backend_stream(NULL_TOKEN, "PING\r\n".as_bytes(), (Instant::now(), 0), stats).is_err() {
                change_state(&mut self.status, BackendStatus::DISCONNECTED);
                self.socket = None;
                return;
            }
            self.waiting_for_ping_resp = true;
            wait_for_resp = true;
        }

        if !wait_for_resp {
            change_state(&mut self.status, BackendStatus::READY);
            *self.cached_backend_shards.borrow_mut() = None;
        }
    }

    // Handles a potential timeout.
    // Returns a boolean, signifying whether to mark this backend as down or not.
    pub fn handle_timeout(
        &mut self,
        clients: &mut HashMap<usize, (BufferedClient, usize)>,
        completed_clients: &mut VecDeque<ClientTokenValue>,
        stats: &mut Stats,
    ) -> bool {
        debug!("Handling ReqestTimeout for Backend {:?}", self.token);

        if self.status == BackendStatus::DISCONNECTED {
            self.timer = None;
            return false;
        }
        if self.queue.len() == 0 {
            return false;
        }
        loop {
            let timer_poll = match self.timer {
                Some(ref mut t) => t.poll(),
                None => {
                    panic!("A timeout event occurred without a backend timer being available.");
                }
            };
            let target_timestamp = match timer_poll {
                None => {
                    // TODO: For some reason, poll says timer is activated,but timer itself doesn't think so.
                    // This is a false poll trigger.
                    return false;
                }
                Some(ts) => ts,
            };
            let head = {
                match self.queue.get(0) {
                    Some(h) => h.clone(),
                    None => { return false; }
                }
            };
            let ref time = head.1;
            if &target_timestamp < time {
                // Should we remove the timer here?
                // In what cases do we have a timer firing with the wrong timestamp? Could be resolved already.
                // Are there cases we want to fire it again?
                // TODO: We need to rethink the whole timer system. Currently, there can only be one requesttimer per backend.
                // But we can have requests slow down, and a certain threshold will be hit. IE. 48 ms, 49 ms, 50 ms THRESHOLD, 51 ms.
                // The last 2 requests should timeout.
                // This means that we need to save all the timestamps of each request?
                // Or can we only record first and last?
                // It seems like the mio timer might be able to take multiple timeouts? This would be nifty then.
                debug!("This is wrong timestamp? {:?} vs {:?}", target_timestamp, time);
                //self.timer = None;
                continue;
            }

            // Get rid of first queue.
            self.queue.pop_front();

            debug!("queue size is now: {:?}", self.queue.len());

            if head.0 == NULL_TOKEN && (self.waiting_for_db_resp || self.waiting_for_auth_resp || self.waiting_for_ping_resp) {
                change_state(&mut self.status, BackendStatus::DISCONNECTED);
                *self.cached_backend_shards.borrow_mut() = None;
                self.init_connection();
            }

            if head.0 != NULL_TOKEN {
                debug!("Trying to find client: {:?}", (head.0));
                handle_write_to_client(
                    clients,
                    &(head.0).0,
                    b"-ERR Proxy timed out\r\n",
                    (head.1, head.2),
                    completed_clients,
                    stats,
                );
            }

            if &target_timestamp == time {
                if  self.status != BackendStatus::READY {
                    // Mark it down because it never initialized properly.
                    return true;
                }
                if self.failure_limit > 0 {
                    self.failure_count += 1;
                    if self.failure_count >= self.failure_limit {
                        debug!("Marking backend as failed");
                        return true;
                    }
                }

                continue;
            }
            else {
                panic!("This shouldn't happen. Timestamp hit: {:?}. Missed previous timestamp: {:?}", target_timestamp, time);
            }
        }
    }

    pub fn disconnect(&mut self) {
        change_state(&mut self.status, BackendStatus::DISCONNECTED);
        *self.cached_backend_shards.borrow_mut() = None;
        self.failure_count = 0;
        self.socket = None;
    }

    // Marks the backend as down. Returns an error message to all pending requests.
    // TODO: Is it still needed to have a mark_backend_down AND handle_backend_failure?
    pub fn mark_backend_down(
        &mut self,
        clients: &mut HashMap<ClientTokenValue, (BufferedClient, PoolTokenValue)>,
        completed_clients: &mut VecDeque<ClientTokenValue>,
        stats: &mut Stats,
    ) {
        self.disconnect();

        // TODO: It's possible that a client sends a request to 1 backend, then another. And the 2nd backend dies before the 1st one finishes.
        // In that case, the client should get response for the first request first, and then the error message for the second.
        // Actually, there is a race condition in general, even without any error, if the 2nd backend responds much quicker.
        // How is this avoided? By only doing one request from the client at a time.
        let mut possible_token = self.queue.pop_front();
        loop {
            match possible_token {
                Some((NULL_TOKEN, _, _)) => {}
                Some((client_token, instant, id)) => {
                    handle_write_to_client(
                        clients,
                        &client_token.0,
                        b"-ERR: Unavailable backend.\r\n",
                        (instant, id),
                        completed_clients,
                        stats,
                    );
                }
                None => break,
            }
            possible_token = self.queue.pop_front();
        }
    }

    pub fn write_message(
        &mut self,
        message: &[u8],
        client_token: Token,
        request_id: (Instant, usize),
        stats: &mut Stats,
    ) -> Result<(), WriteError> {
        // TODO: get rid of this wrapper function.
        match self.status {
            BackendStatus::READY => {
                return self.write_to_backend_stream(client_token, message, request_id, stats);
            }
            _ => {
                debug!("No backend connection.");
                return Err(WriteError::BackendNotReady);
            }
        }
    }

    pub fn handle_backend_response(
        &mut self,
        clients: &mut HashMap<usize, (BufferedClient, usize)>,
        internal_resp_handler: &mut FnMut(&[u8]),
        completed_clients: &mut VecDeque<ClientTokenValue>,
        stats: &mut Stats,
    ) {
        let prev_state = self.status;
        change_state(&mut self.status, BackendStatus::CONNECTED);
        if prev_state == BackendStatus::CONNECTING && self.status == BackendStatus::CONNECTED {
            self.handle_connection(stats);
        }

        // This can be considered DISCONNECTED already. If that's the case, disconnect should flush all responses in the queue.
        // This does happen because when disconnecting, the socket is set to None.

        // Read all responses if there are any left.
        while self.queue.len() > 0 {
            let res = route_backend_response(
                &mut self.socket,
                clients,
                &mut self.queue,
                &mut self.status,
                &mut self.waiting_for_auth_resp,
                &mut self.waiting_for_db_resp,
                &mut self.waiting_for_ping_resp,
                internal_resp_handler,
                &self.cached_backend_shards,
                completed_clients,
                stats,
            );
            match res {
                Ok(true) => continue,
                Ok(false) => { return; }
                Err(err) => {
                    error!("Received incompatible response from backend. Forcing a disconnect. Received error while parsing: {}", err);
                    self.mark_backend_down(clients, completed_clients, stats);
                }
            }
        }
        return;
    }

    pub fn handle_backend_failure(
        &mut self,
        clients: &mut HashMap<usize, (BufferedClient, usize)>,
        completed_clients: &mut VecDeque<ClientTokenValue>,
        stats: &mut Stats,
    ) {
        self.mark_backend_down(clients, completed_clients, stats);
        self.set_retry_timer();
    }

    fn set_retry_timer(&mut self) {
        if self.retry_timer.is_none() {
        debug!("Creating timer");
            let timer = create_timer();
            let timer_token = Token(self.token.0 + self.num_backends);
            match self.poll_registry.borrow_mut().register(&timer, timer_token, Ready::readable(), PollOpt::edge()) {
                Ok(_) => {}
                Err(err) => {
                    // Expected to occur only when timer is registered to a different poll (which shouldn't happen).
                    panic!("Failed to register retry timer to poll. Received error: {}", err);
                }
            };
            self.retry_timer = Some(timer);
        }

        let now = Instant::now();  
        let timestamp = now + Duration::from_millis(self.retry_timeout as u64);
        match self.retry_timer {
            Some(ref mut timer) => {
                match timer.set_timeout(Duration::new(0, (1000000 * self.retry_timeout) as u32), timestamp) {
                    Ok(_) => { }
                    Err(err) => {
                        // Expected to occur only in cases of usize integer overflow.
                        panic!("Failure setting timer timeout: {}.", err);
                    }
                }
            }
            None => {
                // Never expected to occur.
                panic!("Timer does not exist after being instantiated.");
            }
        }
    }

    fn write_to_backend_stream(
        &mut self,
        client_token: ClientToken,
        message: &[u8],
        request_id: (Instant, usize),
        stats: &mut Stats,
    ) -> Result<(), WriteError> {
        debug!("Write to backend {:?} {}: {:?} {:?}", &self.token, self.host, std::str::from_utf8(&message), client_token);
        let bytes_written = match self.socket {
            Some(ref mut s) => try!(write_to_stream(s.get_mut(), message)),
            None => return Err(WriteError::NoSocket),
        };
        stats.send_backend_bytes += bytes_written;
        // TODO: Keep trying on self.socket if it's INTERRUPTED or WOULDBLOCK, otherwise DISCONNECT the backend connection.
        let timestamp = request_id.0 + Duration::from_millis(self.timeout as u64);
        self.queue.push_back((client_token, timestamp, request_id.1));
        // Need to guarantee that queue is ordered. Is there any possibility
        if self.queue.len() == 1 && self.timeout != 0 {
            if self.timer.is_none() {
                let timer = create_timer();
                let timer_token = Token(self.token.0 + 2 * self.num_backends);
                debug!("Registered timer: {:?}", timer_token);
                match self.poll_registry.borrow_mut().register(&timer, timer_token, Ready::readable(), PollOpt::edge()) {
                    Ok(_) => {}
                    Err(err) => {
                        // Expected to occur only when timer is registered to a different poll (which shouldn't happen).
                        panic!("Failed to register timer to poll. Received error: {}", err);
                    }
                };
                self.timer = Some(timer);
            }

            match self.timer {
                Some(ref mut timer) => {
                    match timer.set_timeout(Duration::from_millis(self.timeout as u64), timestamp) {
                        Ok(_) => {}
                        Err(err) => {
                            // Expected to occur only in cases of usize integer overflow.
                            panic!("Failure setting timer timeout: {}.", err);
                        }
                    };
                    debug!("Setting timeout: {:?}", timestamp);
                }
                None => {
                    // Never expected to occur.
                    panic!("Timer does not exist after being instantiated.");
                }
            }
        }
        return Ok(());
    }
}

fn handle_internal_response(
    status: &mut BackendStatus,
    waiting_for_auth_resp: &mut bool,
    waiting_for_db_resp: &mut bool,
    waiting_for_ping_resp: &mut bool,
    response: &[u8],
    internal_resp_handler: &mut FnMut(&[u8]),
    cached_backend_shards: &Rc<RefCell<Option<Vec<usize>>>>,
) {
    // TODO: Handle the various requirements.
    if *waiting_for_auth_resp && response == b"+OK\r\n" {
        *waiting_for_auth_resp = false;
    }
    else if *waiting_for_db_resp && response == b"+OK\r\n" {
        *waiting_for_db_resp = false;
    }
    else if *waiting_for_ping_resp && response == b"+PONG\r\n" {
        *waiting_for_ping_resp = false;
    }
    else {
        internal_resp_handler(response);
        return;
    }
    if !*waiting_for_auth_resp && !*waiting_for_db_resp && !*waiting_for_ping_resp {
        change_state(status, BackendStatus::READY);
        *cached_backend_shards.borrow_mut() = None;
    }
}

fn change_state(status: &mut BackendStatus, target_state: BackendStatus) -> bool {
    // TODO: Rethink change state flow.
    if *status == target_state {
        return false;
    }
    let _prev_status = *status;
    match (*status, target_state) {
        // called when trying to establish a connection to backend.
        (BackendStatus::DISCONNECTED, BackendStatus::CONNECTING) => {}
         // happens when connection to backend has been established and is writable.
        (BackendStatus::CONNECTING, BackendStatus::CONNECTED) => {}
        // Happens when writable connection is validated with a PING (if timeout is enabled)
        (BackendStatus::CONNECTED, BackendStatus::READY) => {}
        (BackendStatus::READY, BackendStatus::CONNECTED) => { return true; }
        // Happens when the establishing connection to backend has timed out.
        (BackendStatus::CONNECTING, BackendStatus::DISCONNECTED) => {}
        // happens when host fails initializing PING
        (BackendStatus::CONNECTED, BackendStatus::DISCONNECTED) => {}
        // happens when host has been blacked out from too many failures/timeouts.
        (BackendStatus::READY, BackendStatus::DISCONNECTED) => {}
        _ => {
            debug!("Backend failed to change state from {:?} to {:?}", status, target_state);
            panic!("Failure to change states"); //return false;
        }
    }
    debug!("Backend changed state from {:?} to {:?}", status, target_state);
    *status = target_state;
    return true;
}

/*
    This should only be called if there is a request in the queue.
    Will panic if the queue is empty.
    Returns whether there may be more responses or not.
*/
fn route_backend_response(
    stream: &mut Option<BufReader<TcpStream>>,
    clients: &mut HashMap<usize, (BufferedClient, usize)>,
    queue: &mut VecDeque<(Token, Instant, usize)>,
    status: &mut BackendStatus,
    waiting_for_auth_resp: &mut bool,
    waiting_for_db_resp: &mut bool,
    waiting_for_ping_resp: &mut bool,
    internal_resp_handler: &mut FnMut(&[u8]),
    cached_backend_shards: &Rc<RefCell<Option<Vec<usize>>>>,
    completed_clients: &mut VecDeque<ClientTokenValue>,
    stats: &mut Stats,
) -> Result<bool, RedisError> {
    match stream {
        Some(ref mut s) => {
            let len = {
                let mut read_attempts = 3;
                loop {
                    let buf = if read_attempts == 3 {
                        match s.fill_buf() {
                            Ok(b) => b,
                            Err(_err) => {
                                return Ok(false);
                            }
                        }
                    } else {
                        s.reset_buf();
                        match s.append_buf() {
                            Ok(b) => b,
                            Err(_err) => { return Ok(false); }
                        }
                    };

                    debug!("Read from backend: {:?}", std::str::from_utf8(buf));
                    //let buf = s.append_buf().unwrap();
                    //error!("Read from backend again: {:?}", std::str::from_utf8(buf));

                    // If receiving a bad protocol backend, then this is an incompatible backend.
                    // Should disconnect backend, and give error message.
                    let response = match extract_redis_command(buf) {
                        Ok(r) => r,
                        Err(RedisError::IncompleteMessage) => {
                            error!("Incomplete message. trying again.");
                            read_attempts -= 1;
                            if read_attempts == 0 {
                                buf
                            } else {
                                continue;
                            }
                        }
                        Err(RedisError::Unknown(a)) => {
                            error!("Received unknown: {}, {:?}", RedisError::Unknown(a), std::str::from_utf8(buf));
                            read_attempts -= 1;
                            if read_attempts == 0 {
                                buf
                            } else {
                                continue;
                            }
                        }
                        Err(err) => { return Err(err); }
                    };
                    if response.len() == 0 {
                        return Ok(false);
                    }

                    let (client_token, request_id) = match queue.pop_front() {
                        Some((client_token, instant, id)) => (client_token, (instant, id)),
                        None => panic!("No more client token in backend queue, even though queue length was >0 just now!"),
                    };

                    if client_token == NULL_TOKEN {
                        handle_internal_response(
                            status,
                            waiting_for_auth_resp,
                            waiting_for_db_resp,
                            waiting_for_ping_resp,
                            response,
                            internal_resp_handler,
                            cached_backend_shards,
                        );
                    } else {
                        handle_write_to_client(clients, &client_token.0, response, request_id, completed_clients, stats);
                    }
                    break response.len()
                }
            };
            s.consume(len);
            stats.recv_backend_bytes += len;

            return Ok(true);
        }
        // This case occurs if the backend is disconnected. If that's the case, then it should send error messges to clients.
        None => {
            let (client_token, request_id) = match queue.pop_front() {
                Some((client_token, instant, id)) => (client_token, (instant, id)),
                None => panic!("No more client token in backend queue, even though queue length was >0 just now!"),
            };
            if client_token != NULL_TOKEN {
                handle_write_to_client(clients,&client_token.0, b"ERR Backend disconnected", request_id, completed_clients, stats);
            }
            return Ok(false);
        }
    }
}

// This extracts the command from the stream.
// TODO: Use a StreamingIterator: https://github.com/rust-lang/rfcs/pull/1598
pub fn parse_redis_command<R: Read>(stream: &mut BufReader<R>) -> String {
    let mut command = String::new();
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
                Ok(result) => {
                    command.push_str(&result);
                    string.push_str(&result);
                }
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
                let next_line = parse_redis_command(stream);
                command.push_str(&next_line);
                string.push_str(&next_line);
            }
        }
        _ => {}
    }
    command
}

// TODO: Should we want more clarity?
fn create_timer() -> Timer<Instant> {
    let mut builder = Builder::default();
    builder = builder.tick_duration(Duration::from_millis(10));
    builder.build()
}

pub fn write_to_stream(stream: &mut TcpStream, mut message: &[u8]) -> Result<(usize), WriteError> {
    loop {
        match stream.write(&message) {
            Ok(bytes_written) => {
                if bytes_written == message.len() {
                    return Ok(bytes_written);
                }
                message = match message.get(bytes_written..) {
                    Some(m) => m,
                    None => {
                        error!("!!!: Somehow more bytes were written than there are in the buffer. This should never happen. Please contact author.");
                        return Err(WriteError::BufOutOfBounds);
                    }
                };
                continue;
            }
            Err(err) => {
                match err.kind() {
                    std::io::ErrorKind::Interrupted => {
                        continue;
                    }
                    std::io::ErrorKind::WouldBlock => {
                        continue;
                    }
                    _ => {
                        let maybe_addr = match stream.peer_addr() {
                            Ok(addr) => Some(addr),
                            Err(_) => None,
                        };
                        return Err(WriteError::WriteFailure(maybe_addr, err));
                    }
                }
            }
        }
    }
}

pub fn handle_write_to_client(
    clients: &mut HashMap<ClientTokenValue, (BufferedClient, PoolTokenValue)>,
    client_token_value: &ClientTokenValue,
    message: &[u8],
    request_id: (Instant, usize),
    completed_clients: &mut VecDeque<ClientTokenValue>,
    stats: &mut Stats,
) {
    let res = match clients.get_mut(client_token_value) {
        Some((client, _)) => write_to_client(client.get_mut(), client_token_value, message, request_id, completed_clients, stats),
        None => { return; }
    };
    match res {
        Ok(bytes_written) => {
            stats.send_client_bytes += bytes_written;
        }
        Err(err) => {
            debug!("Removing client: Received error: {}", err);
            clients.remove(client_token_value);
        }
    }
}


pub fn write_to_client(
    client: &mut Client,
    client_token_value: &ClientTokenValue,
    message: &[u8],
    request_id: (Instant, usize),
    completed_clients: &mut VecDeque<ClientTokenValue>,
    stats: &mut Stats,
) -> std::result::Result<usize, WriteError> {
    if request_id.1 == 0 {
        // Id of 0 means that request is a normal request.
        stats.responses += 1;
        write_to_stream(&mut client.stream, message)
    } else {
        // Id > 0 means that the request is a multikey request.
        client.pending_response[request_id.1 - 1] = message.to_vec();
        client.pending_count -= 1;
        if client.pending_count == 0 {
            // Assemble the full response.
            let mut full_message = Vec::new();
            full_message.extend_from_slice(b"*");
            full_message.extend_from_slice(client.pending_response.len().to_string().as_bytes());
            full_message.extend_from_slice(b"\r\n");
            for i in client.pending_response.iter() {
                full_message.extend_from_slice(&i);
            }

            // Add client to completed_clients, to force an event to trigger for the client. It will normally not
            // fire because the poll is edge-triggered, not level-triggered.
            completed_clients.push_back(*client_token_value);
            stats.responses += 1;
            write_to_stream(&mut client.stream, &full_message)
        } else {
            Ok(0)
        }
    }
}
