use redflareproxy::ClientToken;
use redflareproxy::BackendToken;
use redflareproxy::convert_token_to_client_index;
use redflareproxy::Client;
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
use redisprotocol::extract_redis_command2;
use redisprotocol::RedisError;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BackendStatus {
    READY,
    CONNECTED,
    DISCONNECTED,
    CONNECTING,
    LOADING,
}
pub type Host = String;

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
        token: Token,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
        poll_registry: &Rc<RefCell<Poll>>,
        next_cluster_token_value: &mut usize,
        timeout: usize,
        failure_limit: usize,
        retry_timeout: usize,
        pool_token: usize,
    ) -> (Backend, Vec<Token>) {
        let weight = config.weight;
        let (backend, all_backend_tokens) = match config.use_cluster {
            false => {
                let host = config.host.clone().unwrap().clone();
                let (backend, tokens) = SingleBackend::new(
                    config,
                    host,
                    token,
                    poll_registry,
                    timeout,
                    failure_limit,
                    retry_timeout,
                    pool_token,
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
                );
                (BackendEnum::Cluster(backend), tokens)
            }
        };
        (Backend {
            single: backend,
            weight: weight,
        }, all_backend_tokens)
    }

    pub fn reregister_token(&mut self, new_token: BackendToken, num_backends: usize) -> Result<(), std::io::Error> {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.reregister_token(new_token, num_backends),
            BackendEnum::Cluster(ref mut backend) => backend.reregister_token(new_token),
        }
    }

    pub fn change_pool_token(&mut self, new_token_value: usize) {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.change_pool_token(new_token_value),
            BackendEnum::Cluster(ref mut backend) => backend.change_pool_token(new_token_value),
        }
    }

    pub fn is_available(&mut self) -> bool {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.is_available(),
            BackendEnum::Cluster(ref mut backend) => backend.is_available(),
        }
    }

    pub fn connect(&mut self, cluster_backends: &mut Vec<(SingleBackend, usize)>, num_pools: usize) {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.connect(num_pools),
            BackendEnum::Cluster(ref mut backend) => backend.connect(cluster_backends, num_pools),
        }
    }

    pub fn handle_timeout(&mut self, token: Token, clients: &mut Vec<(Client, usize)>, cluster_backends: &mut Vec<(SingleBackend, usize)>, num_pools: usize, num_backends: usize) -> bool {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.handle_timeout(clients, num_pools, num_backends),
            BackendEnum::Cluster(ref mut backend) => backend.handle_timeout(token, clients, cluster_backends, num_pools, num_backends),
        }
    }

    pub fn write_message(
        &mut self,
        message: &[u8],
        client_token: Token,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
        num_backends: usize,
    ) -> bool {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.write_message(message, client_token, num_backends),
            BackendEnum::Cluster(ref mut backend) => backend.write_message(message, client_token, cluster_backends, num_backends),
        }
    }

    pub fn handle_backend_response(
        &mut self,
        token: BackendToken,
        clients: &mut Vec<(Client, usize)>,
        next_cluster_token_value: &mut usize,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
        num_pools: usize,
        num_backends: usize
    ) {
        match self.single {
            BackendEnum::Single(ref mut backend) => {
                backend.handle_backend_response(clients, num_pools, num_backends);
            }
            BackendEnum::Cluster(ref mut backend) => backend.handle_backend_response(token, clients, next_cluster_token_value, cluster_backends, num_pools, num_backends),
        };
    }

    pub fn handle_backend_failure(
        &mut self,
        token: Token,
        clients: &mut Vec<(Client, usize)>,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
        num_pools: usize,
        num_backends: usize
    ) {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.handle_backend_failure(clients, num_pools, num_backends),
            BackendEnum::Cluster(ref mut backend) => backend.handle_backend_failure(token, clients, cluster_backends, num_pools, num_backends),
        }
    }
}

pub struct SingleBackend {
    token: BackendToken,
    status: BackendStatus,
    pub weight: usize,
    host: String,
    pub queue: VecDeque<(ClientToken, Instant)>,
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
}
impl SingleBackend {
    pub fn new(
        config: BackendConfig,
        host: String,
        token: Token,
        poll_registry: &Rc<RefCell<Poll>>,
        timeout: usize,
        failure_limit: usize,
        retry_timeout: usize,
        pool_token: usize,
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
        };
        (backend, Vec::new())
    }

    pub fn reregister_token(&mut self, new_token: BackendToken, num_backends: usize) -> Result<(), std::io::Error> {
        match self.socket {
            Some(ref s) => {
                self.token = new_token;
                let _ = self.poll_registry.borrow_mut().reregister(s.get_ref(), new_token, Ready::readable() | Ready::writable(), PollOpt::edge());
            }
            None => {}
        }
        match self.retry_timer {
            Some(ref t) => {
                let _ = self.poll_registry.borrow_mut().reregister(t, Token(new_token.0 + num_backends), Ready::readable(), PollOpt::edge());
            }
            None => {}
        }
        match self.timer {
            Some(ref t) => {
                let _ = self.poll_registry.borrow_mut().reregister(t, Token(new_token.0 + 2* num_backends), Ready::readable(), PollOpt::edge());
            }
            None => {}
        }
        // TODO: bubble up error.
        return Ok(());
    }

    pub fn change_pool_token(&mut self, new_token_value: usize) {
        self.pool_token = new_token_value;
    }

    pub fn is_available(&mut self) -> bool {
        return self.status == BackendStatus::READY;
    }

    pub fn connect(
        &mut self,
        num_backends: usize,
    ) {
        if self.status == BackendStatus::READY || self.status == BackendStatus::CONNECTED {
            debug!("Trying to connect when already connected!");
            return;
        }

        let addr = self.host.parse().unwrap();

        // Setup the server socket
        let socket = TcpStream::connect(&addr).unwrap();
        debug!("New socket to {}: {:?}", addr, socket);

        debug!("Registered backend: {:?}", &self.token);
        self.poll_registry.borrow_mut().register(&socket, self.token, Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();
        self.socket = Some(BufReader::new(socket));

        self.change_state(BackendStatus::CONNECTING, num_backends);
    }

    // Callback after initializing a connection.
    fn handle_connection(&mut self, num_backends: usize) {
        //self.timer = None;

        // TODO: Use a macro to encode the requests into redis protocol.

        let mut wait_for_resp = false;

        // TODO: Cache the string pushing to config initialization.
        if self.config.auth != String::new() {
            let mut request = String::with_capacity(14 + self.config.auth.len());
            request.push_str("*2\r\n$4\r\nAUTH\r\n$");
            request.push_str(&self.config.auth.len().to_string());
            request.push_str("\r\n");
            request.push_str(&self.config.auth);
            request.push_str("\r\n");
            self.write_to_stream(NULL_TOKEN, &request.as_bytes(), num_backends);
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
            self.write_to_stream(NULL_TOKEN, &request.as_bytes(), num_backends);
            self.waiting_for_db_resp = true;
            wait_for_resp = true;
        }

        if self.timeout != 0 {
            self.write_to_stream(NULL_TOKEN, "PING\r\n".as_bytes(), num_backends);
            self.waiting_for_ping_resp = true;
            wait_for_resp = true;
        }

        if !wait_for_resp {
            self.change_state(BackendStatus::READY, num_backends);
        }
    }

    // Handles a potential timeout.
    // Returns a boolean, signifying whether to mark this backend as down or not.
    pub fn handle_timeout(
        &mut self,
        clients: &mut Vec<(Client, usize)>,
        num_pools: usize,
        num_backends: usize,
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
            let timer_poll = self.timer.as_mut().unwrap().poll();
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
                self.change_state(BackendStatus::DISCONNECTED, num_backends);
                self.connect(num_backends);
            }

            if head.0 != NULL_TOKEN {
                debug!("Trying to find client: {:?}", (head.0));
                let client_index = convert_token_to_client_index(num_pools, num_backends, (head.0).0);
                let (client, _) = clients.get_mut(client_index).unwrap();
                write_to_client(client, b"-ERR Proxy timed out\r\n");
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

    // Marks the backend as down. Returns an error message to all pending requests.
    // TODO: Is it still needed to have a mark_backend_down AND handle_backend_failure?
    pub fn mark_backend_down(&mut self, clients: &mut Vec<(Client, usize)>, num_backends: usize) {
        if self.socket.is_some() {
            let err = self.socket.as_mut().unwrap().get_mut().take_error();
            debug!("Previous socket error: {:?}", err);
        }
        self.change_state(BackendStatus::DISCONNECTED, num_backends);

        self.failure_count = 0;

        let mut possible_token = self.queue.pop_front();
        loop {
            match possible_token {
                Some((NULL_TOKEN, _)) => {}
                Some((client_token, _)) => {
                    let (ref mut client, _) = clients.get_mut(client_token.0).unwrap();
                    write_to_client(client, b"-ERR: Unavailable backend.\r\n");
                }
                None => break,
            }
            possible_token = self.queue.pop_front();
        }

        self.socket = None;
    }

    pub fn write_message(
        &mut self,
        message: &[u8],
        client_token: Token,
        num_backends: usize,
    ) -> bool {
        match self.status {
            BackendStatus::READY => {
                self.write_to_stream(client_token, message, num_backends);
                true
            }
            _ => {
                debug!("No backend connection.");
                false
            }
        }
    }

    pub fn handle_backend_response(&mut self, clients: &mut Vec<(Client, usize)>, num_pools: usize, num_backends: usize) -> VecDeque<String> {
        self.change_state(BackendStatus::CONNECTED, num_backends);

        let mut unhandled_internal_responses = VecDeque::new();

        // Read all responses if there are any left.
        while self.queue.len() > 0 {
            let a = route_backend_response(
                &mut self.socket,
                clients,
                num_pools,
                num_backends,
                &mut self.queue,
                &mut self.token,
                &mut self.status,
                &mut self.waiting_for_auth_resp,
                &mut self.waiting_for_db_resp,
                &mut self.waiting_for_ping_resp,
                &mut unhandled_internal_responses,
            );
            if a.unwrap() == false {
                return unhandled_internal_responses;
            };
        }
        return unhandled_internal_responses;
    }

    pub fn handle_backend_failure(&mut self, clients: &mut Vec<(Client, usize)>, num_pools: usize, num_backends: usize) {
        self.mark_backend_down(clients, num_pools);
        self.retry_connect(num_backends);
    }

    fn retry_connect(&mut self, num_backends: usize) {
        debug!("Creating timer");
        // Create new timer.
        if self.retry_timer.is_none() {
            let timer = create_timer();
            let timer_token = Token(self.token.0 + num_backends);
            self.poll_registry.borrow_mut().register(&timer, timer_token, Ready::readable(), PollOpt::edge()).unwrap();
            self.retry_timer = Some(timer);
        }

        let now = Instant::now();  
        let timestamp = now + Duration::from_millis(self.retry_timeout as u64);
        match self.retry_timer {
            Some(ref mut timer) => {
                let _ = timer.set_timeout(Duration::new(0, (1000000 * self.retry_timeout) as u32), timestamp);
            }
            None => { panic!("impossible"); }
        }
        // need to handle with specific function for token. How to know what token this is?
        // can stuff into sockets. but it'll ahve timer token.

        //let parent_token = self.pool_token.clone();
        debug!("Original: {:?}", self.pool_token);
        //let timer_token = Token(self.token.0 + 1);
    }

    pub fn change_state(&mut self, target_state: BackendStatus, num_backends: usize) -> bool {
        // TODO: Rethink change state flow.
        if self.status == target_state {
            return true;
        }
        let prev_status = self.status;
        match (self.status, target_state) {
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
                debug!("Backend {:?} failed to change state from {:?} to {:?}", self.token, self.status, target_state);
                panic!("Failure to change states"); //return false;
            }
        }
        debug!("Backend {:?} changed state from {:?} to {:?}", self.token, self.status, target_state);
        self.status = target_state;
        match (prev_status, target_state) {
            (BackendStatus::CONNECTING, BackendStatus::CONNECTED) => {
                self.handle_connection(num_backends);
            }
            _ => {}
        }
        return true;
    }

    fn write_to_stream(
        &mut self,
        client_token: Token,
        message: &[u8],
        num_backends: usize,
    ) {
        debug!("Write to backend {:?} {}: {:?} {:?}", &self.token, self.host, std::str::from_utf8(&message), client_token);
        match self.socket {
            Some(ref mut socket) => {
                let _ = socket.get_mut().write(&message);
            }
            None => panic!("No connection to backend"),
        }
        let now = Instant::now();
        let timestamp = now + Duration::from_millis(self.timeout as u64);
        self.queue.push_back((client_token, timestamp));
        if self.queue.len() == 1 && self.timeout != 0 {
            if self.timer.is_none() {
                let timer = create_timer();
                let timer_token = Token(self.token.0 + 2*num_backends);
                debug!("Registered timer: {:?}", timer_token);
                self.poll_registry.borrow_mut().register(&timer, timer_token, Ready::readable(), PollOpt::edge()).unwrap();
                self.timer = Some(timer);
            }

            match self.timer {
                Some(ref mut timer) => {
                    let _ = timer.set_timeout(Duration::from_millis(self.timeout as u64), timestamp);
                    debug!("Setting timeout: {:?}", timestamp);
                }
                None => { panic!("impossible"); }
            }
        }
    }
}

fn write_to_client(client_stream: &mut Client, message: &[u8]) {
    let _ = client_stream.get_mut().write(message);
}

fn handle_internal_response(token: &Token, status: &mut BackendStatus, waiting_for_auth_resp: &mut bool, waiting_for_db_resp: &mut bool, waiting_for_ping_resp: &mut bool, response: &[u8], unhandled_queue: &mut VecDeque<String>) {
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
        unhandled_queue.push_back(std::str::from_utf8(response).unwrap().to_string());
        return;
    }
    if !*waiting_for_auth_resp && !*waiting_for_db_resp && !*waiting_for_ping_resp {
        change_state(token, status, BackendStatus::READY);
    }
}

fn change_state(token: &Token, status: &mut BackendStatus, target_state: BackendStatus) -> bool {
    // TODO: Rethink change state flow.
    if *status == target_state {
        return true;
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
            debug!("Backend {:?} failed to change state from {:?} to {:?}", token, status, target_state);
            panic!("Failure to change states"); //return false;
        }
    }
    debug!("Backend {:?} changed state from {:?} to {:?}", token, status, target_state);
    *status = target_state;
    return true;
}

fn route_backend_response(
    stream: &mut Option<BufReader<TcpStream>>,
    clients: &mut Vec<(Client, usize)>,
    num_pools: usize,
    num_backends: usize,
    queue: &mut VecDeque<(Token, Instant)>,
    token: &mut Token,
    status: &mut BackendStatus,
    waiting_for_auth_resp: &mut bool,
    waiting_for_db_resp: &mut bool,
    waiting_for_ping_resp: &mut bool,
    unhandled_internal_responses: &mut VecDeque<String>,
) -> Result<bool, RedisError> {
    match stream {
        Some(ref mut s) => {
            let len = {
                let buf = match s.fill_buf() {
                    Ok(b) => b,
                    Err(_err) => {
                        return Ok(false);
                    }
                };

                debug!("Read from backend: {:?}", std::str::from_utf8(buf));
                let response = try!(extract_redis_command2(buf));
                if response.len() == 0 {
                    return Ok(false);
                }

                let client_token = match queue.pop_front() {
                    Some((client_token, _)) => client_token,
                    None => panic!("No more client token in backend queue, even though queue length was >0 just now!"),
                };

                if client_token == NULL_TOKEN {
                    handle_internal_response(
                        token,
                        status,
                        waiting_for_auth_resp,
                        waiting_for_db_resp,
                        waiting_for_ping_resp,
                        response,
                        unhandled_internal_responses
                    );
                } else {
                    let client_index = convert_token_to_client_index(num_pools, num_backends, client_token.0);
                    debug!("Client index: {:?}", client_index);
                    match clients.get_mut(client_index) {
                        Some((stream, _)) => {
                            debug!("Wrote to client {:?}: {:?}", client_token, std::str::from_utf8(response));
                            let _ = stream.get_mut().write(response);
                        }
                        None => panic!("Found listener instead of stream! for clienttoken {:?}", client_token),
                    }
                }
                response.len()
            };
            s.consume(len);


            return Ok(true);
        }
        None => panic!("No backend stream!"),
    }
}

// This extracts the command from the stream.
// TODO: Use a StreamingIterator: https://github.com/rust-lang/rfcs/pull/1598
pub fn parse_redis_command(stream: &mut BufReader<TcpStream>) -> String {
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

