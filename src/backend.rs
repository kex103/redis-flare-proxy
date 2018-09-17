use rustproxy::{StreamType, NULL_TOKEN, Subscriber};
use config::BackendConfig;
use backendpool::{BackendPool, parse_redis_response};
use bufstream::BufStream;
use mio::*;
use mio_more::timer::Timer;
use mio::tcp::{TcpStream};
use std::collections::{VecDeque, HashMap};
use std::string::String;
use std::io::{Read, Write, BufRead};
use std::time::Duration;
use std::time::Instant;
use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use cluster_backend::{ClusterBackend};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BackendStatus {
    CONNECTED,
    DISCONNECTED,
    CONNECTING,
    LOADING,
}
pub type Host = String;

enum BackendEnum {
    Single(SingleBackend),
    Cluster(ClusterBackend),
}

pub struct Backend {
    pub weight: usize,
    single: BackendEnum,
}
impl Backend {
    pub fn new(
        config: BackendConfig,
        token: Token,
        backend_tokens_registry: &Rc<RefCell<HashMap<Token, Token>>>,
        subscribers_registry: &Rc<RefCell<HashMap<Token, Subscriber>>>,
        poll_registry: &Rc<RefCell<Poll>>,
        next_socket_index: &Rc<Cell<usize>>,
        timeout: usize,
        failure_limit: usize,
        retry_timeout: usize,
        pool: &mut BackendPool,
        written_sockets: &mut VecDeque<(Token, StreamType)>,
    ) -> (Backend, Vec<Token>) {
        let weight = config.weight;
        let (backend, all_backend_tokens) = match config.use_cluster {
            false => {
                let host = config.host.clone().unwrap().clone();
                let (backend, tokens) = SingleBackend::new(
                    config,
                    host,
                    token,
                    subscribers_registry,
                    poll_registry,
                    timeout,
                    failure_limit,
                    retry_timeout,
                    pool,
                    written_sockets
                );
                (BackendEnum::Single(backend), tokens)
            }
            true => {
                let (backend, tokens) = ClusterBackend::new(
                    config,
                    token,
                    backend_tokens_registry,
                    subscribers_registry,
                    poll_registry,
                    next_socket_index,
                    timeout,
                    failure_limit,
                    retry_timeout,
                    pool,
                    written_sockets
                );
                (BackendEnum::Cluster(backend), tokens)
            }
        };
        (Backend {
            single: backend,
            weight: weight,
        }, all_backend_tokens)
    }

    pub fn is_available(&mut self) -> bool {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.is_available(),
            BackendEnum::Cluster(ref mut backend) => backend.is_available(),
        }
    }

    pub fn connect(&mut self) {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.connect(),
            BackendEnum::Cluster(ref mut backend) => backend.connect(),
        }
    }

    pub fn next_timeout(&self) -> Option<Instant> {
        match self.single {
            BackendEnum::Single(ref backend) => backend.next_timeout(),
            BackendEnum::Cluster(ref backend) => backend.next_timeout(),
        }
    }

    pub fn handle_timeout(&mut self, token: Token, timestamp: Instant) -> bool {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.handle_timeout(timestamp),
            BackendEnum::Cluster(ref mut backend) => backend.handle_timeout(token, timestamp),
        }
    }

    
    pub fn mark_backend_down(
        &mut self,
    ) {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.mark_backend_down(),
            BackendEnum::Cluster(ref mut _backend) => panic!("unimplemented")
        }
    }

    pub fn write_message(&mut self,
        message: String,
        client_token: Token
    ) -> bool {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.write_message(message, client_token),
            BackendEnum::Cluster(ref mut backend) => backend.write_message(message, client_token),
        }
    }

    pub fn flush_stream(&mut self) {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.flush_stream(),
            BackendEnum::Cluster(ref mut backend) => backend.flush_stream(),
        }
    }

    pub fn handle_backend_response(&mut self, token: Token) {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.handle_backend_response(),
            BackendEnum::Cluster(ref mut backend) => backend.handle_backend_response(token),
        }
    }

    pub fn handle_backend_failure(&mut self, token: Token) {
        match self.single {
            BackendEnum::Single(ref mut backend) => backend.handle_backend_failure(),
            BackendEnum::Cluster(ref mut backend) => backend.handle_backend_failure(token),
        }
    }
}

pub struct SingleBackend {
    token: Token,
    status: BackendStatus,
    pub weight: usize,
    host: String,
    pub queue: VecDeque<(Token, Instant)>,
    failure_limit: usize,
    retry_timeout: usize,
    failure_count: usize,
    config: BackendConfig,
    parent: *mut BackendPool,
    subscribers_registry: Rc<RefCell<HashMap<Token, Subscriber>>>,
    poll_registry: Rc<RefCell<Poll>>,
    written_sockets: *mut VecDeque<(Token, StreamType)>,
    socket: Option<BufStream<TcpStream>>,
    timer: Option<Timer<()>>,
    pub timeout: usize,
}
impl SingleBackend {
    pub fn new(
        config: BackendConfig,
        host: String,
        token: Token,
        subscribers_registry: &Rc<RefCell<HashMap<Token, Subscriber>>>,
        poll_registry: &Rc<RefCell<Poll>>,
        timeout: usize,
        failure_limit: usize,
        retry_timeout: usize,
        pool: *mut BackendPool,
        written_sockets: *mut VecDeque<(Token, StreamType)>
    ) -> (SingleBackend, Vec<Token>) {
        debug!("Initialized Backend: token: {:?}", token);
        // TODO: Configure message queue size per backend.
        let backend = SingleBackend {
            host : host,
            token : token,
            queue: VecDeque::with_capacity(4096),
            status: BackendStatus::DISCONNECTED,
            timeout: timeout,
            subscribers_registry: Rc::clone(subscribers_registry),
            poll_registry: Rc::clone(poll_registry),
            failure_limit: failure_limit,
            retry_timeout: retry_timeout,
            failure_count: 0,
            weight: config.weight,
            config: config,
            parent: pool as *mut BackendPool,
            socket: None,
            timer: None,
            written_sockets: written_sockets as *mut VecDeque<(Token, StreamType)>,
        };
        (backend, Vec::new())
    }

    pub fn is_available(&mut self) -> bool {
        return self.status == BackendStatus::CONNECTED;
    }

    pub fn connect(
        &mut self,
    ) {
        if self.status == BackendStatus::CONNECTED {
            debug!("Trying to connect when already connected!");
            return;
        }

        let addr = self.host.parse().unwrap();

        // Setup the server socket
        let socket = TcpStream::connect(&addr).unwrap();
        debug!("New socket to {}: {:?}", addr, socket);

        self.change_state(BackendStatus::CONNECTING);

        debug!("Registered backend: {:?}", &self.token);
        self.poll_registry.borrow_mut().register(&socket, self.token, Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();
        self.socket = Some(BufStream::new(socket));
        self.subscribers_registry.borrow_mut().insert(self.token, Subscriber::PoolServer(self.parent_token()));
    }

    // Callback after initializing a connection.
    fn handle_connection(&mut self) {
        /*
            Are there any situations where self.timer is used for a request timeout, and not a retry timeout here?
        */
        self.timer = None;


        // if auth, connect with password?
        if self.config.auth != String::new() {
            self.write_to_stream(NULL_TOKEN, "AUTH pass".to_owned());
        }
        // If db, select db.
        if self.config.db != 0 {
            self.write_to_stream(NULL_TOKEN, "SELECT self.config.db".to_owned());
        }
    }

    // Retrieves the next timeout time in the message queue.
    pub fn next_timeout(&self) -> Option<Instant> {
        match self.queue.get(0) {
            Some(request) => Some(request.1),
            None => None,
        }
    }

    // Handles a potential timeout.
    // Returns a boolean, signifying whether to mark this backend as down or not.
    pub fn handle_timeout(
        &mut self,
        timestamp: Instant
    ) -> bool {
        debug!("Handling timeout: Timeout {:?}", self.token);
        if self.status == BackendStatus::DISCONNECTED {
            return false;
        }
        if self.queue.len() == 0 {
            return false;
        }
        let head = {
            let head = self.queue.get(0).unwrap();
            head.clone()
        };

        // Get rid of first queue.
        self.queue.pop_front();

        let ref time = head.1;
        if &timestamp < time {
            return false;
        }

        self.write_to_client(head.0, "-ERR RustProxy timed out\r\n".to_owned());

        if &timestamp == time {
            if self.failure_limit > 0 {
                self.failure_count += 1;
                if self.failure_count >= self.failure_limit {
                    return true;
                }
            }
        }
        else {
            panic!("This shouldn't happen. Timestamp hit: {:?}. Missed previous timestamp: {:?}", timestamp, time);
        }
        return false;
    }

    // Marks the backend as down. Returns an error message to all pending requests.
    // TODO: Is it still needed to have a mark_backend_down AND handle_backend_failure?
    pub fn mark_backend_down(&mut self) {
        if self.status == BackendStatus::CONNECTED {
            self.subscribers_registry.borrow_mut().remove(&Token(self.token.0 - 1));
        }
        if self.socket.is_some() {
            let err = self.socket.as_mut().unwrap().get_mut().take_error();
            debug!("Previous socket error: {:?}", err);
        }
        self.change_state(BackendStatus::DISCONNECTED);

        self.failure_count = 0;

        let mut possible_token = self.queue.pop_front();
        loop {
            match possible_token {
                Some((NULL_TOKEN, _)) => {}
                Some((client_token, _)) => {
                    self.write_to_client(client_token, "-ERR: Unavailable backend.\r\n".to_owned());
                }
                None => break,
            }
            possible_token = self.queue.pop_front();
        }
        
        self.subscribers_registry.borrow_mut().remove(&self.token);

        self.socket = None;
    }

    pub fn flush_stream(&mut self) {
        match self.socket {
            Some(ref mut socket) => {
                let _ = socket.flush();
            }
            None => {
                debug!("Backend {:?} stream not found when flushing. Did it just get DISCONNECTED?", self.token);
            }
        }
    }

    pub fn write_message(&mut self,
        message: String,
        client_token: Token
    ) -> bool {
        match self.status {
            BackendStatus::CONNECTED => {
                self.write_to_stream(client_token, message.clone());
                true
            }
            _ => {
                debug!("No backend connection.");
                false
            }
        }
    }

    pub fn handle_backend_response(&mut self) {
        self.change_state(BackendStatus::CONNECTED);

        // Read all responses if there are any left.
        while self.queue.len() > 0 {
            let response = self.get_backend_response();
            if response.len() == 0 {
                return;
            }
            let client_token = match self.queue.pop_front() {
                Some((client_token, _)) => client_token,
                None => panic!("No more client token in backend queue, even though queue length was >0 just now!"),
            };      

            self.write_to_client(client_token, response);
        }
    }

    pub fn handle_backend_failure(&mut self) {
        self.mark_backend_down();
        self.retry_connect();
    }

    fn retry_connect(&mut self) {
        debug!("Creating timer");
        // Create new timer.
        let mut timer = Timer::default();
        let _ = timer.set_timeout(Duration::new(0, (1000000 * self.retry_timeout) as u32), ());
        let timer_token = Token(self.token.0 + 1);
        self.poll_registry.borrow_mut().register(&timer, timer_token, Ready::readable(), PollOpt::level()).unwrap();
        // need to handle with specific function for token. How to know what token this is?
        // can stuff into sockets. but it'll ahve timer token.
        self.timer = Some(timer);
        let parent_token = self.parent_token().clone();
        debug!("Original: {:?}", self.parent_token());
        debug!("Parent token! {:?}", parent_token);
        self.subscribers_registry.borrow_mut().insert(timer_token, Subscriber::Timeout(parent_token));
    }

    pub fn change_state(&mut self, target_state: BackendStatus) -> bool {
        if self.status == target_state {
            return true;
        }
        match (self.status, target_state) {
            // called when trying to establish a connection to backend.
            (BackendStatus::DISCONNECTED, BackendStatus::CONNECTING) => {}
            (BackendStatus::CONNECTING, BackendStatus::CONNECTED) => {
                // call handle_connection.
                self.handle_connection();
            } // happens when connection to backend has been established and is writable.
            // Happens when the establishing connection to backend has timed out.
            (BackendStatus::CONNECTING, BackendStatus::DISCONNECTED) => {}
            // happens when host has been blacked out from too many failures/timeouts.
            (BackendStatus::CONNECTED, BackendStatus::DISCONNECTED) => {}
            _ => {
                debug!("Backend {:?} failed to change state from {:?} to {:?}", self.token, self.status, target_state);
                panic!("Failure to change states"); //return false;
            }
        }
        debug!("Backend {:?} changed state from {:?} to {:?}", self.token, self.status, target_state);
        self.status = target_state;
        return true;
    }

    fn parent_token(&self) -> Token {
        unsafe {
            let ref parent_pool = *self.parent;
            return parent_pool.token;
        }
    }

    fn parent_clients(&self) -> &mut HashMap<Token, BufStream<TcpStream>> {
        unsafe {
            let parent_pool = &mut *self.parent;
            return &mut parent_pool.client_sockets;
        }
    }

    fn register_written_socket(&self, token: Token, stream_type: StreamType) {
        let written_sockets = unsafe {
            &mut *self.written_sockets as &mut VecDeque<(Token, StreamType)>
        };
        written_sockets.push_back((token, stream_type));
    }

    fn write_to_client(&mut self, client_token: Token, message: String) {
        if client_token == NULL_TOKEN {
            return;
        }
        match self.parent_clients().get_mut(&client_token) {
            Some(stream) => {
                debug!("Wrote to client {:?}: {:?}", client_token, message);
                let _ = stream.write(&message.into_bytes()[..]);
                self.register_written_socket(client_token, StreamType::PoolClient);
            }
            _ => panic!("Found listener instead of stream! for clienttoken {:?}", client_token),
        }
    }

    fn write_to_stream(
        &mut self,
        client_token: Token,
        message: String,
    ) {
        debug!("Write to backend {:?} {}: {}", &self.token, self.host, &message);
        match self.socket {
            Some(ref mut socket) => {
                let _ = socket.write(&message.clone().into_bytes()[..]);
            }
            None => panic!("No connection to backend"),
        }
        self.register_written_socket(self.token.clone(), StreamType::PoolServer);
        let now = Instant::now();
        let timestamp = now + Duration::from_millis(self.timeout as u64);
        self.queue.push_back((client_token, timestamp));
        if self.queue.len() == 1 && self.timeout != 0 {
            let mut timer = Timer::default();
            let _ = timer.set_timeout(Duration::from_millis(self.timeout as u64), ());
            let timer_token = self.get_timeout_token();
            self.poll_registry.borrow_mut().register(&timer, timer_token, Ready::readable(), PollOpt::level()).unwrap();
            // need to handle with specific function for token. How to know what token this is?
            // can stuff into sockets. but it'll ahve timer token.
            self.timer = Some(timer);

            self.subscribers_registry.borrow_mut().insert(self.get_timeout_token(), Subscriber::RequestTimeout(self.parent_token(), timestamp));
        }
    }

    pub fn get_backend_response(&mut self) -> String {
        let response;
        match self.socket {
            Some(ref mut stream) => response = parse_redis_response(stream),
            _ => panic!("Found listener instead of stream!"),
        }
        debug!("Read from backend: {}", response);
        if response.len() == 0 {
            debug!("Completely empty string response from backend {:?}!", self.socket);
            // TODO: remote connection can disconnect, and rustproxy won't' detect
            // that it's down until a client attempts to hit it.
            // Should we listen for peer close to mark it early?
            return response;
        }
        return response
    }

    fn get_timeout_token(&self) -> Token {
        Token(self.token.0 + 1)
    }
}

pub fn parse_redis_command(stream: &mut BufStream<TcpStream>) -> String {
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
