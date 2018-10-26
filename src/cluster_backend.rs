use rustproxy::{BackendToken, PoolToken, ClientToken, generate_backend_token, StreamType, NULL_TOKEN, Subscriber};
use backend::{BackendStatus, Host, SingleBackend};
use backendpool::BackendPool;
use config::BackendConfig;
use bufstream::BufStream;
use mio::tcp::{TcpStream};
use std::collections::{VecDeque, HashMap};
use crc16::*;
use mio::Token;
use mio::Poll;
use std::io::Write;
use std::time::Instant;
use std::str::FromStr;
use std::str::CharIndices;
use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use std::ops::Index;
use std;
use redisprotocol::extract_key;

#[cfg(test)]
use log4rs;
#[cfg(test)]
use log::LogLevelFilter;
#[cfg(test)]
use log4rs::append::console::ConsoleAppender;
#[cfg(test)]
use log4rs::config::{Appender, Config, Root};
#[cfg(test)]
pub fn init_logging() {
    let stdout = ConsoleAppender::builder().build();
    let config = 
            Config::builder()
                .appender(Appender::builder().build("stdout", Box::new(stdout)))
                .build(Root::builder().appender("stdout").build(LogLevelFilter::Debug))
                .unwrap();

    match log4rs::init_config(config) {
        Ok(_) => {},
        Err(logger_error) => {
            println!("Logging error: {:?}", logger_error);
            return;
        }
    };
}

#[test]
fn test_slotsmap() {
    init_logging();
    let r = "*3\r\n*3\r\n:10922\r\n:16382\r\n*2\r\n$9\r\n127.0.0.1\r\n:7002\r\n*3\r\n:1\r\n:5460\r\n*2\r\n$9\r\n127.0.0.1\r\n:7000\r\n*3\r\n:5461\r\n:10921\r\n*2\r\n$9\r\n127.0.0.1\r\n:7001\r\n";
    let mut assigned_slots : Vec<Host>  = vec!["".to_owned(); 16384];
    {
    let mut count_slots = |host:String, start: usize, end: usize| {
        for i in start..end+1 {
            assigned_slots.remove(i-1);
            assigned_slots.insert(i-1, host.clone());
        }
    };
    handle_slotsmap(r.to_owned(), &mut count_slots);
    }
    for i in 0..5460 {
        assert_eq!(assigned_slots.get(i), Some(&"127.0.0.1:7000".to_owned()))
    }
    for i in 5460..10921 {
        assert_eq!(assigned_slots.get(i), Some(&"127.0.0.1:7001".to_owned()))
    }
    for i in 10921..16382 {
        assert_eq!(assigned_slots.get(i), Some(&"127.0.0.1:7002".to_owned()))
    }
}

// TODO: Move this to redisprotocol?
fn handle_slotsmap(
    response: String,
    handle_slots: &mut FnMut(String, usize, usize)
) {
    if response.len() == 0 {
        return;
    }
    let mut copy = response.clone();
    copy = str::replace(copy.as_str(), "\r", "\\r");
    copy = str::replace(copy.as_str(), "\n", "\\n\n");
    debug!("Handling slots map:\n{}", copy);
    // Populate the slots map.

    let mut chars = response.char_indices();
    let mut current_char = chars.next().unwrap();
    if current_char.1 != '*' {
        error!("Parse error: expected * at start of response. Found {:?} instead.", current_char);
    }
    let starting_index = parse_int(&mut chars, response.as_str());

    for _ in 0..starting_index {
        current_char = chars.next().unwrap();
        if current_char.1 != '*' {
            error!("Parse error: expected * at start of response. Found {:?} instead.", current_char);
        }
        let parsed_resp_length = parse_int(&mut chars, response.as_str());
        if parsed_resp_length < 3 {
            error!("Parse error: expected at least 3 lines for each response. Parsed {} instead.", parsed_resp_length);
        }
        // First two lines arer for slot range.
        // Next ones are for master and replicas.
    
        let mut hostname = "".to_string();
        let mut identifier = "".to_string();

        // Parse starting slot range.
        current_char = chars.next().unwrap();
        if current_char.1 != ':' {
            error!("Parse error: expected : at start of line to mark second level of array. Found {:?} instead.", current_char);
        }
        let starting_slot = parse_int(&mut chars, response.as_str());

        // Parse ending slot range.
        if chars.next().unwrap().1 != ':' {
            error!("parse error: expected :");
        }
        let ending_slot = parse_int(&mut chars, response.as_str());

        for _ in 0..parsed_resp_length-2 {
            current_char = chars.next().unwrap();
            if current_char.1 != '*' {
                error!("Parse error: expected * at start of response. Found {:?} instead.", current_char);
            }
            let parsed_slot_array_length = parse_int(&mut chars, response.as_str());
            // Can be 2 for older redis versions, and 3 for newer redis versions.

            current_char = chars.next().unwrap();
            if current_char.1 != '$' {
                error!("Parse error: 1st expected $ at start of line to mark second level of array. Found {:?} instead.", current_char);
            }
            let parsed_string_length = parse_int(&mut chars, response.as_str());
            for _ in 0..parsed_string_length {
                hostname.push(chars.next().unwrap().1);
            }
            expect_eol(&mut chars);

            current_char = chars.next().unwrap();
            if current_char.1 != ':' {
                error!("Parse error: expected : at start of line to mark second level of array. Found {:?} instead.", current_char);
            }
            let port = parse_int(&mut chars, response.as_str());

            if parsed_slot_array_length > 2 {
                current_char = chars.next().unwrap();
                if current_char.1 != '$' {
                    error!("Parse error: 2nd expected $ at start of line to mark second level of array. Found {:?} instead.", current_char);
                }
                let parsed_string_length = parse_int(&mut chars, response.as_str());
                for _ in 0..parsed_string_length {
                    identifier.push(chars.next().unwrap().1);
                }
            }

            // TODO. only do this for first one.
            let host = format!("{}:{}", hostname, port);
            handle_slots(host, starting_slot, ending_slot);
        }
    }
}

fn expect_eol(chars: &mut CharIndices) {
    let mut next = chars.next().unwrap();
    if next.1 != '\r' {
        error!("Parse error: expected \\r, found {:?} instead.", next);
    }
    next = chars.next().unwrap();
    if next.1 != '\n' {
        error!("Parse error: expected \\n, found {:?} instead.", next);
    }
}

fn parse_int(chars: &mut CharIndices, whole_string: &str) -> usize {
    let starting_index = match chars.next().unwrap() {
        (index, character) => {
            if !character.is_numeric() && character != '0' {
                panic!("Expected a numeric character, found {:?} instead!", character);
            }
            index
        }
    };
    loop {
        match chars.next().unwrap() {
            (index, character) => {
                if character == '\r' {
                    chars.next();
                    return usize::from_str(whole_string.index(std::ops::Range { start: starting_index, end: index})).unwrap();
                }
                if !character.is_numeric() {
                    panic!("Expected a numeric character, found {:?} instead!", character);
                }
            }
        }
    }
}

pub struct ClusterBackend {
    hosts: HashMap<BackendToken, SingleBackend>,
    hostnames: HashMap<Host, BackendToken>,
    slots: Vec<Host>,
    status: BackendStatus,
    config: BackendConfig,
    token: BackendToken,
    queue: VecDeque<(ClientToken, Instant)>,
    parent: *mut BackendPool,
    written_sockets: *mut VecDeque<(Token, StreamType)>,
    // Following are stored for future backend connections that can be established.
    timeout: usize,
    failure_limit: usize,
    retry_timeout: usize,
    backend_tokens_registry: Rc<RefCell<HashMap<BackendToken, PoolToken>>>,
    subscribers_registry: Rc<RefCell<HashMap<Token, Subscriber>>>,
    poll_registry: Rc<RefCell<Poll>>,
    next_socket_index: Rc<Cell<usize>>,
}
impl ClusterBackend {
    pub fn new(
        config: BackendConfig,
        token: BackendToken,
        backend_tokens_registry: &Rc<RefCell<HashMap<BackendToken, PoolToken>>>,
        subscribers_registry: &Rc<RefCell<HashMap<Token, Subscriber>>>,
        poll_registry: &Rc<RefCell<Poll>>,
        next_socket_index: &Rc<Cell<usize>>,
        timeout: usize,
        failure_limit: usize,
        retry_timeout: usize,
        pool: &mut BackendPool,
        written_sockets: &mut VecDeque<(Token, StreamType)>,
    ) -> (ClusterBackend, Vec<BackendToken>) {
        let hosts = HashMap::new();
        let mut cluster = ClusterBackend {
            hosts: hosts,
            hostnames: HashMap::new(),
            slots: Vec::with_capacity(16384),
            config: config,
            status: BackendStatus::DISCONNECTED,
            token: token,
            queue: VecDeque::new(),
            parent: pool as *mut BackendPool,
            written_sockets: written_sockets as *mut VecDeque<(Token, StreamType)>,
            timeout: timeout,
            failure_limit: failure_limit,
            retry_timeout: retry_timeout,
            backend_tokens_registry: Rc::clone(backend_tokens_registry),
            subscribers_registry: Rc::clone(subscribers_registry),
            poll_registry: Rc::clone(poll_registry),
            next_socket_index: Rc::clone(next_socket_index),
        };
        for _ in 0..cluster.slots.capacity() {
            cluster.slots.push("".to_owned());
        }

        let mut all_backend_tokens = Vec::with_capacity(cluster.config.cluster_hosts.len());

        for host in &cluster.config.cluster_hosts {
            let backend_token = generate_backend_token(&*cluster.next_socket_index, &*cluster.backend_tokens_registry, cluster.parent_token());
            let (single, _) = SingleBackend::new(
                cluster.config.clone(),
                host.clone(),
                backend_token,
                subscribers_registry,
                poll_registry,
                timeout,
                failure_limit,
                retry_timeout,
                pool,
                written_sockets,
            );
            cluster.hosts.insert(backend_token.clone(), single);
            cluster.hostnames.insert(host.clone(), backend_token);
            all_backend_tokens.push(backend_token.clone());

        }
        debug!("Initializing cluster");
        (cluster, all_backend_tokens)
    }

    fn initialize_host(&mut self, host: Host) {
        let backend_token = generate_backend_token(&*self.next_socket_index, &*self.backend_tokens_registry, self.parent_token());
            let (single, _) = SingleBackend::new(
                self.config.clone(),
                host.clone(),
                backend_token,
                &self.subscribers_registry,
                &self.poll_registry,
                self.timeout,
                self.failure_limit,
                self.retry_timeout,
                self.parent,
                self.written_sockets,
            );
        self.hosts.insert(backend_token.clone(), single);
        self.hostnames.insert(host.clone(), backend_token);
        // TODO: how to signal to pool that we have new backends?
    }
 
    pub fn is_available(&mut self) -> bool {
        return self.status == BackendStatus::CONNECTED;
    }

    pub fn connect(&mut self) {
        for (ref _host, ref mut backend) in &mut self.hosts {
            debug!("KEX: connecting for a host! {:?}", _host);
            backend.connect();
        }
        self.change_state(BackendStatus::CONNECTING, NULL_TOKEN);
    }

    pub fn handle_connection(&mut self, backend_token: Token) {
        let host = self.hosts.get_mut(&backend_token).unwrap();
        //host.handle_connection();
        host.write_message("*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n".to_owned(), NULL_TOKEN);
        self.queue.push_back(host.queue.back().unwrap().clone());
    }

    pub fn handle_backend_response(&mut self, token: Token) {
        // TODO: this should just call backend's handle_backend_response, and intercept any commands for slotsmap.

        // Grab response.
        // Look up queue. If it is token 0.
        // handle it.
        debug!("Queue: {:?}", self.queue);
        self.hosts.get_mut(&token).unwrap().change_state(BackendStatus::CONNECTED);
        self.change_state(BackendStatus::LOADING, token);
        let response = self.hosts.get_mut(&token).unwrap().get_backend_response();

        if response.len() == 0 {
            return;
        }
        let client_token = match self.queue.pop_front() {
            Some((client_token, _)) => client_token,
            None => {
                error!("!!!: No more client token in backend queue!");
                return;
            }
        };


        // BUG: TODO: Move this to handle_connection
        if client_token == NULL_TOKEN {
            {
            let mut register_backend = |host:String, start: usize, end: usize| {
                debug!("Backend slots map registered! {} From {} to {}", host, start, end);
                for i in start..end+1 {
                    self.slots.remove(i);
                    self.slots.insert(i, host.clone());
                }

                if !self.hostnames.contains_key(&host) {
                    self.initialize_host(host.clone());
                    let backend_token = self.hostnames.get(&host).unwrap();
                    self.hosts.get_mut(backend_token).unwrap().connect();
                    self.register_backend_with_parent(backend_token.clone(), self.token.clone());
                }
            };
            handle_slotsmap(response.clone(), &mut register_backend);
            }
            self.change_state(BackendStatus::CONNECTED, NULL_TOKEN);
            return;
        }

        match self.parent_clients().get_mut(&client_token) {
            Some(stream) => {
                debug!("Wrote to client: {:?}", response);
                let _ = stream.write(&response.into_bytes()[..]);
                self.register_written_socket(client_token, StreamType::PoolClient);
            }
            _ => error!("!!!: Found listener instead of stream!"),
        }

        // Check that backend has information in the socket still.
        if self.queue.len() > 0 {
            debug!("Still ahve leftover in backned!");
            self.handle_backend_response(token);
        }
    }

    pub fn handle_backend_failure(&mut self, token: BackendToken) {
        self.hosts.get_mut(&token).unwrap().handle_backend_failure();
    }

    fn change_state(&mut self, target_state: BackendStatus, backend_token: BackendToken) -> bool {

        if self.status == target_state {
            return true;
        }
        match (self.status, target_state) {
            (BackendStatus::DISCONNECTED, BackendStatus::CONNECTING) => {} // called when trying to establish a connection to backend.
            (BackendStatus::CONNECTING, BackendStatus::LOADING) => {
                self.handle_connection(backend_token);
            }

            // State transitions we want to ignore. Why? Because we don't want to keep track of the fact that we're calling a transition to CONNECTED
            // twice. Instead, we'll have subsequent transitions just fail silently.
            (BackendStatus::CONNECTED, BackendStatus::LOADING) => {
                return true;
            }

            (BackendStatus::LOADING, BackendStatus::CONNECTED) => {}
            (BackendStatus::CONNECTING, BackendStatus::DISCONNECTED) => {} // Happens when the establishing connection to backend has timed out.
            (BackendStatus::LOADING, BackendStatus::DISCONNECTED) => {}
            (BackendStatus::CONNECTED, BackendStatus::DISCONNECTED) => {} // happens when host has been blacked out from too many failures/timeouts.
            _ => {
                debug!("ClusterBackend {:?} failed to change state from {:?} to {:?}", self.token, self.status, target_state);
                panic!("Failure to change states");
            }
        }
        debug!("ClusterBackend {:?} changed state from {:?} to {:?}", self.token, self.status, target_state);
        self.status = target_state;
        return true;
    }

    pub fn next_timeout(&self) -> Option<Instant> {
        match self.queue.get(0) {
            Some(request) => Some(request.1),
            None => None,
        }
    }

    // callback when a timeout has occurred.
    pub fn handle_timeout(
        &mut self,
        backend_token: Token,
        target_timestamp: Instant
    ) -> bool {
        self.hosts.get_mut(&backend_token).unwrap().handle_timeout(target_timestamp);
        if self.queue.len() == 0 {
            return false;
        }
        let timeout = self.queue.pop_front().unwrap();
        if target_timestamp < timeout.1 {
            return false;
        }
        if target_timestamp > timeout.1 {
            panic!("CusterBackend: handle_timeout: Timestamp greater than next queue timeout");
        }
        match timeout.0 {
            NULL_TOKEN => {
                // try issueing another slot command.
            }
            token => {
                if token != backend_token {
                    panic!("ClusterBackend: handle_timeout: Tokens don't match! {:?} and {:?}", token, backend_token);
                }
            }

        }
        // How does blacking out a cluster due to excessive timeouts sound? Should happen on this side, and not SingleBackend's.
        // But should the cluster know how to migrate and ask for a new slot map?
        false
    }

    fn get_shard(&self, message: String)-> Token {
        let key = extract_key(&message).unwrap();
        let hash_no = State::<XMODEM>::calculate(key.as_bytes());
        let shard_no = hash_no % 16384;
        let hostname = self.slots.get(shard_no as usize).unwrap();
        debug!("KEX: Sharded to {}, which is {}. {:?}", shard_no as usize, hostname, self.hostnames.keys());
        return self.hostnames.get(hostname).unwrap().clone();
    }

    pub fn write_message(&mut self,
        message: String,
        client_token: Token
    ) -> bool {
        // get the predicted backend to write to.
        let backend_token = self.get_shard(message.clone());
        debug!("Cluster Writing to {:?}. Source: {:?}", backend_token, client_token);
        let result = self.hosts.get_mut(&backend_token).unwrap().write_message(message, client_token);
        if result {
            self.queue.push_back(self.hosts.get(&backend_token).unwrap().queue.back().unwrap().clone());
            return true;
        }
        false
    }

    pub fn flush_stream(&mut self) {
        for (_, ref mut backend) in &mut self.hosts {
            backend.flush_stream();
        }
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

    fn register_backend_with_parent(&self, backend_token: Token, owner_backend_token: Token) {
        unsafe {
            let parent_pool = &mut *self.parent;
            parent_pool.all_backend_tokens.insert(backend_token, owner_backend_token);
        }
    }

    fn register_written_socket(&self, token: Token, stream_type: StreamType) {
        let written_sockets = unsafe {
            &mut *self.written_sockets
        };
        written_sockets.push_back((token, stream_type));
    }
}