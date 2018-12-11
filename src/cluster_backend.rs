use std::net::SocketAddr;
use redflareproxy::PoolTokenValue;
use redflareproxy::convert_token_to_cluster_index;
use redflareproxy::Client;
use redflareproxy::{BackendToken, ClientToken, NULL_TOKEN};
use backend::{BackendStatus, SingleBackend};
use config::BackendConfig;
use std::collections::{VecDeque};
use hashbrown::HashMap;
use crc16::*;
use mio::{Token, Poll};
use std::time::Instant;
use std::str::FromStr;
use std::str::CharIndices;
use std::cell::{RefCell};
use std::rc::Rc;
use std::ops::Index;
use std;
use redisprotocol::extract_key;

pub type Host = String;

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
#[cfg(test)]
pub fn init_logging_info() {
    let stdout = ConsoleAppender::builder().build();
    let config = 
            Config::builder()
                .appender(Appender::builder().build("stdout", Box::new(stdout)))
                .build(Root::builder().appender("stdout").build(LogLevelFilter::Info))
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
            expect_eol(&mut chars);
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
    hostnames: HashMap<Host, BackendToken>,
    slots: Vec<Host>,
    status: BackendStatus,
    config: BackendConfig,
    token: BackendToken,
    queue: VecDeque<(ClientToken, Instant)>,
    pool_token: PoolTokenValue,
    // Following are stored for future backend connections that can be established.
    timeout: usize,
    failure_limit: usize,
    retry_timeout: usize,
    poll_registry: Rc<RefCell<Poll>>,
    num_backends: usize,
}
impl ClusterBackend {
    pub fn new(
        config: BackendConfig,
        token: BackendToken,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
        poll_registry: &Rc<RefCell<Poll>>,
        next_cluster_token_value: &mut usize,
        timeout: usize,
        failure_limit: usize,
        retry_timeout: usize,
        pool_token: usize,
        num_backends: usize,
    ) -> (ClusterBackend, Vec<BackendToken>) {
        let mut cluster = ClusterBackend {
            hostnames: HashMap::new(),
            slots: Vec::with_capacity(16384),
            config: config,
            status: BackendStatus::DISCONNECTED,
            token: token,
            queue: VecDeque::new(),
            pool_token: pool_token,
            timeout: timeout,
            failure_limit: failure_limit,
            retry_timeout: retry_timeout,
            poll_registry: Rc::clone(poll_registry),
            num_backends: num_backends,
        };
        for _ in 0..cluster.slots.capacity() {
            cluster.slots.push("".to_owned());
        }

        let mut all_backend_tokens = Vec::with_capacity(cluster.config.cluster_hosts.len());

        for host in &cluster.config.cluster_hosts {
            let backend_token = Token(*next_cluster_token_value);
            *next_cluster_token_value += 1;
            let (single, _) = SingleBackend::new(
                cluster.config.clone(),
                host.clone(),
                backend_token,
                poll_registry,
                timeout,
                failure_limit,
                retry_timeout,
                pool_token,
                num_backends,
            );
            cluster_backends.push((single, token.0));
            cluster.hostnames.insert(host.to_string(), backend_token);
            all_backend_tokens.push(backend_token.clone());

        }
        debug!("Initializing cluster");
        (cluster, all_backend_tokens)
    }

    fn initialize_host(&mut self, host: SocketAddr, next_cluster_token_value: &mut usize, cluster_backends: &mut Vec<(SingleBackend, usize)>) {
        let backend_token = Token(*next_cluster_token_value);
        *next_cluster_token_value += 1;
            let (single, _) = SingleBackend::new(
                self.config.clone(),
                host,
                backend_token,
                &self.poll_registry,
                self.timeout,
                self.failure_limit,
                self.retry_timeout,
                self.pool_token,
                self.num_backends,
            );
        cluster_backends.push((single, self.token.0));
        self.hostnames.insert(host.to_string(), backend_token.clone());
    }

    pub fn reregister_token(&mut self, new_token: BackendToken) -> Result<(), std::io::Error> {
        self.token = new_token;
        return Ok(());
    }

    pub fn change_pool_token(&mut self, new_token_value: usize) {
        self.pool_token = new_token_value;
    }
 
    pub fn is_available(&mut self) -> bool {
        return self.status == BackendStatus::READY;
    }

    pub fn connect(&mut self, cluster_backends: &mut Vec<(SingleBackend, usize)>) -> Result<(), std::io::Error> {
        for backend_token in self.hostnames.values() {
            let client_index = convert_token_to_cluster_index(backend_token.0);
            let ref mut backend = cluster_backends.get_mut(client_index).unwrap().0;
            // TODO: Should backend connection fail on the first connection? Perhaps a config option should determine
            // whether cluster needs to connect to all hosts, or just try one.
            try!(backend.connect());
        }
        self.change_state(BackendStatus::CONNECTING);
        return Ok(());
    }

    fn initialize_slotmap(&mut self, backend_token: BackendToken, cluster_backends: &mut Vec<(SingleBackend, usize)>, num_backends: usize) {
        let cluster_index = convert_token_to_cluster_index(backend_token.0);
        let ref mut host = cluster_backends.get_mut(cluster_index).unwrap().0;
        host.write_message(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n", NULL_TOKEN, num_backends);
        self.queue.push_back(host.queue.back().unwrap().clone());
    }

    pub fn handle_backend_response(
        &mut self,
        backend_token: BackendToken,
        clients: &mut HashMap<usize, (Client, usize)>,
        next_cluster_token_value: &mut usize,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
        num_backends: usize
    ) {
        debug!("Queue: {:?}", self.queue);

        // Check if state changed to READY. If so, will want to change self state to CONNECTED.
        let cluster_index = convert_token_to_cluster_index(backend_token.0);
        let unhandled_responses = cluster_backends.get_mut(cluster_index).unwrap().0.handle_backend_response(clients, num_backends);

        let prev_state = self.status;
        self.change_state(BackendStatus::LOADING);
        if prev_state == BackendStatus::CONNECTING && self.status == BackendStatus::LOADING {
            self.initialize_slotmap(backend_token, cluster_backends, num_backends);
        }

        // TODO: Handle multiple responses. Assuming it's slotsmap.
        for response in unhandled_responses {
            {
                let mut register_backend = |host:String, start: usize, end: usize| {
                    debug!("Backend slots map registered! {} From {} to {}", host, start, end);

                    for i in start..end+1 {
                        self.slots.remove(i);
                        self.slots.insert(i, host.clone());
                    }

                    if !self.hostnames.contains_key(&host) {
                        self.initialize_host(host.parse().unwrap(), next_cluster_token_value, cluster_backends);
                        let backend_token = self.hostnames.get(&host).unwrap();
                        let cluster_index = convert_token_to_cluster_index(backend_token.0);
                        // TODO: Handle connection failure.
                        cluster_backends.get_mut(cluster_index).unwrap().0.init_connection();
                    }
                };
                handle_slotsmap(response.clone(), &mut register_backend);
            }
            self.change_state(BackendStatus::READY);
            return;
        }
    }

    pub fn handle_backend_failure(
        &mut self,
        backend_token: BackendToken,
        clients: &mut HashMap<usize, (Client, usize)>,
        cluster_backends: &mut Vec<(SingleBackend, usize)>
    ) {
        let cluster_index = convert_token_to_cluster_index(backend_token.0);
        cluster_backends.get_mut(cluster_index).unwrap().0.handle_backend_failure(clients);
    }

    fn change_state(&mut self, target_state: BackendStatus) -> bool {
        if self.status == target_state {
            return true;
        }
        // For cluster, should go from CONNECTING (waiting for connection) => LOADING(Waiting for slotsmap) => READY (slotsmap returned)
        match (self.status, target_state) {
            (BackendStatus::DISCONNECTED, BackendStatus::CONNECTING) => {} // called when trying to establish a connection to backend.
            (BackendStatus::CONNECTING, BackendStatus::LOADING) => {}
            (BackendStatus::LOADING, BackendStatus::READY) => {}

            // State transitions we want to ignore. Why? Because we don't want to keep track of the fact that we're calling a transition to CONNECTED
            // twice. Instead, we'll have subsequent transitions just fail silently.
            (BackendStatus::READY, BackendStatus::LOADING) => {
                return true;
            }

            (BackendStatus::CONNECTING, BackendStatus::DISCONNECTED) => {} // Happens when the establishing connection to backend has timed out.
            (BackendStatus::LOADING, BackendStatus::DISCONNECTED) => {}
            (BackendStatus::READY, BackendStatus::DISCONNECTED) => {} // happens when host has been blacked out from too many failures/timeouts.
            _ => {
                debug!("ClusterBackend {:?} failed to change state from {:?} to {:?}", self.token, self.status, target_state);
                panic!("Failure to change states");
            }
        }
        debug!("ClusterBackend {:?} changed state from {:?} to {:?}", self.token, self.status, target_state);
        self.status = target_state;
        return true;
    }

    // callback when a timeout has occurred.
    pub fn handle_timeout(
        &mut self,
        backend_token: BackendToken,
        clients: &mut HashMap<usize, (Client, usize)>,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
    ) -> bool {
        let cluster_index = convert_token_to_cluster_index(backend_token.0);
        cluster_backends.get_mut(cluster_index).unwrap().0.handle_timeout(clients);
        if self.queue.len() == 0 {
            return false;
        }
        let timeout = self.queue.pop_front().unwrap();
        // TODO: Handle cluster timeouts.
        /*if target_timestamp < timeout.1 {
            return false;
        }
        if target_timestamp > timeout.1 {
            panic!("CusterBackend: handle_timeout: Timestamp greater than next queue timeout");
        }*/
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

    fn get_shard(&self, message: &[u8])-> BackendToken {
        let key = extract_key(&message).unwrap();
        let hash_no = State::<XMODEM>::calculate(key);
        let shard_no = hash_no % 16384;
        let hostname = self.slots.get(shard_no as usize).unwrap();
        debug!("KEX: Sharded to {}, which is {}. {:?}", shard_no as usize, hostname, self.hostnames.keys());
        return self.hostnames.get(hostname).unwrap().clone();
    }

    pub fn write_message(
        &mut self,
        message: &[u8],
        client_token: ClientToken,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
        num_backends: usize,
    ) -> bool {
        // get the predicted backend to write to.
        let backend_token = self.get_shard(message);
        debug!("Cluster Writing to {:?}. Source: {:?}", backend_token, client_token);
        let cluster_index = convert_token_to_cluster_index(backend_token.0);
        let result = cluster_backends.get_mut(cluster_index).unwrap().0.write_message(message, client_token, num_backends);
        if result {
            self.queue.push_back(cluster_backends.get(cluster_index).unwrap().0.queue.back().unwrap().clone());
            return true;
        }
        false
    }
}