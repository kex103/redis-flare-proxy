use redisprotocol::handle_slotsmap;
use redisprotocol::WriteError;
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
use std::cell::{RefCell};
use std::rc::Rc;
use std;
use redisprotocol::extract_key;

pub type Host = String;

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

    pub fn reregister_token(&mut self, new_token: BackendToken, cluster_backends: &mut Vec<(SingleBackend, usize)>, new_num_backends: usize) -> Result<(), std::io::Error> {
        self.token = new_token;
        self.num_backends = new_num_backends;

        for backend_token in self.hostnames.values() {
            let client_index = convert_token_to_cluster_index(backend_token.0);
            let ref mut backend = cluster_backends.get_mut(client_index).unwrap().0;
            // TODO: Should backend connection fail on the first connection? Perhaps a config option should determine
            // whether cluster needs to connect to all hosts, or just try one.
            backend.num_backends = new_num_backends;
        }
        return Ok(());
    }

    pub fn change_pool_token(&mut self, new_token_value: usize) {
        self.pool_token = new_token_value;
    }
 
    pub fn is_available(&self) -> bool {
        return self.status == BackendStatus::READY;
    }

    pub fn init_connection(&mut self, cluster_backends: &mut Vec<(SingleBackend, usize)>) {
        for backend_token in self.hostnames.values() {
            let client_index = convert_token_to_cluster_index(backend_token.0);
            let ref mut backend = cluster_backends.get_mut(client_index).unwrap().0;
            // TODO: Should backend connection fail on the first connection? Perhaps a config option should determine
            // whether cluster needs to connect to all hosts, or just try one.
            backend.init_connection();
        }
        change_state(&mut self.status, BackendStatus::CONNECTING);
    }


    pub fn handle_backend_response(
        &mut self,
        backend_token: BackendToken,
        clients: &mut HashMap<usize, (Client, usize)>,
        next_cluster_token_value: &mut usize,
        cluster_backends: &mut Vec<(SingleBackend, usize)>,
    ) {
        debug!("Queue: {:?}", self.queue);

        // Check if state changed to READY. If so, will want to change self state to CONNECTED.
        let cluster_index = convert_token_to_cluster_index(backend_token.0);
        let unhandled_responses = cluster_backends.get_mut(cluster_index).unwrap().0.handle_backend_response(clients);

        // This should only fire once for the cluster.
        if self.status == BackendStatus::CONNECTING {
            if initialize_slotmap(&mut self.queue, backend_token, cluster_backends).is_ok() {
                change_state(&mut self.status, BackendStatus::LOADING);
            }
        }

        let mut initialized_slotsmap = false;
        // TODO: Handle multiple responses. Assuming it's slotsmap.
        for response in unhandled_responses {
            {
                {let mut register_backend = |host:String, start: usize, end: usize| {
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
                match handle_slotsmap(&response, &mut register_backend) {
                    Ok(_) => {
                        initialized_slotsmap = true;
                        continue;
                    }
                    Err(err) => {
                    }
                }}

                        // Bad slotsmap response. Mark the cluster backend as down, and should mark self as CONNECTING against.
                        // In addition, we need to send another initiale_slotsmap request. Perhaps it's simplest to retry everything?
                        cluster_backends.get_mut(cluster_index).unwrap().0.disconnect();
            }

            // Change status from LOADING to READY. This 
            if initialized_slotsmap {
                change_state(&mut self.status, BackendStatus::READY);
            } else {
                // Send another initialize_slotsmap.
                for (_, b_token) in self.hostnames.iter() {
                    let cluster_index = convert_token_to_cluster_index(b_token.0);
                    let available = {
                        let cluster_backend = &cluster_backends.get(cluster_index).unwrap().0;
                        cluster_backend.is_available()
                    };
                    if available {
                        if initialize_slotmap(&mut self.queue, *b_token, cluster_backends).is_ok() {
                            change_state(&mut self.status, BackendStatus::LOADING);
                            break;
                        }
                    }
                }
                // If none available, just wait, just set to CONNECTING>
                // But want to check that there are backends that are connecting.
                change_state(&mut self.status, BackendStatus::CONNECTING);
            }
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
    ) -> Result<(), WriteError> {
        // get the predicted backend to write to.
        let backend_token = self.get_shard(message);
        debug!("Cluster Writing to {:?}. Source: {:?}", backend_token, client_token);
        let cluster_index = convert_token_to_cluster_index(backend_token.0);
        try!(cluster_backends.get_mut(cluster_index).unwrap().0.write_message(message, client_token));
        self.queue.push_back(cluster_backends.get(cluster_index).unwrap().0.queue.back().unwrap().clone());
        return Ok(());
    }
}

fn initialize_slotmap(queue: &mut VecDeque<(ClientToken, Instant)>, backend_token: BackendToken, cluster_backends: &mut Vec<(SingleBackend, usize)>) -> Result<(), WriteError> {
    let cluster_index = convert_token_to_cluster_index(backend_token.0);
    let ref mut host = cluster_backends.get_mut(cluster_index).unwrap().0;
    try!(host.write_message(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n", NULL_TOKEN));
    queue.push_back(host.queue.back().unwrap().clone());
    return Ok(());
}

fn change_state(status: &mut BackendStatus, target_state: BackendStatus) -> bool {
    if *status == target_state {
        return true;
    }
    // For cluster, should go from CONNECTING (waiting for connection) => LOADING(Waiting for slotsmap) => READY (slotsmap returned)
    match (*status, target_state) {
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
            debug!("ClusterBackend failed to change state from {:?} to {:?}", status, target_state);
            panic!("Failure to change states");
        }
    }
    debug!("ClusterBackend changed state from {:?} to {:?}", status, target_state);
    *status = target_state;
    return true;
}
