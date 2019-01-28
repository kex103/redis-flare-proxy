use std::sync::Mutex;
use std::sync::Arc;
use client::BufferedClient;
use stats::Stats;
use redflareproxy::ClientTokenValue;
use redisprotocol::RedisError;
use redisprotocol::handle_slotsmap;
use redisprotocol::WriteError;
use std::net::SocketAddr;
use redflareproxy::PoolTokenValue;
use redflareproxy::convert_token_to_cluster_index;
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
use redisprotocol::{extract_key, KeyPos};

pub type Host = String;

pub struct ClusterBackendResource {
    slots: Vec<Host>,
    hostnames: HashMap<Host, BackendToken>,
    status: BackendStatus,
    queue: VecDeque<(ClientToken, Instant, usize)>,
    waiting_for_slotsmap_resp: bool,
}

pub struct ClusterBackend {
    config: BackendConfig,
    token: BackendToken,
    pool_token: PoolTokenValue,
    // Following are stored for future backend connections that can be established.
    timeout: usize,
    failure_limit: usize,
    retry_timeout: usize,
    poll_registry: Rc<RefCell<Poll>>,
    num_backends: usize,
    cached_backend_shards: Rc<RefCell<Option<Vec<usize>>>>,
    inner: Mutex<ClusterBackendResource>,
}
impl ClusterBackend {
    pub fn new(
        config: BackendConfig,
        token: BackendToken,
        cluster_backends: &mut Vec<(Arc<SingleBackend>, usize)>,
        poll_registry: &Rc<RefCell<Poll>>,
        next_cluster_token_value: &mut usize,
        timeout: usize,
        failure_limit: usize,
        retry_timeout: usize,
        pool_token: usize,
        num_backends: usize,
        cached_backend_shards: &Rc<RefCell<Option<Vec<usize>>>>,
    ) -> (ClusterBackend, Vec<BackendToken>) {
        let cluster = ClusterBackend {
            inner: Mutex::new(ClusterBackendResource {
                slots: Vec::with_capacity(16384),
                hostnames: HashMap::new(),
                status: BackendStatus::DISCONNECTED,
                queue: VecDeque::new(),
                waiting_for_slotsmap_resp: false,
            }),
            config: config,
            token: token,
            pool_token: pool_token,
            timeout: timeout,
            failure_limit: failure_limit,
            retry_timeout: retry_timeout,
            poll_registry: Rc::clone(poll_registry),
            num_backends: num_backends,
            cached_backend_shards: Rc::clone(cached_backend_shards),
        };

        let mut all_backend_tokens = Vec::with_capacity(cluster.config.cluster_hosts.len());
        {

        let mut inner_lock = cluster.inner.lock().unwrap();
        for _ in 0..inner_lock.slots.capacity() {
            inner_lock.slots.push("".to_owned());
        }

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
                &cluster.cached_backend_shards,
            );
            // KEX: make it so cluster_backends can be pushed to, because that actually changes.
            cluster_backends.push((Arc::new(single), token.0));
            inner_lock.hostnames.insert(host.to_string(), backend_token);
            all_backend_tokens.push(backend_token.clone());

        }

        }
        debug!("Initializing cluster");
        (cluster, all_backend_tokens)
    }

    pub fn reregister_token(&mut self, new_token: BackendToken, cluster_backends: &mut Vec<(Arc<SingleBackend>, usize)>, new_num_backends: usize) -> Result<(), std::io::Error> {
        self.token = new_token;
        self.num_backends = new_num_backends;

        for backend_token in self.inner.get_mut().unwrap().hostnames.values() {
            let client_index = convert_token_to_cluster_index(backend_token.0);
            match cluster_backends.get_mut(client_index) {
                Some((backend, _)) => {
                    Arc::get_mut(backend).unwrap().num_backends = new_num_backends;
                }
                None => {
                    panic!("ClusterBackend is referencing a Backend that does not exist! Occurred during reregistering token.");
                }
            };
        }
        return Ok(());
    }

    pub fn change_pool_token(&mut self, new_token_value: usize) {
        self.pool_token = new_token_value;
    }
 
    pub fn is_available(&self) -> bool {
        return self.inner.lock().unwrap().status == BackendStatus::READY;
    }

    pub fn init_connection(&self, cluster_backends: &mut Vec<(Arc<SingleBackend>, usize)>) {
        let mut inner_lock = self.inner.lock().unwrap();
        for backend_token in inner_lock.hostnames.values() {
            let client_index = convert_token_to_cluster_index(backend_token.0);
            match cluster_backends.get_mut(client_index) {
                Some((backend, _)) => {
                    backend.init_connection();
                }
                None => {
                    panic!("ClusterBackend is referencing a Backend that does not exist! Occurred when initializing connections.");
                }
            };
            // TODO: Should backend connection fail on the first connection? Perhaps a config option should determine
            // whether cluster needs to connect to all hosts, or just try one.
        }
        change_state(&mut inner_lock.status, BackendStatus::CONNECTING);
    }


    pub fn handle_backend_response(
        &self,
        backend_token: BackendToken,
        clients: &mut HashMap<usize, (BufferedClient, usize)>,
        next_cluster_token_value: &mut usize,
        cluster_backends: &mut Vec<(Arc<SingleBackend>, usize)>,
        completed_clients: &mut VecDeque<ClientTokenValue>,
        stats: &Stats,
    ) {
        let cluster_index = convert_token_to_cluster_index(backend_token.0);
        let mut additional_cluster_backends = Vec::new();
        let mut failed_slotsmap = false;

        // Accumulate all potential new cluster backends.
        {
            let mut resp_handler = |response: &[u8]| -> () {
                handle_unhandled_response(self, response, next_cluster_token_value, &mut additional_cluster_backends, &mut failed_slotsmap);
            };
            match cluster_backends.get_mut(cluster_index) {
                Some((backend, _)) => backend.handle_backend_response(clients, &mut resp_handler, completed_clients, stats),
                None => {
                    panic!("ClusterBackend is referencing a Backend that does not exist! Occurred when handling backend response.");
                }
            };
        }

        // Append new cluster backends to the permanent cluster backend collection.
        for (ref mut backend, _) in additional_cluster_backends.iter_mut() {
            backend.init_connection();
        }
        cluster_backends.append(&mut additional_cluster_backends);

        // Handle status changes.
        let inner_lock : &mut ClusterBackendResource = &mut self.inner.lock().unwrap();
        if inner_lock.status == BackendStatus::LOADING {
            if inner_lock.waiting_for_slotsmap_resp == false {
                change_state(&mut inner_lock.status, BackendStatus::READY);
                *self.cached_backend_shards.borrow_mut() = None;
            } else if failed_slotsmap {
                let hostnames = &inner_lock.hostnames;
                let mut queue = &mut inner_lock.queue;
                let mut status = &mut inner_lock.status;
                // Resend slotsmap request if previous request failed.
                for (_, b_token) in hostnames.iter() {
                    let cluster_index = convert_token_to_cluster_index(b_token.0);
                    let available = {
                        let cluster_backend = &cluster_backends.get(cluster_index).unwrap().0;
                        cluster_backend.is_available()
                    };
                    if available {
                        if initialize_slotmap(&mut queue, *b_token, cluster_backends, stats).is_ok() {
                            change_state(&mut status, BackendStatus::LOADING);
                            return;
                        }
                    }
                }
                // If none available, just wait, just set to CONNECTING.
                // TODO: Verify that there are backends that are actually connecting.
                change_state(&mut status, BackendStatus::CONNECTING);
                return;
            }
        }

        // This should only fire once for the cluster.
        if inner_lock.status == BackendStatus::CONNECTING {
            if initialize_slotmap(&mut inner_lock.queue, backend_token, cluster_backends, stats).is_ok() {
                inner_lock.waiting_for_slotsmap_resp = true;
                change_state(&mut inner_lock.status, BackendStatus::LOADING);
            }
        }
    }

    pub fn handle_backend_failure(
        &self,
        backend_token: BackendToken,
        clients: &mut HashMap<usize, (BufferedClient, usize)>,
        cluster_backends: &mut Vec<(Arc<SingleBackend>, usize)>,
        completed_clients: &mut VecDeque<ClientTokenValue>,
        stats: &Stats,
    ) {
        let cluster_index = convert_token_to_cluster_index(backend_token.0);
        cluster_backends.get_mut(cluster_index).unwrap().0.handle_backend_failure(clients, completed_clients, stats);
    }

    // callback when a timeout has occurred.
    pub fn handle_timeout(
        &self,
        backend_token: BackendToken,
        clients: &mut HashMap<usize, (BufferedClient, usize)>,
        cluster_backends: &mut Vec<(Arc<SingleBackend>, usize)>,
        completed_clients: &mut VecDeque<ClientTokenValue>,
        stats: &Stats,
    ) -> bool {
        let cluster_index = convert_token_to_cluster_index(backend_token.0);
        cluster_backends.get_mut(cluster_index).unwrap().0.handle_timeout(clients, completed_clients, stats);
        let mut inner_lock = self.inner.lock().unwrap();
        if inner_lock.queue.len() == 0 {
            return false;
        }
        let timeout = match inner_lock.queue.pop_front() {
            Some(t) => t,
            None => {
                error!("Received a timeout event, but no request in queue!");
                return false;
            }
        };
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
        let key = match key {
            KeyPos::Single(k) => k,
            _ => panic!("TODO: unsupported Multi and other keypos"),
        };
        let hash_no = State::<XMODEM>::calculate(key);
        let shard_no = hash_no % 16384;
        let inner_lock = self.inner.lock().unwrap();
        let hostname = inner_lock.slots.get(shard_no as usize).unwrap();
        return inner_lock.hostnames.get(hostname).unwrap().clone();
    }

    pub fn write_message(
        &self,
        message: &[u8],
        client_token: ClientToken,
        cluster_backends: &mut Vec<(Arc<SingleBackend>, usize)>,
        request_id: (Instant, usize),
        stats: &Stats,
    ) -> Result<(), WriteError> {
        // get the predicted backend to write to.
        let backend_token = self.get_shard(message);
        debug!("Cluster Writing to {:?}. Source: {:?}", backend_token, client_token);
        let cluster_index = convert_token_to_cluster_index(backend_token.0);
        try!(cluster_backends.get_mut(cluster_index).unwrap().0.write_message(message, client_token, request_id, stats));
        self.inner.lock().unwrap().queue.push_back(cluster_backends.get(cluster_index).unwrap().0.inner.lock().unwrap().queue.back().unwrap().clone());
        return Ok(());
    }
}

fn initialize_slotmap(
    queue: &mut VecDeque<(ClientToken, Instant, usize)>,
    backend_token: BackendToken,
    cluster_backends: &mut Vec<(Arc<SingleBackend>, usize)>,
    stats: &Stats,
) -> Result<(), WriteError> {
    let cluster_index = convert_token_to_cluster_index(backend_token.0);
    let ref mut host = cluster_backends.get_mut(cluster_index).unwrap().0;
    try!(host.write_message(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n", NULL_TOKEN, (Instant::now(), 0), stats));
    queue.push_back(host.inner.lock().unwrap().queue.back().unwrap().clone());
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

fn handle_unhandled_response(
    cluster: &ClusterBackend,
    response: &[u8],
    next_cluster_token_value: &mut usize,
    cluster_backends: &mut Vec<(Arc<SingleBackend>, usize)>,
    failed_slotsmap: &mut bool,
) {
    let mut inner_lock = cluster.inner.lock().unwrap();
    let mut handled_slotsmap = false;
    {
        let mut register_backend = |host:String, start: usize, end: usize| -> Result<(), RedisError> {
            debug!("Backend slots map registered! {} From {} to {}", host, start, end);

            for i in start..end+1 {
                inner_lock.slots.remove(i);
                inner_lock.slots.insert(i, host.clone());
            }

            if !inner_lock.hostnames.contains_key(&host) {
                let addr = match host.parse() {
                    Ok(a) => a,
                    Err(err) => {
                        error!("Unable to parse host: {}. Received error: {}", host, err);
                        return Err(RedisError::UnparseableHost);
                    }
                };
                initialize_host(
                    &mut inner_lock.hostnames,
                    cluster.token,
                    &cluster.config,
                    &cluster.poll_registry,
                    cluster.timeout,
                    cluster.failure_limit,
                    cluster.retry_timeout,
                    cluster.pool_token,
                    cluster.num_backends,
                    &cluster.cached_backend_shards,
                    addr,
                    next_cluster_token_value,
                    cluster_backends
                );
            }
            return Ok(());
        };
        // TODO: Verify the response is for a slotsmap
        match handle_slotsmap(&response, &mut register_backend) {
            Ok(_) => {
                handled_slotsmap = true;
            }
            Err(err) => {
                error!("Failed to parse slotsmap response. Received error: {:?}", err);
                *failed_slotsmap = true;
            }
        }
    }
    if handled_slotsmap {
        inner_lock.waiting_for_slotsmap_resp = false;
    }
}

/*
    Creates a connection to a cluster node.
*/
fn initialize_host(
    hostnames: &mut HashMap<Host, BackendToken>,
    self_token: Token,
    config: &BackendConfig,
    poll_registry: &Rc<RefCell<Poll>>,
    timeout: usize,
    failure_limit: usize,
    retry_timeout: usize,
    pool_token: PoolTokenValue,
    num_backends: usize,
    cached_backend_shards: &Rc<RefCell<Option<Vec<usize>>>>,
    host: SocketAddr,
    next_cluster_token_value: &mut usize,
    cluster_backends: &mut Vec<(Arc<SingleBackend>, usize)>,
) {
    let backend_token = Token(*next_cluster_token_value);
    *next_cluster_token_value += 1;
        let (single, _) = SingleBackend::new(
            config.clone(),
            host,
            backend_token,
            poll_registry,
            timeout,
            failure_limit,
            retry_timeout,
            pool_token,
            num_backends,
            cached_backend_shards,
        );
    cluster_backends.push((Arc::new(single), self_token.0));
    hostnames.insert(host.to_string(), backend_token.clone());
}