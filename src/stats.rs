use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub struct Stats {
    pub accepted_clients: AtomicUsize,
    pub client_connections: AtomicUsize,
    pub requests: AtomicUsize,
    pub responses: AtomicUsize,
    pub send_client_bytes: AtomicUsize,
    pub recv_client_bytes: AtomicUsize,
    pub send_backend_bytes: AtomicUsize,
    pub recv_backend_bytes: AtomicUsize,
}

impl Stats {
    pub fn new() -> Stats {
        Stats {
            accepted_clients: AtomicUsize::new(0),
            client_connections: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            responses: AtomicUsize::new(0),
            send_client_bytes: AtomicUsize::new(0),
            recv_client_bytes: AtomicUsize::new(0),
            send_backend_bytes: AtomicUsize::new(0),
            recv_backend_bytes: AtomicUsize::new(0),
        }
    }

    pub fn reset(&mut self) {
        self.accepted_clients.store(0, Ordering::Release);
        self.client_connections.store(0, Ordering::Release);
        self.requests.store(0, Ordering::Release);
        self.responses.store(0, Ordering::Release);
        self.send_client_bytes.store(0, Ordering::Release);
        self.recv_client_bytes.store(0, Ordering::Release);
        self.send_backend_bytes.store(0, Ordering::Release);
        self.recv_backend_bytes.store(0, Ordering::Release);
    }
}
impl std::fmt::Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        try!(write!(f, "Stats:\n"));
        try!(write!(f, "accepted_clients: {}\n", self.accepted_clients.load(Ordering::Relaxed)));
        try!(write!(f, "client_connections: {}\n", self.client_connections.load(Ordering::Relaxed)));
        try!(write!(f, "requests: {}\n", self.requests.load(Ordering::Relaxed)));
        try!(write!(f, "responses: {}\n", self.responses.load(Ordering::Relaxed)));
        try!(write!(f, "send_client_bytes: {}\n", self.send_client_bytes.load(Ordering::Relaxed)));
        try!(write!(f, "recv_client_bytes: {}\n", self.recv_client_bytes.load(Ordering::Relaxed)));
        try!(write!(f, "send_backend_bytes: {}\n", self.send_backend_bytes.load(Ordering::Relaxed)));
        write!(f, "recv_backend_bytes: {}", self.recv_backend_bytes.load(Ordering::Relaxed))
    }
}