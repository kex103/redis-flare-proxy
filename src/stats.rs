pub struct Stats {
    pub accepted_clients: usize,
    pub client_connections: usize,
    pub requests: usize,
    pub responses: usize,
    pub send_client_bytes: usize,
    pub recv_client_bytes: usize,
    pub send_backend_bytes: usize,
    pub recv_backend_bytes: usize,
}

impl Stats {
    pub fn new() -> Stats {
        Stats {
            accepted_clients: 0,
            client_connections: 0,
            requests: 0,
            responses: 0,
            send_client_bytes: 0,
            recv_client_bytes: 0,
            send_backend_bytes: 0,
            recv_backend_bytes: 0,
        }
    }

    pub fn reset(&mut self) {
        self.accepted_clients = 0;
        self.client_connections = 0;
        self.requests = 0;
        self.responses = 0;
        self.send_client_bytes = 0;
        self.recv_client_bytes = 0;
        self.send_backend_bytes = 0;
        self.recv_backend_bytes = 0;
    }
}
impl std::fmt::Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        try!(write!(f, "Stats:\n"));
        try!(write!(f, "accepted_clients: {}\n", self.accepted_clients));
        try!(write!(f, "client_connections: {}\n", self.client_connections));
        try!(write!(f, "requests: {}\n", self.requests));
        try!(write!(f, "responses: {}\n", self.responses));
        try!(write!(f, "send_client_bytes: {}\n", self.send_client_bytes));
        try!(write!(f, "recv_client_bytes: {}\n", self.recv_client_bytes));
        try!(write!(f, "send_backend_bytes: {}\n", self.send_backend_bytes));
        write!(f, "recv_backend_bytes: {}", self.recv_backend_bytes)
    }
}