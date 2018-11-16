use mio::*;

use mio::unix::{UnixReady};
use mio::tcp::{TcpListener, TcpStream};
use std::time::{Instant, Duration};
use std::net::SocketAddr;
use std::io::Write;
use bufstream::BufStream;
use std::io::BufRead;
use std::ops::AddAssign;

type Host = String;
type Port = usize;
type FullHost = SocketAddr;
type ErrorMessage = String;

const BACKEND_SERVER: Token = Token(1);
const BACKEND_CONN: Token = Token(2);

const FIRST_CLIENT_TOKEN_INDEX: usize = 3;
const NUMBER_OF_CLIENTS: usize = 3;
const NUMBER_OF_REQUESTS: usize = 10000;

struct BenchmarkingClient {
    stream: Option<BufStream<TcpStream>>,
    last_time: Option<Instant>,
    min_time: Duration,
    max_time: Duration,
    id: usize,
}
impl BenchmarkingClient {
    pub fn new(
        proxy_addr: FullHost,
        event_poll: &mut Poll,
        id: usize,
    ) -> Result<BenchmarkingClient, ErrorMessage> {
        let stream = TcpStream::connect(&proxy_addr).unwrap();
        event_poll.register(
            &stream, Token(id + FIRST_CLIENT_TOKEN_INDEX),
            Ready::readable() | Ready::writable(),
            PollOpt::edge()
        ).unwrap();
        let client = BenchmarkingClient {
            stream: Some(BufStream::new(stream)),
            last_time: None,
            min_time: Duration::new(0, 0),
            max_time: Duration::new(0, 0),
            id: id,
        };
        Ok(client)
    }

    pub fn send_request(&mut self, request: String) {
        self.last_time = Some(Instant::now());
        match self.stream {
            Some(ref mut bufstream) => {
                bufstream.write(request.as_bytes()).unwrap();
                bufstream.flush().unwrap();
            }
            None => {
                panic!("No request");
            }
        }
    }

    pub fn handle_response(&mut self, report_results_fn: &mut FnMut(usize, Duration)) {
        if self.last_time == None {
            self.send_request("*2\r\n$3\r\nget\r\n$2\r\nhi\r\n".to_owned());
            return;
        }
        let mut reading = true;
        let mut buf = String::new();
        while reading {
            buf = String::new();
            match self.stream {
                Some(ref mut bufstream) => {
                    match bufstream.read_line(&mut buf) {
                        Ok(_) => {
                            debug!("Got response: {:?}", buf);
                            reading = false; // assume it's only a single line: +OK\r\n
                        }
                        Err(err) => {
                            error!("Got an error: {:?}", err);
                            reading = false;
                        }
                    }
                }
                None => {
                    panic!("no stream");
                }
            };
            if buf.len() == 0 {
                reading = false;
            }
        }
        self.process_response(buf, report_results_fn);
        self.send_request("*2\r\n$3\r\nget\r\n$2\r\nhi\r\n".to_owned());
    }

    fn process_response(
        &mut self,
        _response: String,
        report_results_fn: &mut FnMut(usize, Duration)
    ) {
        let time_delta = Instant::now() - self.last_time.unwrap();
        if time_delta < self.min_time {
            self.min_time = time_delta;
        } else if time_delta > self.max_time {
            self.max_time = time_delta;
        }

        // Report to main thread?
        debug!("Reporting results: id {} time_delta {:?}", self.id, time_delta.subsec_micros());
        (report_results_fn)(self.id, time_delta);
    }
}

pub struct ProxyBenchmarker {
    proxy_host: Host,
    proxy_port: Port,
    _proxy_backend_host: Host,
    _proxy_backend_port: Port,
    backend_listener: TcpListener,
    event_poll: Poll,
    backend_stream: Option<BufStream<TcpStream>>,
    client_streams: Vec<BenchmarkingClient>,
    requests_completed: usize,
    total_request_duration: Duration,
    longest_request_duration: Duration,
}
impl ProxyBenchmarker {
    pub fn new(
        proxy_host: Host,
        proxy_port: Port,
        proxy_backend_host: Host,
        proxy_backend_port: Port
    ) -> Result<ProxyBenchmarker, ErrorMessage> {
        let backend_addr = format!("{}:{}", proxy_backend_host, proxy_backend_port).parse().unwrap();

        // Establish backend listener.
        let backend_listener = match TcpListener::bind(&backend_addr) {
            Ok(socket) => socket,
            Err(error) => {
                panic!("Unable to bind to proxy backend: {:?}. Reason: {:?}", backend_addr, error);
            }
        };

        let poll = Poll::new().unwrap();
        match poll.register(&backend_listener, BACKEND_SERVER, Ready::readable(), PollOpt::edge()) {
            Ok(_) => {}
            Err(error) => {
                panic!("Failed to register proxy backend token to poll. Reason: {:?}", error);
            }
        };
        debug!("Registered backend socket.");


        let benchmarker = ProxyBenchmarker {
            proxy_host: proxy_host,
            proxy_port: proxy_port,
            _proxy_backend_host: proxy_backend_host,
            _proxy_backend_port: proxy_backend_port,
            event_poll: poll,
            backend_listener: backend_listener,
            backend_stream: None,
            client_streams: Vec::with_capacity(NUMBER_OF_CLIENTS),
            requests_completed: 0,
            total_request_duration: Duration::new(0, 0),
            longest_request_duration: Duration::new(0, 0),
        };
        Ok(benchmarker)
    }

    pub fn run(&mut self) {
        // Accept any backend connections from the proxy.
        let mut events = Events::with_capacity(1024);
        let timeout = Some(Duration::from_secs(10));
        match self.event_poll.poll(&mut events, timeout) {
            Ok(_poll_size) => {}
            Err(error) => {
                panic!("Error polling. Shutting down: {:?}", error);
            }
        };
        for event in events.iter() {
            debug!("Event detected: {:?} {:?}", &event.token(), event.readiness());
            self.handle_event(&event);
        }
        debug!("Moving to next phase");

        // Establish client connections. start timing.
        let proxy_addr: SocketAddr = format!("{}:{}", self.proxy_host, self.proxy_port).parse().unwrap();
        debug!("Connecting to {:?}", proxy_addr);
        for i in 0..NUMBER_OF_CLIENTS {
            self.client_streams.push(BenchmarkingClient::new(
                proxy_addr,
                &mut self.event_poll,
                i,
            ).unwrap());
        }

        // Handle backend requests. Send back a response to the proxy.
        // poll.
        events = Events::with_capacity(1024);
        let timeout = Some(Duration::from_secs(30));
        let mut is_running = true;
        while is_running {
            match self.event_poll.poll(&mut events, timeout) {
                Ok(_poll_size) => {}
                Err(error) => {
                    error!("Error polling: {:?}", error);
                    is_running = false;
                }
            };
            for event in events.iter() {
                debug!("Event detected: {:?} {:?}", &event.token(), event.readiness());
                self.handle_event(&event);
            }
            if self.requests_completed >= NUMBER_OF_REQUESTS {
                is_running = false;
            }
        }
        // print out results.
        println!("Average latency: {:?}", self.total_request_duration / self.requests_completed as u32);
        println!("Longest latency: {:?}", self.longest_request_duration);
        println!("All done! {}", self.requests_completed);
    }

    fn handle_backend(&mut self) {
        match self.backend_stream {
            Some(ref mut stream) => {
                let mut reading = true;
                while reading {
                    let mut buf = String::new(); 
                    match stream.read_line(&mut buf) {
                        Ok(_poll_size) => {
                            debug!("Got message from proxy: {:?}", buf);
                        }
                        Err(_err) => {
                        }
                    };
                    if buf.len() == 0 {
                        reading = false;
                    }
                }
                let _ = stream.write("+OK\r\n".as_bytes());
                let _ = stream.flush();
            }
            None => {
                panic!("Backend connection no longer exists");
            }
        };
    }

    fn handle_event(&mut self, event: &Event) {
        if event.readiness().contains(UnixReady::error()) {
            error!("Event error: {:?}", event);
            return;
        }
        match event.token() {
            BACKEND_SERVER => self.accept_backend_connection(),
            BACKEND_CONN => {
                info!("Received a backend message!");
                self.handle_backend();
            }
            t => {
                if t.0 >= FIRST_CLIENT_TOKEN_INDEX && t.0 < FIRST_CLIENT_TOKEN_INDEX + NUMBER_OF_CLIENTS {
                    let ref mut requests_completed = self.requests_completed;
                    let ref mut total_request_duration = self.total_request_duration;
                    let ref mut longest_request_duration = self.longest_request_duration;
                    let mut report_results_fn = move |_benchmarker_id:usize, request_duration:Duration| {
                        requests_completed.add_assign(1);
                        total_request_duration.add_assign(request_duration);
                        // Skip the first set of requests as they usually require initialization.
                        if requests_completed > &mut NUMBER_OF_CLIENTS && request_duration > *longest_request_duration {
                            println!("Longest latency is now: {:?} request number: {}", request_duration, requests_completed);
                            let a = request_duration - *longest_request_duration;
                            longest_request_duration.add_assign(a);
                        }
                    };
                    let client_id = t.0 - FIRST_CLIENT_TOKEN_INDEX;
                    match self.client_streams.get_mut(client_id) {
                        Some(stream) => {
                            stream.handle_response(&mut report_results_fn);
                        }
                        None => {
                            panic!("No client stream available for {:?}", client_id);
                        }
                    }
                } else {
                    error!("Token not recognized: {:?}", event.token());
                }
            }
        }
    }

    fn accept_backend_connection(&mut self) {
        let (c, _) = match self.backend_listener.accept() {
            Ok(conn) => conn,
            Err(error) => {
                panic!("Unable to accept backend connection. Reason: {:?}", error);
            }
        };
        match self.event_poll.register(&c, BACKEND_CONN, Ready::readable(), PollOpt::edge()) {
            Ok(_) => {}
            Err(error) => {
                panic!("Failed to register backend conn to poll. Reason: {:?}", error);
            }
        };
        self.backend_stream = Some(BufStream::new(c));
        debug!("Accepted backend connection.");
    }
}