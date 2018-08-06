use rustproxy::{RustProxy, StreamType, Subscriber, SOCKET_INDEX_SHIFT, SERVER};
use rustproxy::{ClientToken};
use config::{AdminConfig};
use backend::parse_redis_command;

use mio::*;
use bufstream::BufStream;
use mio::tcp::{TcpListener, TcpStream};
use std::collections::*;
use std::io::Write;
use toml;
use std::process;
use std::cell::Cell;

pub struct AdminPort {
    pub client_sockets: HashMap<ClientToken, BufStream<TcpStream>>,
    pub socket: TcpListener,
    pub config: AdminConfig,
}

impl AdminPort {
    pub fn new(config: AdminConfig, poll : &Poll, subscribers: &mut HashMap<Token, Subscriber>) -> AdminPort {
        /*let mut tcp_backlog = 128; // SOMAXCONN
        if config.get("tcp-backlog") != None {
            tcp_backlog = config["tcp-backlog"].as_integer().unwrap();
        }*/

        let addr = match config.listen.parse() {
            Ok(addr) => addr,
            Err(error) => {
                panic!("Unable to parse the admin listen port from config: {}. Reason: {:?}", config.listen, error);
            }
        };

        // Setup the server socket
        let server_socket = match TcpListener::bind(&addr) {
            Ok(socket) => socket,
            Err(error) => {
                panic!("Unable to bind to admin list port: {:?}. Reason: {:?}", addr, error);
            }
        };

        match poll.register(&server_socket, SERVER, Ready::readable(), PollOpt::edge()) {
            Ok(_) => {}
            Err(error) => {
                panic!("Failed to register admin listener socket to poll. Reason: {:?}", error);
            }
        };
        subscribers.insert(SERVER, Subscriber::AdminListener);
        debug!("Registered admin socket.");

        AdminPort {
            client_sockets: HashMap::new(),
            socket: server_socket,
            config: config,
        }
    }

    pub fn accept_client_connection(&mut self, token_index: &Cell<usize>, poll: &mut Poll, subscribers: &mut HashMap<Token, Subscriber>) {
        let (c, _) = match self.socket.accept() {
            Ok(socket) => socket,
            Err(error) => {
                error!("Unable to accept admin client connection. Reason: {:?}", error);
                return;
            }
        };
        token_index.set(token_index.get() + SOCKET_INDEX_SHIFT);
        let token = Token(token_index.get().clone());
        match poll.register(&c, token, Ready::readable(), PollOpt::edge()) {
            Ok(_) => {}
            Err(error) => {
                error!("Failed to register admin client socket to poll. Reason: {:?}", error);
            }
        };
        subscribers.insert(token, Subscriber::AdminClient);
        self.client_sockets.insert(token, BufStream::new(c));
    }

    pub fn write_to_client(&mut self, client_token: ClientToken, message: String, written_sockets: &mut Box<VecDeque<(Token, StreamType)>>) {
        match self.client_sockets.get_mut(&client_token) {
            Some(client_stream) => {
                let _ = client_stream.write(&message.into_bytes()[..]);
                written_sockets.push_back((client_token, StreamType::AdminClient));
            }
            None => {
                debug!("No client found for admin: {:?}. Did a switch_config just occur?", client_token);
            }
        }
    }
}

// TODO: Refactor this to move into RustProxy impl or AdminPort impl.
// Trick here is that some client commands will require RustProxy-level changes.
// What will this look like?
// For AdminPort impl, it'll... have to return a string or something. No access to config.. so expose API from RustProxy to AdminPort
// Or... have RustProxy handle the handle_client_socket. But it's not a great abstraction...
// RustProxy needs: LOADCONFIG, GETSTAGEDCONFIG, GETCURRENTCONFIG, and SWITCH_CONFIG.

/*
Normal pattern is this:... Rustproxy-> handle_client_socket -> gets a messae, and then it returns it.
// But we need a call tor Rustproxy from handle_client_socket...? how to do? Or return internal commands?
API would be... Func, Params
*/
pub fn handle_client_socket(rustproxy: &mut RustProxy, token: ClientToken) {
    let mut switching_config = false;
    {
        let command = {
            let client_stream = match rustproxy.admin.client_sockets.get_mut(&token) {
                Some(stream) => stream,
                None => {
                    error!("AdminClient {:?} triggered an event, but it is no longer stored.", token);
                    return;
                }
            };
            parse_redis_command(client_stream)
        };
        debug!("RECEIVED COMMAND: {}", command);
        let mut lines = command.lines();
        let current_line = lines.next();
        let res = match current_line {
            None => {
                error!("AdminClient socket has nothing, when something was expected.");
                return;
            }
            Some("INFO") => {
                "DERP".to_owned()
            }
            Some("PING") => {
                "PONG".to_owned()
            }
            Some("LOADCONFIG") => {
                let next_line = lines.next();
                if next_line.is_none() {
                    "Missing filepath argument!".to_owned()
                } else {
                    let argument = next_line.unwrap();
                    rustproxy.load_config(argument.to_owned()).unwrap();
                    argument.to_owned()
                }
            }
            Some("SHUTDOWN") => {
                process::exit(0);
            }
            Some("STAGEDCONFIG") => {
                let staged_config = rustproxy.get_staged_config();
                if staged_config.is_none() {
                    "No config staged.".to_owned()
                } else {
                    toml::to_string(&staged_config).unwrap()
                }
            }
            Some("CONFIGINFO") => {
                toml::to_string(&rustproxy.get_current_config()).unwrap()
            }
            Some("SWITCHCONFIG") => {
                // TODO: Need to lose reference to the stream, OR
                // best is to orphan it. and respond OK.
                switching_config = true;
                // need to respond to socket later.switch_config(rustproxy
                "OK".to_owned()
            }
            Some(unknown_command) => {
                debug!("Unknown command: {}", unknown_command);
                "Unknown command".to_owned()
            }
        };
        if !switching_config {
            let mut response = String::new();
            response.push_str("+");
            response.push_str(res.as_str());
            response.push_str("\r\n");
            //response = "$-1\r\n".to_owned();
            debug!("RESPONSE: {}", &response);
            rustproxy.admin.write_to_client(token, response, &mut rustproxy.written_sockets);
            //let client_stream = rustproxy.admin.client_sockets.get_mut(&token).unwrap();
            //let _ = client_stream.write(&response.into_bytes()[..]);
            //rustproxy.written_sockets.push_back((token, StreamType::AdminClient));
        }   
    } 
    if switching_config {
        let result = {
            rustproxy.switch_config()
        };
        match result {
            Ok(_) => {
                let mut response = String::new();
                response.push_str("+");
                response.push_str("OK");
                response.push_str("\r\n");
                rustproxy.admin.write_to_client(token, response, &mut rustproxy.written_sockets);

            }
            Err(message) => {
                let mut response = String::new();
                response.push_str("-");
                response.push_str(message.as_str());
                response.push_str("\r\n");
                rustproxy.admin.write_to_client(token, response, &mut rustproxy.written_sockets);

            }
        }
    }
}

/*
fn handle_client_command(command: String) -> String {
    let mut lines = command.lines();
        let current_line = lines.next();
        let res = match current_line {
            None => {
                error!("AdminClient socket has nothing, when something was expected.");
                return "".to_owned();
            }
            Some("INFO") => {
                "DERP".to_owned()
            }
            Some("PING") => {
                "PONG".to_owned()
            }
            Some("LOADCONFIG") => {
                let next_line = lines.next();
                if next_line.is_none() {
                    "Missing filepath argument!".to_owned()
                } else {
                    let argument = next_line.unwrap();
                    rustproxy.load_config(argument.to_owned()).unwrap();
                    argument.to_owned()
                }
            }
            Some("SHUTDOWN") => {
                process::exit(0);
            }
            Some("STAGEDCONFIG") => {
                let staged_config = rustproxy.get_staged_config();
                if staged_config.is_none() {
                    "No config staged.".to_owned()
                } else {
                    toml::to_string(&staged_config).unwrap()
                }
            }
            Some("CONFIGINFO") => {
                toml::to_string(&rustproxy.get_current_config()).unwrap()
            }
            Some("SWITCHCONFIG") => {
                // TODO: Need to lose reference to the stream, OR
                // best is to orphan it. and respond OK.
                // need to respond to socket later.switch_config(rustproxy
                "OK".to_owned()
            }
            Some(unknown_command) => {
                debug!("Unknown command: {}", unknown_command);
                "Unknown command".to_owned()
            }
        };

    return "".to_owned();
}*/
