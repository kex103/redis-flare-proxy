use client::BufferedClient;
use client::Client;
use redflareproxy::ClientTokenValue;
use backend::write_to_stream;
use redflareproxy::{ADMIN_LISTENER};
use redflareproxy::{ClientToken};
use config::{AdminConfig};
use bufreader::BufReader;

use mio::*;
use mio::tcp::{TcpListener};
use hashbrown::HashMap;

pub struct AdminPort {
    pub client_sockets: HashMap<ClientTokenValue, BufferedClient>,
    pub socket: TcpListener,
    pub config: AdminConfig,
}

impl AdminPort {
    pub fn new(config: AdminConfig, poll : &Poll) -> AdminPort {
        // TODO: Add configuration for tcp backlog

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

        match poll.register(&server_socket, ADMIN_LISTENER, Ready::readable(), PollOpt::edge()) {
            Ok(_) => {}
            Err(error) => {
                panic!("Failed to register admin listener socket to poll. Reason: {:?}", error);
            }
        };
        debug!("Registered admin socket.");

        AdminPort {
            client_sockets: HashMap::new(),
            socket: server_socket,
            config: config,
        }
    }

    pub fn accept_client_connection(&mut self, next_admin_token: usize, poll: &mut Poll) {
        loop {
            match self.socket.accept() {
                Ok((s, _)) => {
                    let token = Token(next_admin_token);
                    match poll.register(&s, token, Ready::readable(), PollOpt::edge()) {
                        Ok(_) => {}
                        Err(error) => {
                            error!("Failed to register admin client socket to poll. Reason: {:?}", error);
                        }
                    };
                    self.client_sockets.insert(token.0, BufReader::new(Client::new(s)));
                }
                Err(error) => {
                    if error.kind() == std::io::ErrorKind::WouldBlock {
                        return;
                    }
                    error!("Unable to accept admin client connection. Reason: {:?}", error);
                    return;
                }
            }
        }
    }

    pub fn write_to_client(&mut self, client_token: ClientToken, message: String) {
        match self.client_sockets.get_mut(&client_token.0) {
            Some(client) => {
                match write_to_stream(&mut client.get_mut().stream, &message.into_bytes()[..]) {
                    Ok(_) => { return; }
                    Err(err) => {
                        debug!("Unable to write to admin client. Received error: {}", err);
                    }
                }
            }
            None => {
                debug!("No client found for admin: {:?}. Did a switch_config just occur?", client_token);
            }
        }

        self.client_sockets.remove(&client_token.0);
    }
}
