use std::io::Read;
use mio::net::TcpStream;
use bufreader::BufReader;

pub struct Client {
    pub stream: TcpStream,
    // Used to house response for a multikey request.
    pub pending_response: Vec<Vec<u8>>,
    // Remaining number of responses needed for multikey request. 0 means that no multikey request is inflight.
    pub pending_count: usize,
}

impl Client {
    pub fn new(stream: TcpStream) -> Client {
        Client {
            stream: stream,
            pending_response: Vec::new(),
            pending_count: 0,
        }
    }
}

impl Read for Client {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.stream.read(buf)
    }
}

pub type BufferedClient = BufReader<Client>;
