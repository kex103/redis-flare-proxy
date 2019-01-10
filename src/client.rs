use mio::net::TcpStream;
use bufreader::BufReader;

pub struct Client {
    pub stream: BufReader<TcpStream>,
    // Used to house response for a multikey request.
    pub pending_response: Vec<Vec<u8>>,
    // Remaining number of responses needed for multikey request. 0 means that no multikey request is inflight.
    pub pending_count: usize,
}

impl Client {
    pub fn new(stream: TcpStream) -> Client {
        Client {
            stream: BufReader::new(stream),
            pending_response: Vec::new(),
            pending_count: 0,
        }
    }
}
