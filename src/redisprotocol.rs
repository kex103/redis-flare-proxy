#[cfg(test)]
use {init_logging, init_logging_info};
#[cfg(test)]
use cluster_backend::Host;
use memchr::memchr;
use std::result::Result;

#[cfg(test)]
use std::time::Instant;
use std::fmt;

use std::error;
use std::net::SocketAddr;

#[derive(Debug)]
pub enum WriteError {
    NoSocket,
    BufOutOfBounds,
    BackendNotReady,
    WriteFailure(Option<SocketAddr>, std::io::Error),
}
impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WriteError::NoSocket => write!(f, "No TcpStream for this backend"),
            WriteError::BufOutOfBounds => write!(f, "This should be impossible. Somehow send wrote more bytes than the buffer size"),
            WriteError::BackendNotReady => write!(f, "Backend is not available."),
            WriteError::WriteFailure(ref s, ref e) => write!(f, "Failed to write to stream: {:?}. Received error: {}.", s, e),
        }
    }
}
impl error::Error for WriteError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            WriteError::NoSocket => None,
            WriteError::BufOutOfBounds => None,
            WriteError::BackendNotReady => None,
            WriteError::WriteFailure(_, ref e) => Some(e),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum RedisError {
    NoBackend,
    Unknown(Vec<u8>),
    UnsupportedCommand,
    InvalidScript,
    InvalidProtocol,
    UnparseableHost,
    IncompleteMessage,
    MissingArgsMget,
    MissingArgsMset,
    WrongArgsMset,
}
impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RedisError::Unknown(v) => {
                match std::str::from_utf8(v) {
                    Ok(s) => write!(f, "RedisError::Unknown {}", s),
                    Err(_err) => write!(f, "RedisError::Unknown {:?}", v),
                }
            }
            err => write!(f, "{:?}", err),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum KeyPos<'a> {
    Single(&'a [u8]),
    Multi(Vec<&'a [u8]>),
    MultiSet(Vec<(&'a [u8], &'a [u8])>),
}

enum KeyPosition {
    Next,
    Multi,
    MultiInterleaved,
    Unsupported,
    Eval,
}

#[test]
fn test_parsing_redis() {
    init_logging();
    let req = b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n";
    let res = extract_key(req);
    assert_eq!(res, Ok(KeyPos::Single(b"key1")));
    let req = b"*5\r\n$4\r\nEVAL\r\n$40\r\nreturn redis.call('set',KEYS[1],ARGV[1])\r\n$1\r\n1\r\n$5\r\nkey10\r\n$7\r\nvalue10";
    let res = extract_key(req);
    assert_eq!(res, Ok(KeyPos::Single(b"key10")));
    let req = b"*4\r\n$4\r\nMGET\r\n$2\r\nab\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n";
    let res = extract_key(req);
    assert_eq!(res, Ok(KeyPos::Multi(vec!(b"ab", b"key2", b"key3"))));
    let req = b"*5\r\n$4\r\nMSET\r\n$2\r\nab\r\n$2\r\ncd\r\n$4\r\nkey2\r\n$0\r\n\r\n";
    let res = extract_key(req);
    assert_eq!(res, Ok(KeyPos::MultiSet(vec!((b"ab", b"cd"), (b"key2", b"")))));
}

#[test]
fn test_parsing_speed() {
    let num_runs = 10000000;
    init_logging_info();
    let a = "*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n".to_string();
    let b = a.as_bytes();

    // Using this test function to test how fast hashing can be.
    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = extract_key(b);
    }
    info!("Time spent with extract_key: {:?}", Instant::now() - start);
}

fn skip_past_eol(bytes: &[u8], index: &mut usize) -> Result<(), RedisError> {
    let bytes2 = unsafe { bytes.get_unchecked(*index..) };
    match memchr('\n' as u8, bytes2) {
        Some(delta) => {
            *index += delta + 1;
            return Ok(());
        }
        None => {
            return Err(RedisError::Unknown(bytes.to_vec()));
        }
    }
}

pub fn interpret_num(bytes: &[u8], index: &mut usize) -> Result<isize, RedisError> {
    let mut negative = false;
    let mut result = 0;
    loop {
        match unsafe {*bytes.get_unchecked(*index) as char} {
            '0' => { result = result * 10; }
            '1' => { result = result * 10 + 1; }
            '2' => { result = result * 10 + 2; }
            '3' => { result = result * 10 + 3; }
            '4' => { result = result * 10 + 4; }
            '5' => { result = result * 10 + 5; }
            '6' => { result = result * 10 + 6; }
            '7' => { result = result * 10 + 7; }
            '8' => { result = result * 10 + 8; }
            '9' => { result = result * 10 + 9; }
            '-' => { negative = true; }
            '\r' => {
                if negative {
                    return Ok(-result);
                } else {
                    return Ok(result);
                }
            }
            _ => {
                return Err(RedisError::InvalidProtocol);
            }
        }
        *index += 1;
    }
}

#[test]
fn test_extract_redis_command() {
    init_logging();

    let a = "$-1\r\naeras".to_string();
    let resp = extract_redis_command(&a.as_bytes());
    assert_eq!(resp, Ok("$-1\r\n".as_bytes()));

    let a = "+THREE\r\naeras".to_string();
    let resp = extract_redis_command(&a.as_bytes());
    assert_eq!(resp, Ok("+THREE\r\n".as_bytes()));

    let a = "-FERAC\r\ndera".to_string();
    let resp = extract_redis_command(&a.as_bytes());
    assert_eq!(resp, Ok("-FERAC\r\n".as_bytes()));

    let a = ":1234567\r\ndera".to_string();
    let resp = extract_redis_command(&a.as_bytes());
    assert_eq!(resp, Ok(":1234567\r\n".as_bytes()));

    let a = "$4\r\nTHRE\r\ndera".to_string();
    let resp = extract_redis_command(&a.as_bytes());
    assert_eq!(resp, Ok("$4\r\nTHRE\r\n".as_bytes()));

    let a = "*3\r\n+dera\r\n$2\r\nab\r\n*2\r\n$4\r\nBLAR\r\n:34\r\nadaerare".to_string();
    let resp = extract_redis_command(&a.as_bytes());
    assert_eq!(resp, Ok("*3\r\n+dera\r\n$2\r\nab\r\n*2\r\n$4\r\nBLAR\r\n:34\r\n".as_bytes()));

    let a = "*3\r\n*3\r\n:10922\r\n:14624\r\n*3\r\n$9\r\n127.0.0.1\r\n:7002\r\n$40\r\ncb0a0a8d38708ce6369a969854e6076e3b3133f5\r\n".to_string();
    let resp = extract_redis_command(&a.as_bytes());
    assert_eq!(resp, Ok("*3\r\n*3\r\n:10922\r\n:14624\r\n*3\r\n$9\r\n127.0.0.1\r\n:7002\r\n$40\r\ncb0a0a8d38708ce6369a969854e6076e3b3133f5\r\n".as_bytes()));
}

pub fn extract_redis_command(bytes: &[u8]) -> Result<&[u8], RedisError> {
    let mut index = 0;
    try!(parse_redis_request(bytes, &mut index));
    return unsafe { Ok(bytes.get_unchecked(0..index)) };
}

/*
    Iterates through one redis request in bytes, moving the index to the end of the request.
*/
fn parse_redis_request(bytes: &[u8], index: &mut usize) -> Result<(), RedisError> {
    let next_char = match bytes.get(*index) {
        Some(c) => *c as char,
        None => { return Err(RedisError::IncompleteMessage); }
    };
    match next_char {
        '+' =>  {
            *index += 1;
            try!(skip_past_eol(bytes, index));
            return Ok(());
        }
        '-' =>  {
            *index += 1;
            try!(skip_past_eol(bytes, index));
            return Ok(());
        }
        ':' =>  {
            *index += 1;
            try!(skip_past_eol(bytes, index));
            return Ok(());
        }
        '$' => {
            *index += 1;
            let num = try!(interpret_num(bytes, index));
            *index += 2;
            if num < 0 {
                return Ok(());
            }
            *index += num as usize + 2;
            if *index > bytes.len() {
                return Err(RedisError::IncompleteMessage);
            }
            return Ok(());
        }
        '*' => {
            *index += 1;
            let num = try!(interpret_num(bytes, index));
            *index += 2;
            for _ in 0..num {
                try!(parse_redis_request(bytes, index));
            }
            return Ok(());
            
        }
        _ => { return Err(RedisError::InvalidProtocol); }
    }
}

pub fn extract_key(bytes: &[u8]) -> Result<KeyPos, RedisError> {
    if bytes[0] == '*' as u8 {
        // then it is standard redis protcol.
        let mut index = 0;

        // skip 1
        try!(skip_past_eol(&bytes, &mut index));

        // verify next byte is '$'
        if '$' as u8 != unsafe { *bytes.get_unchecked(index) } {
            return Err(RedisError::InvalidProtocol);
        }
        index += 1;
        let num = try!(interpret_num(bytes, &mut index)) as usize;
        index += 2;

        // grab the command list.
        let command = unsafe {
            bytes.get_unchecked(index..index+num)
        };

        match supported_keys(command) {
            KeyPosition::Unsupported => { return Err(RedisError::UnsupportedCommand); }
            KeyPosition::Next => {
                index += num + 2;

                if '$' as u8 != unsafe { *bytes.get_unchecked(index) } {
                    return Err(RedisError::InvalidProtocol);
                }
                index += 1;
                let n = try!(interpret_num(bytes, &mut index));
                if n < 0 {
                    return Err(RedisError::InvalidProtocol);
                }
                let num = n as usize;
                index += 2;

                // grab the command list.
                let key = unsafe {
                    bytes.get_unchecked(index..index+num)
                };
                return Ok(KeyPos::Single(key));
            }
            KeyPosition::Eval => {
                index += num + 2;
                if '$' as u8 != unsafe { *bytes.get_unchecked(index) } {
                    return Err(RedisError::InvalidProtocol);
                }
                index += 1;
                let num = try!(interpret_num(bytes, &mut index)) as usize;
                index += 2;
                
                index += num + 2;
                if '$' as u8 != unsafe { *bytes.get_unchecked(index) } {
                    return Err(RedisError::InvalidProtocol);
                }
                index += 1;
                let num = try!(interpret_num(bytes, &mut index)) as usize;
                index += 2;

                let num_keys = unsafe {
                    bytes.get_unchecked(index..index+num)
                };
                if num_keys != ['1' as u8] {
                    return Err(RedisError::InvalidScript);
                }

                index += num + 2;
                if '$' as u8 != unsafe { *bytes.get_unchecked(index) } {
                    return Err(RedisError::InvalidProtocol);
                }
                index += 1;
                let num = try!(interpret_num(bytes, &mut index)) as usize;
                index += 2;

                let key = unsafe {
                    bytes.get_unchecked(index..index+num)
                };
                return Ok(KeyPos::Single(key));
            }
            KeyPosition::Multi => {
                // Go back to the beginning to determine number of keys.
                let mut temp = 1;
                let num_keys = try!(interpret_num(bytes, &mut temp)) as usize - 1;

                let mut vec = Vec::new();

                let mut num = num;

                for _ in 0..num_keys {
                    index += num + 2;

                    if '$' as u8 != unsafe { *bytes.get_unchecked(index) } {
                        return Err(RedisError::InvalidProtocol);
                    }
                    index += 1;
                    let n = try!(interpret_num(bytes, &mut index));
                    if n < 0 {
                        return Err(RedisError::InvalidProtocol);
                    }
                    num = n as usize;
                    index += 2;

                    // grab the command list.
                    let key = unsafe {
                        bytes.get_unchecked(index..index+num)
                    };
                    vec.push(key);
                }
                if vec.len() == 0 {
                    return Err(RedisError::MissingArgsMget);
                }
                return Ok(KeyPos::Multi(vec));
            }
            KeyPosition::MultiInterleaved => {
                // Go back to the beginning to determine number of keys.
                let mut temp = 1;
                let num_keys = try!(interpret_num(bytes, &mut temp)) as usize - 1;

                let mut vec = Vec::new();

                let mut num = num;

                if num_keys == 0 {
                    return Err(RedisError::MissingArgsMset);
                }
                if num_keys % 2 == 1 {
                    return Err(RedisError::WrongArgsMset);
                }

                for _ in 0..(num_keys)/2 {
                    index += num + 2;

                    if '$' as u8 != unsafe { *bytes.get_unchecked(index) } {
                        return Err(RedisError::InvalidProtocol);
                    }
                    index += 1;
                    let n = try!(interpret_num(bytes, &mut index));
                    if n < 0 {
                        return Err(RedisError::InvalidProtocol);
                    }
                    num = n as usize;
                    index += 2;

                    // grab the command list.
                    let key = unsafe {
                        bytes.get_unchecked(index..index+num)
                    };

                    index += num + 2;

                    if '$' as u8 != unsafe { *bytes.get_unchecked(index) } {
                        return Err(RedisError::InvalidProtocol);
                    }
                    index += 1;
                    let n = try!(interpret_num(bytes, &mut index));
                    if n < 0 {
                        return Err(RedisError::InvalidProtocol);
                    }
                    num = n as usize;
                    index += 2;

                    // grab the command list.
                    let value = unsafe {
                        bytes.get_unchecked(index..index+num)
                    };
                    vec.push((key, value));

                }
                return Ok(KeyPos::MultiSet(vec));
            }
        };
    } else {
        panic!("Unimplemented support for plain text commands");
    }
}

fn supported_keys(command: &[u8]) -> KeyPosition {
    match command.len() {
        3 => {
            if str3compare(command, 'G', 'E', 'T') { return KeyPosition::Next; }
            if str3compare(command, 'D', 'E', 'L') { return KeyPosition::Next; }
            if str3compare(command, 'S', 'E', 'T') { return KeyPosition::Next; }
            if str3compare(command, 'T', 'T', 'L') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        4 => {
            if str4compare(command, 'E', 'V', 'A', 'L') { return KeyPosition::Eval; }
            if str4compare(command, 'M', 'G', 'E', 'T') { return KeyPosition::Multi; }
            if str4compare(command, 'M', 'S', 'E', 'T') { return KeyPosition::MultiInterleaved; }
            if str4compare(command, 'D', 'U', 'M', 'P') { return KeyPosition::Next; }
            if str4compare(command, 'P', 'T', 'T', 'L') { return KeyPosition::Next; }
            if str4compare(command, 'S', 'O', 'R', 'T') { return KeyPosition::Next; }
            if str4compare(command, 'T', 'Y', 'P', 'E') { return KeyPosition::Next; }
            if str4compare(command, 'D', 'E', 'C', 'R') { return KeyPosition::Next; }
            if str4compare(command, 'I', 'N', 'C', 'R') { return KeyPosition::Next; }
            if str4compare(command, 'H', 'D', 'E', 'L') { return KeyPosition::Next; }
            if str4compare(command, 'H', 'G', 'E', 'T') { return KeyPosition::Next; }
            if str4compare(command, 'H', 'L', 'E', 'N') { return KeyPosition::Next; }
            if str4compare(command, 'H', 'S', 'E', 'T') { return KeyPosition::Next; }
            if str4compare(command, 'L', 'L', 'E', 'N') { return KeyPosition::Next; }
            if str4compare(command, 'L', 'P', 'O', 'P') { return KeyPosition::Next; }
            if str4compare(command, 'L', 'R', 'E', 'M') { return KeyPosition::Next; }
            if str4compare(command, 'L', 'S', 'E', 'T') { return KeyPosition::Next; }
            if str4compare(command, 'R', 'P', 'O', 'P') { return KeyPosition::Next; }
            if str4compare(command, 'S', 'A', 'D', 'D') { return KeyPosition::Next; }
            if str4compare(command, 'S', 'P', 'O', 'P') { return KeyPosition::Next; }
            if str4compare(command, 'S', 'R', 'E', 'M') { return KeyPosition::Next; }
            if str4compare(command, 'Z', 'A', 'D', 'D') { return KeyPosition::Next; }
            if str4compare(command, 'Z', 'R', 'E', 'M') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        5 => {
            if str5compare(command, 'T', 'O', 'U', 'C', 'H') { return KeyPosition::Next; }
            if str5compare(command, 'S', 'E', 'T', 'E', 'X') { return KeyPosition::Next; }
            if str5compare(command, 'S', 'E', 'T', 'N', 'X') { return KeyPosition::Next; }
            if str5compare(command, 'H', 'K', 'E', 'Y', 'S') { return KeyPosition::Next; }
            if str5compare(command, 'H', 'M', 'G', 'E', 'T') { return KeyPosition::Next; }
            if str5compare(command, 'H', 'M', 'S', 'E', 'T') { return KeyPosition::Next; }
            if str5compare(command, 'H', 'S', 'C', 'A', 'N') { return KeyPosition::Next; }
            if str5compare(command, 'H', 'V', 'A', 'L', 'S') { return KeyPosition::Next; }
            if str5compare(command, 'B', 'L', 'P', 'O', 'P') { return KeyPosition::Next; }
            if str5compare(command, 'B', 'R', 'P', 'O', 'P') { return KeyPosition::Next; }
            if str5compare(command, 'L', 'P', 'U', 'S', 'H') { return KeyPosition::Next; }
            if str5compare(command, 'L', 'T', 'R', 'I', 'M') { return KeyPosition::Next; }
            if str5compare(command, 'R', 'P', 'U', 'S', 'H') { return KeyPosition::Next; }
            if str5compare(command, 'S', 'C', 'A', 'R', 'D') { return KeyPosition::Next; }
            if str5compare(command, 'S', 'S', 'C', 'A', 'N') { return KeyPosition::Next; }
            if str5compare(command, 'Z', 'C', 'A', 'R', 'D') { return KeyPosition::Next; }
            if str5compare(command, 'Z', 'R', 'A', 'N', 'K') { return KeyPosition::Next; }
            if str5compare(command, 'Z', 'S', 'C', 'A', 'N') { return KeyPosition::Next; }
            if str5compare(command, 'P', 'F', 'A', 'D', 'D') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        6 => {
            if str6compare(command, 'E', 'X', 'I', 'S', 'T', 'S') { return KeyPosition::Next; }
            if str6compare(command, 'E', 'X', 'P', 'I', 'R', 'E') { return KeyPosition::Next; }
            if str6compare(command, 'U', 'N', 'L', 'I', 'N', 'K') { return KeyPosition::Next; }
            if str6compare(command, 'A', 'P', 'P', 'E', 'N', 'D') { return KeyPosition::Next; }
            if str6compare(command, 'B', 'I', 'T', 'P', 'O', 'S') { return KeyPosition::Next; }
            if str6compare(command, 'D', 'E', 'C', 'R', 'B', 'Y') { return KeyPosition::Next; }
            if str6compare(command, 'G', 'E', 'T', 'B', 'I', 'T') { return KeyPosition::Next; }
            if str6compare(command, 'G', 'E', 'T', 'S', 'E', 'T') { return KeyPosition::Next; }
            if str6compare(command, 'I', 'N', 'C', 'R', 'B', 'Y') { return KeyPosition::Next; }
            if str6compare(command, 'P', 'S', 'E', 'T', 'E', 'X') { return KeyPosition::Next; }
            if str6compare(command, 'S', 'E', 'T', 'B', 'I', 'T') { return KeyPosition::Next; }
            if str6compare(command, 'S', 'T', 'R', 'L', 'E', 'N') { return KeyPosition::Next; }
            if str6compare(command, 'H', 'S', 'E', 'T', 'N', 'X') { return KeyPosition::Next; }
            if str6compare(command, 'L', 'I', 'N', 'D', 'E', 'X') { return KeyPosition::Next; }
            if str6compare(command, 'L', 'P', 'U', 'S', 'H', 'X') { return KeyPosition::Next; }
            if str6compare(command, 'L', 'R', 'A', 'N', 'G', 'E') { return KeyPosition::Next; }
            if str6compare(command, 'R', 'P', 'U', 'S', 'H', 'X') { return KeyPosition::Next; }
            if str6compare(command, 'Z', 'C', 'O', 'U', 'N', 'T') { return KeyPosition::Next; }
            if str6compare(command, 'Z', 'R', 'A', 'N', 'G', 'E') { return KeyPosition::Next; }
            if str6compare(command, 'Z', 'S', 'C', 'O', 'R', 'E') { return KeyPosition::Next; }
            if str6compare(command, 'G', 'E', 'O', 'A', 'D', 'D') { return KeyPosition::Next; }
            if str6compare(command, 'G', 'E', 'O', 'P', 'O', 'S') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        7 => {
            if str7compare(command, 'P', 'E', 'R', 'S', 'I', 'S', 'T') { return KeyPosition::Next; }
            if str7compare(command, 'P', 'E', 'X', 'P', 'I', 'R', 'E') { return KeyPosition::Next; }
            if str7compare(command, 'R', 'E', 'S', 'T', 'O', 'R', 'E') { return KeyPosition::Next; }
            if str7compare(command, 'H', 'E', 'X', 'I', 'S', 'T', 'S') { return KeyPosition::Next; }
            if str7compare(command, 'H', 'G', 'E', 'T', 'A', 'L', 'L') { return KeyPosition::Next; }
            if str7compare(command, 'H', 'I', 'N', 'C', 'R', 'B', 'Y') { return KeyPosition::Next; }
            if str7compare(command, 'H', 'S', 'T', 'R', 'L', 'E', 'N') { return KeyPosition::Next; }
            if str7compare(command, 'L', 'I', 'N', 'S', 'E', 'R', 'T') { return KeyPosition::Next; }
            if str7compare(command, 'Z', 'I', 'N', 'C', 'R', 'B', 'Y') { return KeyPosition::Next; }
            if str7compare(command, 'Z', 'P', 'O', 'P', 'M', 'A', 'X') { return KeyPosition::Next; }
            if str7compare(command, 'Z', 'P', 'O', 'P', 'M', 'I', 'N') { return KeyPosition::Next; }
            if str7compare(command, 'P', 'F', 'C', 'O', 'U', 'N', 'T') { return KeyPosition::Next; }
            if str7compare(command, 'G', 'E', 'O', 'D', 'I', 'S', 'T') { return KeyPosition::Next; }
            if str7compare(command, 'G', 'E', 'O', 'H', 'A', 'S', 'H') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        8 => {
            if str8compare(command, 'E', 'X', 'P', 'I', 'R', 'E', 'A', 'T') { return KeyPosition::Next; }
            if str8compare(command, 'B', 'I', 'T', 'F', 'I', 'E', 'L', 'D') { return KeyPosition::Next; }
            if str8compare(command, 'B', 'I', 'T', 'C', 'O', 'U', 'N', 'T') { return KeyPosition::Next; }
            if str8compare(command, 'G', 'E', 'T', 'R', 'A', 'N', 'G', 'E') { return KeyPosition::Next; }
            if str8compare(command, 'S', 'E', 'T', 'R', 'A', 'N', 'G', 'E') { return KeyPosition::Next; }
            if str8compare(command, 'S', 'M', 'E', 'M', 'B', 'E', 'R', 'S') { return KeyPosition::Next; }
            if str8compare(command, 'B', 'Z', 'P', 'O', 'P', 'M', 'A', 'X') { return KeyPosition::Next; }
            if str8compare(command, 'B', 'Z', 'P', 'O', 'P', 'M', 'I', 'N') { return KeyPosition::Next; }
            if str8compare(command, 'Z', 'R', 'E', 'V', 'R', 'A', 'N', 'K') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        9 => {
            if str9compare(command, 'P', 'E', 'X', 'P', 'I', 'R', 'E', 'A', 'T') { return KeyPosition::Next; }
            if str9compare(command, 'S', 'I', 'S', 'M', 'E', 'M', 'B', 'E', 'R') { return KeyPosition::Next; }
            if str9compare(command, 'Z', 'L', 'E', 'X', 'C', 'O', 'U', 'N', 'T') { return KeyPosition::Next; }
            if str9compare(command, 'Z', 'R', 'E', 'V', 'R', 'A', 'N', 'G', 'E') { return KeyPosition::Next; }
            if str9compare(command, 'G', 'E', 'O', 'R', 'A', 'D', 'I', 'U', 'S') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        10 => {
            return KeyPosition::Unsupported;
        }
        11 => {
            if str11compare(command, 'I', 'N', 'C', 'R', 'B', 'Y', 'F', 'L', 'O', 'A', 'T') { return KeyPosition::Next; }
            if str11compare(command, 'S', 'R', 'A', 'N', 'D', 'M', 'E', 'M', 'B', 'E', 'R') { return KeyPosition::Next; }
            if str11compare(command, 'Z', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'L', 'E', 'X') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        12 => {
            if str12compare(command, 'H', 'I', 'N', 'C', 'R', 'B', 'Y', 'F', 'L', 'O', 'A', 'T') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        13 => {
            if str13compare(command, 'Z', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'S', 'C', 'O', 'R', 'E') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        14 => {
            if str14compare(command, 'Z', 'R', 'E', 'M', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'L', 'E', 'X') { return KeyPosition::Next; }
            if str14compare(command, 'Z', 'R', 'E', 'V', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'L', 'E', 'X') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        15 => {
            if str15compare(command, 'Z', 'R', 'E', 'M', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'R', 'A', 'N', 'K') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        16 => {
            if str16compare(command, 'Z', 'R', 'E', 'M', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'S', 'C', 'O', 'R', 'E') { return KeyPosition::Next; }
            if str16compare(command, 'Z', 'R', 'E', 'V', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'S', 'C', 'O', 'R', 'E') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        17 => {
            if str17compare(command, 'G', 'E', 'O', 'R', 'A', 'D', 'I', 'U', 'S', 'B', 'Y', 'M', 'E', 'M', 'B', 'E', 'R') { return KeyPosition::Next; }
            return KeyPosition::Unsupported;
        }
        _ => {
            return KeyPosition::Unsupported;
        }
    }
}

fn str3compare(byte: &[u8], first: char, second: char, third: char) -> bool {
    let first = &(first as u8);
    let second = &(second as u8);
    let third = &(third as u8);
    unsafe {
        return (byte.get_unchecked(0) == first || &(byte.get_unchecked(0) + 0x20u8) == first) &&
            (byte.get_unchecked(1) == second || &(byte.get_unchecked(1) + 0x20u8) == second) &&
            (byte.get_unchecked(2) == third || &(byte.get_unchecked(2) + 0x20u8) == third);
    }
}

fn str4compare(byte: &[u8], first: char, second: char, third: char, fourth: char) -> bool {
    let first = &(first as u8);
    let second = &(second as u8);
    let third = &(third as u8);
    let fourth = &(fourth as u8);
    unsafe {
        return (byte.get_unchecked(0) == first || &(byte.get_unchecked(0) + 0x20u8) == first) &&
            (byte.get_unchecked(1) == second || &(byte.get_unchecked(1) + 0x20u8) == second) &&
            (byte.get_unchecked(2) == third || &(byte.get_unchecked(2) + 0x20u8) == third) &&
            (byte.get_unchecked(3) == fourth || &(byte.get_unchecked(3) + 0x20u8) == fourth);
    }
}

fn str5compare(byte: &[u8], c1: char, c2: char, c3: char, c4: char, c5: char) -> bool {
    let c1 = &(c1 as u8);
    let c2 = &(c2 as u8);
    let c3 = &(c3 as u8);
    let c4 = &(c4 as u8);
    let c5 = &(c5 as u8);
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3) &&
            (byte.get_unchecked(3) == c4 || &(byte.get_unchecked(3) + 0x20u8) == c4) &&
            (byte.get_unchecked(4) == c5 || &(byte.get_unchecked(4) + 0x20u8) == c5);
    }
}

fn str6compare(byte: &[u8], c1: char, c2: char, c3: char, c4: char, c5: char, c6: char) -> bool {
    let c1 = &(c1 as u8);
    let c2 = &(c2 as u8);
    let c3 = &(c3 as u8);
    let c4 = &(c4 as u8);
    let c5 = &(c5 as u8);
    let c6 = &(c6 as u8);
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3) &&
            (byte.get_unchecked(3) == c4 || &(byte.get_unchecked(3) + 0x20u8) == c4) &&
            (byte.get_unchecked(4) == c5 || &(byte.get_unchecked(4) + 0x20u8) == c5) &&
            (byte.get_unchecked(5) == c6 || &(byte.get_unchecked(5) + 0x20u8) == c6);
    }
}

fn str7compare(byte: &[u8], c1: char, c2: char, c3: char, c4: char, c5: char, c6: char, c7: char) -> bool {
    let c1 = &(c1 as u8);
    let c2 = &(c2 as u8);
    let c3 = &(c3 as u8);
    let c4 = &(c4 as u8);
    let c5 = &(c5 as u8);
    let c6 = &(c6 as u8);
    let c7 = &(c7 as u8);
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3) &&
            (byte.get_unchecked(3) == c4 || &(byte.get_unchecked(3) + 0x20u8) == c4) &&
            (byte.get_unchecked(4) == c5 || &(byte.get_unchecked(4) + 0x20u8) == c5) &&
            (byte.get_unchecked(5) == c6 || &(byte.get_unchecked(5) + 0x20u8) == c6) &&
            (byte.get_unchecked(6) == c7 || &(byte.get_unchecked(6) + 0x20u8) == c7);
    }
}

fn str8compare(byte: &[u8], c1: char, c2: char, c3: char, c4: char, c5: char, c6: char, c7: char, c8: char) -> bool {
    let c1 = &(c1 as u8);
    let c2 = &(c2 as u8);
    let c3 = &(c3 as u8);
    let c4 = &(c4 as u8);
    let c5 = &(c5 as u8);
    let c6 = &(c6 as u8);
    let c7 = &(c7 as u8);
    let c8 = &(c8 as u8);
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3) &&
            (byte.get_unchecked(3) == c4 || &(byte.get_unchecked(3) + 0x20u8) == c4) &&
            (byte.get_unchecked(4) == c5 || &(byte.get_unchecked(4) + 0x20u8) == c5) &&
            (byte.get_unchecked(5) == c6 || &(byte.get_unchecked(5) + 0x20u8) == c6) &&
            (byte.get_unchecked(6) == c7 || &(byte.get_unchecked(6) + 0x20u8) == c7) &&
            (byte.get_unchecked(7) == c8 || &(byte.get_unchecked(7) + 0x20u8) == c8);
    }
}

fn str9compare(byte: &[u8], c1: char, c2: char, c3: char, c4: char, c5: char, c6: char, c7: char, c8: char, c9: char) -> bool {
    let c1 = &(c1 as u8);
    let c2 = &(c2 as u8);
    let c3 = &(c3 as u8);
    let c4 = &(c4 as u8);
    let c5 = &(c5 as u8);
    let c6 = &(c6 as u8);
    let c7 = &(c7 as u8);
    let c8 = &(c8 as u8);
    let c9 = &(c9 as u8);
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3) &&
            (byte.get_unchecked(3) == c4 || &(byte.get_unchecked(3) + 0x20u8) == c4) &&
            (byte.get_unchecked(4) == c5 || &(byte.get_unchecked(4) + 0x20u8) == c5) &&
            (byte.get_unchecked(5) == c6 || &(byte.get_unchecked(5) + 0x20u8) == c6) &&
            (byte.get_unchecked(6) == c7 || &(byte.get_unchecked(6) + 0x20u8) == c7) &&
            (byte.get_unchecked(7) == c8 || &(byte.get_unchecked(7) + 0x20u8) == c8) &&
            (byte.get_unchecked(8) == c9 || &(byte.get_unchecked(8) + 0x20u8) == c9);
    }
}

#[allow(dead_code)]
fn str10compare(byte: &[u8], c1: char, c2: char, c3: char, c4: char, c5: char, c6: char, c7: char, c8: char, c9: char, c10: char) -> bool {
    let c1 = &(c1 as u8);
    let c2 = &(c2 as u8);
    let c3 = &(c3 as u8);
    let c4 = &(c4 as u8);
    let c5 = &(c5 as u8);
    let c6 = &(c6 as u8);
    let c7 = &(c7 as u8);
    let c8 = &(c8 as u8);
    let c9 = &(c9 as u8);
    let c10 = &(c10 as u8);
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3) &&
            (byte.get_unchecked(3) == c4 || &(byte.get_unchecked(3) + 0x20u8) == c4) &&
            (byte.get_unchecked(4) == c5 || &(byte.get_unchecked(4) + 0x20u8) == c5) &&
            (byte.get_unchecked(5) == c6 || &(byte.get_unchecked(5) + 0x20u8) == c6) &&
            (byte.get_unchecked(6) == c7 || &(byte.get_unchecked(6) + 0x20u8) == c7) &&
            (byte.get_unchecked(7) == c8 || &(byte.get_unchecked(7) + 0x20u8) == c8) &&
            (byte.get_unchecked(8) == c9 || &(byte.get_unchecked(8) + 0x20u8) == c9) &&
            (byte.get_unchecked(9) == c10 || &(byte.get_unchecked(9) + 0x20u8) == c10);
    }
}

fn str11compare(byte: &[u8], c1: char, c2: char, c3: char, c4: char, c5: char, c6: char, c7: char, c8: char, c9: char, c10: char, c11: char) -> bool {
    let c1 = &(c1 as u8);
    let c2 = &(c2 as u8);
    let c3 = &(c3 as u8);
    let c4 = &(c4 as u8);
    let c5 = &(c5 as u8);
    let c6 = &(c6 as u8);
    let c7 = &(c7 as u8);
    let c8 = &(c8 as u8);
    let c9 = &(c9 as u8);
    let c10 = &(c10 as u8);
    let c11 = &(c11 as u8);
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3) &&
            (byte.get_unchecked(3) == c4 || &(byte.get_unchecked(3) + 0x20u8) == c4) &&
            (byte.get_unchecked(4) == c5 || &(byte.get_unchecked(4) + 0x20u8) == c5) &&
            (byte.get_unchecked(5) == c6 || &(byte.get_unchecked(5) + 0x20u8) == c6) &&
            (byte.get_unchecked(6) == c7 || &(byte.get_unchecked(6) + 0x20u8) == c7) &&
            (byte.get_unchecked(7) == c8 || &(byte.get_unchecked(7) + 0x20u8) == c8) &&
            (byte.get_unchecked(8) == c9 || &(byte.get_unchecked(8) + 0x20u8) == c9) &&
            (byte.get_unchecked(9) == c10 || &(byte.get_unchecked(9) + 0x20u8) == c10) &&
            (byte.get_unchecked(10) == c11 || &(byte.get_unchecked(10) + 0x20u8) == c11);
    }
}

fn str12compare(byte: &[u8], c1: char, c2: char, c3: char, c4: char, c5: char, c6: char, c7: char, c8: char, c9: char, c10: char, c11: char, c12: char) -> bool {
    let c1 = &(c1 as u8);
    let c2 = &(c2 as u8);
    let c3 = &(c3 as u8);
    let c4 = &(c4 as u8);
    let c5 = &(c5 as u8);
    let c6 = &(c6 as u8);
    let c7 = &(c7 as u8);
    let c8 = &(c8 as u8);
    let c9 = &(c9 as u8);
    let c10 = &(c10 as u8);
    let c11 = &(c11 as u8);
    let c12 = &(c12 as u8);
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3) &&
            (byte.get_unchecked(3) == c4 || &(byte.get_unchecked(3) + 0x20u8) == c4) &&
            (byte.get_unchecked(4) == c5 || &(byte.get_unchecked(4) + 0x20u8) == c5) &&
            (byte.get_unchecked(5) == c6 || &(byte.get_unchecked(5) + 0x20u8) == c6) &&
            (byte.get_unchecked(6) == c7 || &(byte.get_unchecked(6) + 0x20u8) == c7) &&
            (byte.get_unchecked(7) == c8 || &(byte.get_unchecked(7) + 0x20u8) == c8) &&
            (byte.get_unchecked(8) == c9 || &(byte.get_unchecked(8) + 0x20u8) == c9) &&
            (byte.get_unchecked(9) == c10 || &(byte.get_unchecked(9) + 0x20u8) == c10) &&
            (byte.get_unchecked(10) == c11 || &(byte.get_unchecked(10) + 0x20u8) == c11) &&
            (byte.get_unchecked(11) == c12 || &(byte.get_unchecked(11) + 0x20u8) == c12);
    }
}

fn str13compare(byte: &[u8], c1: char, c2: char, c3: char, c4: char, c5: char, c6: char, c7: char, c8: char, c9: char, c10: char, c11: char, c12: char, c13: char) -> bool {
    let c1 = &(c1 as u8);
    let c2 = &(c2 as u8);
    let c3 = &(c3 as u8);
    let c4 = &(c4 as u8);
    let c5 = &(c5 as u8);
    let c6 = &(c6 as u8);
    let c7 = &(c7 as u8);
    let c8 = &(c8 as u8);
    let c9 = &(c9 as u8);
    let c10 = &(c10 as u8);
    let c11 = &(c11 as u8);
    let c12 = &(c12 as u8);
    let c13 = &(c13 as u8);
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3) &&
            (byte.get_unchecked(3) == c4 || &(byte.get_unchecked(3) + 0x20u8) == c4) &&
            (byte.get_unchecked(4) == c5 || &(byte.get_unchecked(4) + 0x20u8) == c5) &&
            (byte.get_unchecked(5) == c6 || &(byte.get_unchecked(5) + 0x20u8) == c6) &&
            (byte.get_unchecked(6) == c7 || &(byte.get_unchecked(6) + 0x20u8) == c7) &&
            (byte.get_unchecked(7) == c8 || &(byte.get_unchecked(7) + 0x20u8) == c8) &&
            (byte.get_unchecked(8) == c9 || &(byte.get_unchecked(8) + 0x20u8) == c9) &&
            (byte.get_unchecked(9) == c10 || &(byte.get_unchecked(9) + 0x20u8) == c10) &&
            (byte.get_unchecked(10) == c11 || &(byte.get_unchecked(10) + 0x20u8) == c11) &&
            (byte.get_unchecked(11) == c12 || &(byte.get_unchecked(11) + 0x20u8) == c12) &&
            (byte.get_unchecked(12) == c13 || &(byte.get_unchecked(12) + 0x20u8) == c13);
    }
}

fn str14compare(byte: &[u8], c1: char, c2: char, c3: char, c4: char, c5: char, c6: char, c7: char, c8: char, c9: char, c10: char, c11: char, c12: char, c13: char, c14: char) -> bool {
    let c1 = &(c1 as u8);
    let c2 = &(c2 as u8);
    let c3 = &(c3 as u8);
    let c4 = &(c4 as u8);
    let c5 = &(c5 as u8);
    let c6 = &(c6 as u8);
    let c7 = &(c7 as u8);
    let c8 = &(c8 as u8);
    let c9 = &(c9 as u8);
    let c10 = &(c10 as u8);
    let c11 = &(c11 as u8);
    let c12 = &(c12 as u8);
    let c13 = &(c13 as u8);
    let c14 = &(c14 as u8);
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3) &&
            (byte.get_unchecked(3) == c4 || &(byte.get_unchecked(3) + 0x20u8) == c4) &&
            (byte.get_unchecked(4) == c5 || &(byte.get_unchecked(4) + 0x20u8) == c5) &&
            (byte.get_unchecked(5) == c6 || &(byte.get_unchecked(5) + 0x20u8) == c6) &&
            (byte.get_unchecked(6) == c7 || &(byte.get_unchecked(6) + 0x20u8) == c7) &&
            (byte.get_unchecked(7) == c8 || &(byte.get_unchecked(7) + 0x20u8) == c8) &&
            (byte.get_unchecked(8) == c9 || &(byte.get_unchecked(8) + 0x20u8) == c9) &&
            (byte.get_unchecked(9) == c10 || &(byte.get_unchecked(9) + 0x20u8) == c10) &&
            (byte.get_unchecked(10) == c11 || &(byte.get_unchecked(10) + 0x20u8) == c11) &&
            (byte.get_unchecked(11) == c12 || &(byte.get_unchecked(11) + 0x20u8) == c12) &&
            (byte.get_unchecked(12) == c13 || &(byte.get_unchecked(12) + 0x20u8) == c13) &&
            (byte.get_unchecked(13) == c14 || &(byte.get_unchecked(13) + 0x20u8) == c14);
    }
}

fn str15compare(byte: &[u8], c1: char, c2: char, c3: char, c4: char, c5: char, c6: char, c7: char, c8: char, c9: char, c10: char, c11: char, c12: char, c13: char, c14: char, c15: char) -> bool {
    let c1 = &(c1 as u8);
    let c2 = &(c2 as u8);
    let c3 = &(c3 as u8);
    let c4 = &(c4 as u8);
    let c5 = &(c5 as u8);
    let c6 = &(c6 as u8);
    let c7 = &(c7 as u8);
    let c8 = &(c8 as u8);
    let c9 = &(c9 as u8);
    let c10 = &(c10 as u8);
    let c11 = &(c11 as u8);
    let c12 = &(c12 as u8);
    let c13 = &(c13 as u8);
    let c14 = &(c14 as u8);
    let c15 = &(c15 as u8);
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3) &&
            (byte.get_unchecked(3) == c4 || &(byte.get_unchecked(3) + 0x20u8) == c4) &&
            (byte.get_unchecked(4) == c5 || &(byte.get_unchecked(4) + 0x20u8) == c5) &&
            (byte.get_unchecked(5) == c6 || &(byte.get_unchecked(5) + 0x20u8) == c6) &&
            (byte.get_unchecked(6) == c7 || &(byte.get_unchecked(6) + 0x20u8) == c7) &&
            (byte.get_unchecked(7) == c8 || &(byte.get_unchecked(7) + 0x20u8) == c8) &&
            (byte.get_unchecked(8) == c9 || &(byte.get_unchecked(8) + 0x20u8) == c9) &&
            (byte.get_unchecked(9) == c10 || &(byte.get_unchecked(9) + 0x20u8) == c10) &&
            (byte.get_unchecked(10) == c11 || &(byte.get_unchecked(10) + 0x20u8) == c11) &&
            (byte.get_unchecked(11) == c12 || &(byte.get_unchecked(11) + 0x20u8) == c12) &&
            (byte.get_unchecked(12) == c13 || &(byte.get_unchecked(12) + 0x20u8) == c13) &&
            (byte.get_unchecked(13) == c14 || &(byte.get_unchecked(13) + 0x20u8) == c14) &&
            (byte.get_unchecked(14) == c15 || &(byte.get_unchecked(14) + 0x20u8) == c15);
    }
}

fn str16compare(byte: &[u8], c1: char, c2: char, c3: char, c4: char, c5: char, c6: char, c7: char, c8: char, c9: char, c10: char, c11: char, c12: char, c13: char, c14: char, c15: char, c16: char) -> bool {
    let c1 = &(c1 as u8);
    let c2 = &(c2 as u8);
    let c3 = &(c3 as u8);
    let c4 = &(c4 as u8);
    let c5 = &(c5 as u8);
    let c6 = &(c6 as u8);
    let c7 = &(c7 as u8);
    let c8 = &(c8 as u8);
    let c9 = &(c9 as u8);
    let c10 = &(c10 as u8);
    let c11 = &(c11 as u8);
    let c12 = &(c12 as u8);
    let c13 = &(c13 as u8);
    let c14 = &(c14 as u8);
    let c15 = &(c15 as u8);
    let c16 = &(c16 as u8);
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3) &&
            (byte.get_unchecked(3) == c4 || &(byte.get_unchecked(3) + 0x20u8) == c4) &&
            (byte.get_unchecked(4) == c5 || &(byte.get_unchecked(4) + 0x20u8) == c5) &&
            (byte.get_unchecked(5) == c6 || &(byte.get_unchecked(5) + 0x20u8) == c6) &&
            (byte.get_unchecked(6) == c7 || &(byte.get_unchecked(6) + 0x20u8) == c7) &&
            (byte.get_unchecked(7) == c8 || &(byte.get_unchecked(7) + 0x20u8) == c8) &&
            (byte.get_unchecked(8) == c9 || &(byte.get_unchecked(8) + 0x20u8) == c9) &&
            (byte.get_unchecked(9) == c10 || &(byte.get_unchecked(9) + 0x20u8) == c10) &&
            (byte.get_unchecked(10) == c11 || &(byte.get_unchecked(10) + 0x20u8) == c11) &&
            (byte.get_unchecked(11) == c12 || &(byte.get_unchecked(11) + 0x20u8) == c12) &&
            (byte.get_unchecked(12) == c13 || &(byte.get_unchecked(12) + 0x20u8) == c13) &&
            (byte.get_unchecked(13) == c14 || &(byte.get_unchecked(13) + 0x20u8) == c14) &&
            (byte.get_unchecked(14) == c15 || &(byte.get_unchecked(14) + 0x20u8) == c15) &&
            (byte.get_unchecked(15) == c16 || &(byte.get_unchecked(15) + 0x20u8) == c16);
    }
}

fn str17compare(byte: &[u8], c1: char, c2: char, c3: char, c4: char, c5: char, c6: char, c7: char, c8: char, c9: char, c10: char, c11: char, c12: char, c13: char, c14: char, c15: char, c16: char, c17: char) -> bool {
    let c1 = &(c1 as u8);
    let c2 = &(c2 as u8);
    let c3 = &(c3 as u8);
    let c4 = &(c4 as u8);
    let c5 = &(c5 as u8);
    let c6 = &(c6 as u8);
    let c7 = &(c7 as u8);
    let c8 = &(c8 as u8);
    let c9 = &(c9 as u8);
    let c10 = &(c10 as u8);
    let c11 = &(c11 as u8);
    let c12 = &(c12 as u8);
    let c13 = &(c13 as u8);
    let c14 = &(c14 as u8);
    let c15 = &(c15 as u8);
    let c16 = &(c16 as u8);
    let c17 = &(c17 as u8);
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3) &&
            (byte.get_unchecked(3) == c4 || &(byte.get_unchecked(3) + 0x20u8) == c4) &&
            (byte.get_unchecked(4) == c5 || &(byte.get_unchecked(4) + 0x20u8) == c5) &&
            (byte.get_unchecked(5) == c6 || &(byte.get_unchecked(5) + 0x20u8) == c6) &&
            (byte.get_unchecked(6) == c7 || &(byte.get_unchecked(6) + 0x20u8) == c7) &&
            (byte.get_unchecked(7) == c8 || &(byte.get_unchecked(7) + 0x20u8) == c8) &&
            (byte.get_unchecked(8) == c9 || &(byte.get_unchecked(8) + 0x20u8) == c9) &&
            (byte.get_unchecked(9) == c10 || &(byte.get_unchecked(9) + 0x20u8) == c10) &&
            (byte.get_unchecked(10) == c11 || &(byte.get_unchecked(10) + 0x20u8) == c11) &&
            (byte.get_unchecked(11) == c12 || &(byte.get_unchecked(11) + 0x20u8) == c12) &&
            (byte.get_unchecked(12) == c13 || &(byte.get_unchecked(12) + 0x20u8) == c13) &&
            (byte.get_unchecked(13) == c14 || &(byte.get_unchecked(13) + 0x20u8) == c14) &&
            (byte.get_unchecked(14) == c15 || &(byte.get_unchecked(14) + 0x20u8) == c15) &&
            (byte.get_unchecked(15) == c16 || &(byte.get_unchecked(15) + 0x20u8) == c16) &&
            (byte.get_unchecked(16) == c17 || &(byte.get_unchecked(16) + 0x20u8) == c17);
    }
}

pub fn handle_slotsmap(
    response: &[u8],
    handle_slots: &mut FnMut(String, usize, usize) -> Result<(), RedisError>,
) -> Result<(), RedisError> {
    if response.len() == 0 {
        return Ok(());
    }
    /*let mut copy = response.clone();
    copy = str::replace(copy.as_str(), "\r", "\\r");
    copy = str::replace(copy.as_str(), "\n", "\\n\n");
    debug!("Handling slots map:\n{}", copy);*/
    // Populate the slots map.

    //let mut chars = response.iter();
    let mut index = 0;
    let mut current_char = response.get(index).unwrap();
    index += 1;
    if *current_char != '*' as u8 {
        error!("Parse error: expected * at start of response. Found {:?} instead.", *current_char as char);
        return Err(RedisError::InvalidProtocol);
    }
    let starting_index = try!(interpret_num(response, &mut index));
    index += 2;

    for _ in 0..starting_index {
        current_char = response.get(index).unwrap();
        index += 1;
        if *current_char != '*' as u8 {
            error!("Parse error: expected * at start of response. Found {:?} instead.", *current_char as char);
            return Err(RedisError::InvalidProtocol);
        }
        //let parsed_resp_length = parse_int(&mut chars, response);
        let parsed_resp_length = try!(interpret_num(response, &mut index));
        index += 2;
        if parsed_resp_length < 3 {
            error!("Parse error: expected at least 3 lines for each response. Parsed {} instead.", parsed_resp_length);
            return Err(RedisError::InvalidProtocol);
        }
        // First two lines arer for slot range.
        // Next ones are for master and replicas.
    
        let mut hostname = "".to_string();
        let mut identifier = "".to_string();

        // Parse starting slot range.
        current_char = response.get(index).unwrap();
        index += 1;
        if *current_char != ':' as u8 {
            error!("Parse error: expected : at start of line to mark second level of array. Found {:?} instead.", current_char);
            return Err(RedisError::InvalidProtocol);
        }
        //let starting_slot = parse_int(&mut chars, response);
        let starting_slot = try!(interpret_num(response, &mut index));
        index += 2;
        if starting_slot < 0 {
            return Err(RedisError::InvalidProtocol);
        }

        // Parse ending slot range.
        if *response.get(index).unwrap() != ':' as u8 {
            error!("parse error: expected :");
            return Err(RedisError::InvalidProtocol);
        }
        index += 1;
        //let ending_slot = parse_int(&mut chars, response);
        let ending_slot = try!(interpret_num(response, &mut index));
        index += 2;
        if ending_slot < 0 {
            return Err(RedisError::InvalidProtocol);
        }

        for _ in 0..parsed_resp_length-2 {
            current_char = response.get(index).unwrap();
            index += 1;
            if *current_char != '*' as u8 {
                error!("Parse error: expected * at start of response. Found {:?} instead.", current_char);
                return Err(RedisError::InvalidProtocol);
            }
            let parsed_slot_array_length = try!(interpret_num(response, &mut index));
            index += 2;
            // Can be 2 for older redis versions, and 3 for newer redis versions.

            current_char = response.get(index).unwrap();
            index += 1;
            if *current_char != '$' as u8 {
                error!("Parse error: 1st expected $ at start of line to mark second level of array. Found {:?} instead.", current_char);
                return Err(RedisError::InvalidProtocol);
            }
            let parsed_string_length = try!(interpret_num(response, &mut index));
            index += 2;
            for _ in 0..parsed_string_length {
                hostname.push(*response.get(index).unwrap() as char);
                index += 1;
            }
            try!(expect_eol(response, &mut index));

            current_char = response.get(index).unwrap();
            index += 1;
            if *current_char != ':' as u8 {
                error!("Parse error: expected : at start of line to mark second level of array. Found {:?} instead.", current_char);
                return Err(RedisError::InvalidProtocol);
            }
            let port = try!(interpret_num(response, &mut index));
            index += 2;

            if parsed_slot_array_length > 2 {
                current_char = response.get(index).unwrap();
                index += 1;
                if *current_char != '$' as u8 {
                    error!("Parse error: 2nd expected $ at start of line to mark second level of array. Found {:?} instead.", current_char);
                    return Err(RedisError::InvalidProtocol);
                }
                let parsed_string_length = try!(interpret_num(response, &mut index));
                index += 2;
                for _ in 0..parsed_string_length {
                    identifier.push(*response.get(index).unwrap() as char);
                    index += 1;
                }
                index += 2;
            }

            // TODO. only do this for first one.
            let host = format!("{}:{}", hostname, port);
            try!(handle_slots(host, starting_slot as usize, ending_slot as usize));
        }
    }
    return Ok(());
}

fn expect_eol(bytes: &[u8], index: &mut usize) -> Result<(), RedisError> {
    debug!("Expecitng eol: {}", index);
    let mut next = bytes.get(*index).unwrap();
    *index += 1;
    if *next != '\r' as u8 {
        error!("Parse error: expected \\r, found {:?} instead.", *next as char);
        return Err(RedisError::InvalidProtocol);
    }
    next = bytes.get(*index).unwrap();
    *index += 1;
    if *next != '\n' as u8 {
        error!("Parse error: expected \\n, found {:?} instead.", next);
        return Err(RedisError::InvalidProtocol);
    }
    return Ok(());
}

#[test]
fn test_slotsmap() {
    init_logging();
    let r = "*3\r\n*3\r\n:10922\r\n:16382\r\n*2\r\n$9\r\n127.0.0.1\r\n:7002\r\n*3\r\n:1\r\n:5460\r\n*2\r\n$9\r\n127.0.0.1\r\n:7000\r\n*3\r\n:5461\r\n:10921\r\n*2\r\n$9\r\n127.0.0.1\r\n:7001\r\n";
    let mut assigned_slots : Vec<Host>  = vec!["".to_owned(); 16384];
    {
    let mut count_slots = |host:String, start: usize, end: usize| -> Result<(), RedisError> {
        for i in start..end+1 {
            assigned_slots.remove(i-1);
            assigned_slots.insert(i-1, host.clone());
        }
        return Ok(());
    };
    handle_slotsmap(r.as_bytes(), &mut count_slots).unwrap();
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

    let r = "*3\r\n*3\r\n:10922\r\n:16382\r\n*3\r\n$9\r\n127.0.0.1\r\n:7002\r\n$40\r\nd0380b35d40bd7f271accef4a3e51d9514c9c645\r\n*3\r\n:1\r\n:5460\r\n*3\r\n$9\r\n127.0.0.1\r\n:7000\r\n$40\r\nef87505cb77d00e9f7886cfecc81413418e95bfd\r\n*3\r\n:5461\r\n:10921\r\n*3\r\n$9\r\n127.0.0.1\r\n:7001\r\n$40\r\nb6caef27795d29d068989e38fec89bc92158930d\r\n";
        let mut assigned_slots : Vec<Host>  = vec!["".to_owned(); 16384];
    {
    let mut count_slots = |host:String, start: usize, end: usize| -> Result<(), RedisError> {
        for i in start..end+1 {
            assigned_slots.remove(i-1);
            assigned_slots.insert(i-1, host.clone());
        }
        return Ok(());
    };
    handle_slotsmap(r.as_bytes(), &mut count_slots).unwrap();
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
