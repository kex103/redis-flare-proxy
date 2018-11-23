use memchr::memchr;
use std::slice::Iter;
use std::result::Result;
use std;

#[cfg(test)]
use std::time::Instant;
#[cfg(test)]
use cluster_backend::{init_logging, init_logging_info};

#[derive(Debug, PartialEq)]
pub enum RedisError {
    NoBackend,
    Unknown,
    UnsupportedCommand,
    InvalidScript,
    InvalidProtocol,
}

enum KeyPosition {
    None,
    Next,
    Unsupported,
    Eval,
}

#[test]
fn test_parsing_redis() {
    init_logging();
    let a = "*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n".to_string();
    let resp = extract_key(&a.as_bytes());
    assert_eq!(resp, Ok("key1".as_bytes()));
    let a = "*5\r\n$4\r\nEVAL\r\n$40\r\nreturn redis.call('set',KEYS[1],ARGV[1])\r\n$1\r\n1\r\n$5\r\nkey10\r\n$7\r\nvalue10".to_string();
    let resp = extract_key(&a.as_bytes());
    assert_eq!(resp, Ok("key10".as_bytes()));


    let a = "*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n".to_string();
    let resp = extract_key2(&a.as_bytes());
    assert_eq!(resp, Ok("key1".as_bytes()));
    let a = "*5\r\n$4\r\nEVAL\r\n$40\r\nreturn redis.call('set',KEYS[1],ARGV[1])\r\n$1\r\n1\r\n$5\r\nkey10\r\n$7\r\nvalue10".to_string();
    let resp = extract_key2(&a.as_bytes());
    assert_eq!(resp, Ok("key10".as_bytes()));
}

#[test]
fn test_speed() {
    let num_runs = 100000;
    init_logging_info();
    let a = format!("*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n{:?}", time::now());
    let b = a.as_bytes();
    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = iteration2(b);
    }
    info!("Time spent with iteration2: {:?}", Instant::now() - start);
    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = iteration1(b);
    }
    info!("Time spent with iteration1: {:?}", Instant::now() - start);
    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = iteration2(b);
    }
    info!("Time spent with iteration2: {:?}", Instant::now() - start);
    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = iteration1(b);
    }
    info!("Time spent with iteration1 again: {:?}", Instant::now() - start);
    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = iteration2(b);
    }
    info!("Time spent with iteration2 again: {:?}", Instant::now() - start);
    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = iteration1(b);
    }
    info!("Time spent with iteration1 again: {:?}", Instant::now() - start);
    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = iteration2(b);
    }
    info!("Time spent with iteration2 again: {:?}", Instant::now() - start);
}

pub fn iteration1(bytes: &[u8]) -> Result<usize, RedisError> {
    let mut index = 0;
    let mut bytes_iter = bytes.iter();

    // skip 1
    /*
    let mut b = match bytes_iter.next() {
        Some(byte) => *byte,
        None => { return Err(RedisError::Unknown); }
    };
    while b != '\n' as u8 {
        index += 1;
        b = match bytes_iter.next() {
            Some(byte) => *byte,
            None => { return Err(RedisError::Unknown); }
        };
    }
    index += 1;*/

    try!(skip_past_eol(&mut bytes_iter, &mut index));
    index += 1;
    Ok(index)
}

pub fn iteration2(bytes: &[u8]) -> Result<usize, RedisError> {
    let mut index = 0;
    let mut bytes_iter = bytes.iter();

    // skip 1
    let mut b = match bytes_iter.next() {
        Some(byte) => *byte,
        None => { return Err(RedisError::Unknown); }
    };
    while b != '\n' as u8 {
        index += 1;
        b = match bytes_iter.next() {
            Some(byte) => *byte,
            None => { return Err(RedisError::Unknown); }
        };
    }
    index += 1;
    Ok(index)
}

fn skip_past_eol(iter: &mut Iter<u8>, index: &mut usize) -> Result<(), RedisError> {
    let mut b = match iter.next() {
        Some(byte) => *byte,
        None => { return Err(RedisError::Unknown); }
    };
    while b != '\n' as u8 {
        *index += 1;
        b = match iter.next() {
            Some(byte) => *byte,
            None => { return Err(RedisError::Unknown); }
        };
    }
    // return number of steps forward taken.
    return Ok(());
}

fn skip_past_eol2(bytes: &[u8], index: &mut usize) -> Result<(), RedisError> {
    match memchr('\n' as u8, bytes) {
        Some(delta) => {
            *index += delta + 1;
            return Ok(());
        }
        None => {
            return Err(RedisError::Unknown);
        }
    }
}

fn skip_bytes(num_bytes: usize, iter: &mut Iter<u8>, index: &mut usize) -> Result<u8, RedisError> {
    *index += num_bytes + 1;
    let c = match iter.nth(num_bytes) {
        Some(c) => *c,
        None => { return Err(RedisError::InvalidProtocol); }
    };
    return Ok(c);
}

fn next_byte(iter: &mut Iter<u8>, index: &mut usize) -> Result<u8, RedisError> {
    let b = match iter.next() {
        Some(b) => *b,
        None => { return Err(RedisError::InvalidProtocol); }
    };
    *index += 1;
    return Ok(b);
}

fn parse_num_bytes(first_byte: u8, iter: &mut Iter<u8>, index: &mut usize) -> Result<isize, RedisError> {
    let b = first_byte;
    if b != '$' as u8 {
        //debug!("Expected a - but got {:?} instead", b as char);
        return Err(RedisError::InvalidProtocol);
    }

    return interpret_num(iter, index);
}

fn parse_num(iter: &mut Iter<u8>, index: &mut usize, bytes: &[u8]) -> Result<isize, RedisError> {
    // parse the integer.
    let start_index = *index;
    let mut b = try!(next_byte(iter, index));
    while b != '\n' as u8 {
        *index += 1;
        b = match iter.next() {
            Some(byte) => *byte,
            None => { return Err(RedisError::Unknown); }
        };
    }
    let raw_num = unsafe {
        bytes.get_unchecked(start_index..*index-2)
    };
    let num = match std::str::from_utf8(raw_num) {
        Ok(n) => {
            match n.parse::<isize>() {
                Ok(n) => n,
                Err(_err) => { return Err(RedisError::InvalidProtocol); }
            }
        }
        Err(_error) => { return Err(RedisError::InvalidProtocol); }
    };
    return Ok(num);
}

fn interpret_num(bytes_iter: &mut Iter<u8>, index: &mut usize) -> Result<isize, RedisError> {
    let mut negative = false;
    let mut result = 0;
    loop {
        *index += 1;

        match *bytes_iter.next().unwrap() as char {
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
                bytes_iter.next();
                *index += 1;
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
    }
}

fn interpret_num2(bytes: &[u8], index: &mut usize) -> Result<isize, RedisError> {
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



    let a = "$-1\r\naeras".to_string();
    let resp = extract_redis_command2(&a.as_bytes());
    assert_eq!(resp, Ok("$-1\r\n".as_bytes()));

    let a = "+THREE\r\naeras".to_string();
    let resp = extract_redis_command2(&a.as_bytes());
    assert_eq!(resp, Ok("+THREE\r\n".as_bytes()));

    let a = "-FERAC\r\ndera".to_string();
    let resp = extract_redis_command2(&a.as_bytes());
    assert_eq!(resp, Ok("-FERAC\r\n".as_bytes()));

    let a = ":1234567\r\ndera".to_string();
    let resp = extract_redis_command2(&a.as_bytes());
    assert_eq!(resp, Ok(":1234567\r\n".as_bytes()));

    let a = "$4\r\nTHRE\r\ndera".to_string();
    let resp = extract_redis_command2(&a.as_bytes());
    assert_eq!(resp, Ok("$4\r\nTHRE\r\n".as_bytes()));

    let a = "*3\r\n+dera\r\n$2\r\nab\r\n*2\r\n$4\r\nBLAR\r\n:34\r\nadaerare".to_string();
    let resp = extract_redis_command2(&a.as_bytes());
    assert_eq!(resp, Ok("*3\r\n+dera\r\n$2\r\nab\r\n*2\r\n$4\r\nBLAR\r\n:34\r\n".as_bytes()));
}

pub fn extract_redis_command2(bytes: &[u8]) -> Result<&[u8], RedisError> {
    let mut index = 0;
    try!(parse_redis_request2(bytes, &mut index));
    return unsafe { Ok(bytes.get_unchecked(0..index)) };
}
fn parse_redis_request2(bytes: &[u8], index: &mut usize) -> Result<(), RedisError> {
    match *bytes.get(*index).unwrap() as char {
        '+' =>  {
            *index += 1;
            let bytes2 = unsafe { bytes.get_unchecked(*index..) };
            try!(skip_past_eol2(bytes2, index));
            return Ok(());
        }
        '-' =>  {
            *index += 1;
            let bytes2 = unsafe { bytes.get_unchecked(*index..) };
            try!(skip_past_eol2(bytes2, index));
            return Ok(());
        }
        ':' =>  {
            *index += 1;
            let bytes2 = unsafe { bytes.get_unchecked(*index..) };
            try!(skip_past_eol2(bytes2, index));
            return Ok(());
        }
        '$' => {
            *index += 1;
            let num = try!(interpret_num2(bytes, index));
            *index += 2;
            if num < 0 {
                return Ok(());
            }
            *index += num as usize + 2;
            return Ok(());
        }
        '*' => {
            *index += 1;
            let num = try!(interpret_num2(bytes, index));
            *index += 2;
            for _ in 0..num {
                try!(parse_redis_request2(bytes, index));
            }
            return Ok(());
            
        }
        _ => { return Err(RedisError::InvalidProtocol); }
    }
}


pub fn extract_redis_command(bytes: &[u8]) -> Result<&[u8], RedisError> {
    let mut bytes_iter = bytes.iter();
    let mut index = 0;
    try!(parse_redis_request(&mut bytes_iter, &mut index, bytes));
    return unsafe { Ok(bytes.get_unchecked(0..index)) };
    /*
    match *bytes_iter.next().unwrap() as char {
        '+' =>  {
            try!(skip_past_eol(&mut bytes_iter, &mut index));
            return unsafe {
                Ok(bytes.get_unchecked(0..index+1))
            }
        }
        '-' =>  {
            try!(skip_past_eol(&mut bytes_iter, &mut index));
            return unsafe {
                Ok(bytes.get_unchecked(0..index+1))
            };
        }
        ':' =>  {
            try!(skip_past_eol(&mut bytes_iter, &mut index));
            return unsafe {
                Ok(bytes.get_unchecked(0..index+1))
            }
        }
        '$' => {
            let num = try!(parse_num(&mut bytes_iter, &mut index, bytes));
            return unsafe {
                Ok(bytes.get_unchecked(0..index+num+2))
            };
        }
        '*' => {
            let num = try!(parse_num(&mut bytes_iter, &mut index, bytes));
            for _ in 0..num {
            }
            panic!("not done");

        }
        _ => { return Err(RedisError::InvalidProtocol); }
    }*/
}
fn parse_redis_request(bytes_iter: &mut Iter<u8>, index: &mut usize, bytes: &[u8]) -> Result<(), RedisError> {
    *index += 1;
    match *bytes_iter.next().unwrap() as char {
        '+' =>  {
            try!(skip_past_eol(bytes_iter, index));
            *index += 1;
            return Ok(());
        }
        '-' =>  {
            try!(skip_past_eol(bytes_iter, index));
            *index += 1;
            return Ok(());
        }
        ':' =>  {
            try!(skip_past_eol(bytes_iter, index));
            *index += 1;
            return Ok(());
        }
        '$' => {
            let num = try!(interpret_num(bytes_iter, index));
            if num < 0 {
                return Ok(());
            }
            try!(skip_bytes(num as usize + 1, bytes_iter, index));
            return Ok(());
        }
        '*' => {
            let num = try!(interpret_num(bytes_iter, index));
            for _ in 0..num {
                try!(parse_redis_request(bytes_iter, index, bytes));
            }
            return Ok(());
            
        }
        _ => { return Err(RedisError::InvalidProtocol); }
    }
}

pub fn extract_key2(bytes: &[u8]) -> Result<&[u8], RedisError> {
    if bytes[0] == '*' as u8 {
        // then it is standard redis protcol.
        let mut index = 0;
        let mut bytes_iter = bytes.iter();

        // skip 1
        try!(skip_past_eol(&mut bytes_iter, &mut index));
        let byte = try!(next_byte(&mut bytes_iter, &mut index));
        let num = try!(parse_num_bytes(byte, &mut bytes_iter, &mut index)) as usize;

        // grab the command list.
        let command = unsafe {
            bytes.get_unchecked(index+1..index+num+1)
        };
        //debug!("Parsed command: {:?}", std::str::from_utf8(command));

        match supported_keys(command) {
            KeyPosition::Unsupported => { return Err(RedisError::UnsupportedCommand); }
            KeyPosition::None => { return Err(RedisError::Unknown); }
            KeyPosition::Next => {
                let c = try!(skip_bytes(num+ 2, &mut bytes_iter, &mut index));
                let num = try!(parse_num_bytes(c, &mut bytes_iter, &mut index)) as usize;

                // TODO: Account fro num being -1.

                // grab the command list.
                let key = unsafe {
                    bytes.get_unchecked(index+1..index+num+1)
                };
                return Ok(key);
            }
            KeyPosition::Eval => {
                let c = try!(skip_bytes(num+ 2, &mut bytes_iter, &mut index));
                let num = try!(parse_num_bytes(c, &mut bytes_iter, &mut index)) as usize;
                
                let c = try!(skip_bytes(num+ 2, &mut bytes_iter, &mut index));
                let num = try!(parse_num_bytes(c, &mut bytes_iter, &mut index)) as usize;

                let num_keys = unsafe {
                    bytes.get_unchecked(index+1..index+num+1)
                };
                if num_keys != ['1' as u8] {
                    return Err(RedisError::InvalidScript);
                }

                let c = try!(skip_bytes(num+ 2, &mut bytes_iter, &mut index));
                let num = try!(parse_num_bytes(c, &mut bytes_iter, &mut index)) as usize;

                let key = unsafe {
                    bytes.get_unchecked(index+1..index+num+1)
                };
                return Ok(key);
            }
        };
    } else {
        panic!("Unimplemented support for plain text commands");
    }
}

pub fn extract_key(bytes: &[u8]) -> Result<&[u8], RedisError> {
    if bytes[0] == '*' as u8 {
        // then it is standard redis protcol.
        let mut index = 0;

        // skip 1
        try!(skip_past_eol2(&bytes, &mut index));

        // verify next byte is '$'
        if '$' as u8 != unsafe { *bytes.get_unchecked(index) } {
            return Err(RedisError::InvalidProtocol);
        }
        index += 1;
        let num = try!(interpret_num2(bytes, &mut index)) as usize;
        index += 2;

        // grab the command list.
        let command = unsafe {
            bytes.get_unchecked(index..index+num)
        };

        match supported_keys(command) {
            KeyPosition::Unsupported => { return Err(RedisError::UnsupportedCommand); }
            KeyPosition::None => { return Err(RedisError::Unknown); }
            KeyPosition::Next => {
                index += num + 2;

                if '$' as u8 != unsafe { *bytes.get_unchecked(index) } {
                    return Err(RedisError::InvalidProtocol);
                }
                index += 1;
                let num = try!(interpret_num2(bytes, &mut index)) as usize;
                index += 2;
                // TODO: Account fro num being -1.

                // grab the command list.
                let key = unsafe {
                    bytes.get_unchecked(index..index+num)
                };
                return Ok(key);
            }
            KeyPosition::Eval => {
                index += num + 2;
                if '$' as u8 != unsafe { *bytes.get_unchecked(index) } {
                    return Err(RedisError::InvalidProtocol);
                }
                index += 1;
                let num = try!(interpret_num2(bytes, &mut index)) as usize;
                index += 2;
                
                index += num + 2;
                if '$' as u8 != unsafe { *bytes.get_unchecked(index) } {
                    return Err(RedisError::InvalidProtocol);
                }
                index += 1;
                let num = try!(interpret_num2(bytes, &mut index)) as usize;
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
                let num = try!(interpret_num2(bytes, &mut index)) as usize;
                index += 2;

                let key = unsafe {
                    bytes.get_unchecked(index..index+num)
                };
                return Ok(key);
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
            if str4compare(command, 'E', 'V', 'A', 'L') { return KeyPosition::Eval; }
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

    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = extract_key2(b);
    }
    info!("Time spent with extract_key2: {:?}", Instant::now() - start);

    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = extract_key(b);
    }
    info!("Time spent with extract_key: {:?}", Instant::now() - start);

    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = extract_key2(b);
    }
    info!("Time spent with extract_key2: {:?}", Instant::now() - start);
}
