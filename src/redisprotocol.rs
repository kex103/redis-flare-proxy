use std::result::Result;
use std::error::Error;
use std::fmt;
use std;

#[cfg(test)]
use std::time::Instant;
#[cfg(test)]
use cluster_backend::init_logging_info;

#[derive(Debug)]
pub struct RedisProtocolError {}
impl fmt::Display for RedisProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RedisProtocolError is here!")
    }
}
impl Error for RedisProtocolError {
    fn description(&self) -> &str {
        "Failed to parse."
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

pub fn extract_key3(command: &String) -> Result<&str, RedisProtocolError> {
    let mut seen_space = 0;
    let mut parsed_command = false;

    let bytes = command.as_bytes();
    let mut first_index = 0;
    let mut index = 0;
    for &b in bytes {
        debug!("{:?}", b);
        // space U+0020
        // line ending U+0010
        if b.eq(&10u8) {
            if seen_space < 1 {
                seen_space += 1;
            } else if first_index == 0 {
                debug!("Starting to record");
                first_index = index + 1;
            } else if parsed_command == false {
                let first_word2 = unsafe {
                    bytes.get_unchecked(first_index..index-1)
                };
                debug!("First word: {:?}", first_word2);
                if supported_keys3(first_word2) {
                    seen_space = 1;
                    first_index = 0;
                    parsed_command = true;
                } else {
                    return Err(RedisProtocolError {});
                }
            } else if parsed_command == true {
                let body = unsafe {
                    std::str::from_utf8_unchecked(bytes.get_unchecked(first_index..index-1))
                };
                debug!("Key: {:?}", body);
                return Ok(body);
            }
        }
        index += 1;
    }

    return Err(RedisProtocolError {});
}

pub fn extract_key2(command: &String) -> Result<&str, RedisProtocolError> {
    let mut seen_space = 0;
    let mut parsed_command = false;

    let bytes = command.as_bytes();
    let mut first_index = 0;
    let mut index = 0;
    for &b in bytes {
        debug!("{:?}", b);
        // space U+0020
        // line ending U+0010
        if b.eq(&10u8) {
            if seen_space < 1 {
                seen_space += 1;
            } else if first_index == 0 {
                debug!("Starting to record");
                first_index = index + 1;
            } else if parsed_command == false {
                let first_word2 = unsafe {
                    bytes.get_unchecked(first_index..index-1)
                };
                debug!("First word: {:?}", first_word2);
                if supported_keys3(first_word2) {
                    seen_space = 1;
                    first_index = 0;
                    parsed_command = true;
                } else {
                    return Err(RedisProtocolError {});
                }
            } else if parsed_command == true {
                let body = unsafe {
                    std::str::from_utf8_unchecked(bytes.get_unchecked(first_index..index-1))
                };
                debug!("Key: {:?}", body);
                return Ok(body);
            }
        }
        index += 1;
    }

    return Err(RedisProtocolError {});
}

fn supported_keys3(command: &[u8]) -> bool {
    match command.len() {
        3 => {
            if str3compare2(command, &0x67u8, &0x65u8, &0x74u8) { return true; }
            if str3compare(command, 'G', 'E', 'T') { return true; }
            if str3compare(command, 'D', 'E', 'L') { return true; }
            if str3compare(command, 'S', 'E', 'T') { return true; }
            if str3compare(command, 'T', 'T', 'L') { return true; }
            return false;
        }
        4 => {
            if str4compare(command, 'D', 'U', 'M', 'P') { return true; }
            if str4compare(command, 'P', 'T', 'T', 'L') { return true; }
            if str4compare(command, 'S', 'O', 'R', 'T') { return true; }
            if str4compare(command, 'T', 'Y', 'P', 'E') { return true; }
            if str4compare(command, 'D', 'E', 'C', 'R') { return true; }
            if str4compare(command, 'I', 'N', 'C', 'R') { return true; }
            if str4compare(command, 'H', 'D', 'E', 'L') { return true; }
            if str4compare(command, 'H', 'G', 'E', 'T') { return true; }
            if str4compare(command, 'H', 'L', 'E', 'N') { return true; }
            if str4compare(command, 'H', 'S', 'E', 'T') { return true; }
            if str4compare(command, 'L', 'L', 'E', 'N') { return true; }
            if str4compare(command, 'L', 'P', 'O', 'P') { return true; }
            if str4compare(command, 'L', 'R', 'E', 'M') { return true; }
            if str4compare(command, 'L', 'S', 'E', 'T') { return true; }
            if str4compare(command, 'R', 'P', 'O', 'P') { return true; }
            if str4compare(command, 'S', 'A', 'D', 'D') { return true; }
            if str4compare(command, 'S', 'P', 'O', 'P') { return true; }
            if str4compare(command, 'S', 'R', 'E', 'M') { return true; }
            if str4compare(command, 'Z', 'A', 'D', 'D') { return true; }
            if str4compare(command, 'Z', 'R', 'E', 'M') { return true; }
            return false;
        }
        5 => {
            if str5compare(command, 'T', 'O', 'U', 'C', 'H') { return true; }
            if str5compare(command, 'S', 'E', 'T', 'E', 'X') { return true; }
            if str5compare(command, 'S', 'E', 'T', 'N', 'X') { return true; }
            if str5compare(command, 'H', 'K', 'E', 'Y', 'S') { return true; }
            if str5compare(command, 'H', 'M', 'G', 'E', 'T') { return true; }
            if str5compare(command, 'H', 'M', 'S', 'E', 'T') { return true; }
            if str5compare(command, 'H', 'S', 'C', 'A', 'N') { return true; }
            if str5compare(command, 'H', 'V', 'A', 'L', 'S') { return true; }
            if str5compare(command, 'B', 'L', 'P', 'O', 'P') { return true; }
            if str5compare(command, 'B', 'R', 'P', 'O', 'P') { return true; }
            if str5compare(command, 'L', 'P', 'U', 'S', 'H') { return true; }
            if str5compare(command, 'L', 'T', 'R', 'I', 'M') { return true; }
            if str5compare(command, 'R', 'P', 'U', 'S', 'H') { return true; }
            if str5compare(command, 'S', 'C', 'A', 'R', 'D') { return true; }
            if str5compare(command, 'S', 'S', 'C', 'A', 'N') { return true; }
            if str5compare(command, 'Z', 'C', 'A', 'R', 'D') { return true; }
            if str5compare(command, 'Z', 'R', 'A', 'N', 'K') { return true; }
            if str5compare(command, 'Z', 'S', 'C', 'A', 'N') { return true; }
            if str5compare(command, 'P', 'F', 'A', 'D', 'D') { return true; }
            return false;
        }
        6 => {
            if str6compare(command, 'E', 'X', 'I', 'S', 'T', 'S') { return true; }
            if str6compare(command, 'E', 'X', 'P', 'I', 'R', 'E') { return true; }
            if str6compare(command, 'U', 'N', 'L', 'I', 'N', 'K') { return true; }
            if str6compare(command, 'A', 'P', 'P', 'E', 'N', 'D') { return true; }
            if str6compare(command, 'B', 'I', 'T', 'P', 'O', 'S') { return true; }
            if str6compare(command, 'D', 'E', 'C', 'R', 'B', 'Y') { return true; }
            if str6compare(command, 'G', 'E', 'T', 'B', 'I', 'T') { return true; }
            if str6compare(command, 'G', 'E', 'T', 'S', 'E', 'T') { return true; }
            if str6compare(command, 'I', 'N', 'C', 'R', 'B', 'Y') { return true; }
            if str6compare(command, 'P', 'S', 'E', 'T', 'E', 'X') { return true; }
            if str6compare(command, 'S', 'E', 'T', 'B', 'I', 'T') { return true; }
            if str6compare(command, 'S', 'T', 'R', 'L', 'E', 'N') { return true; }
            if str6compare(command, 'H', 'S', 'E', 'T', 'N', 'X') { return true; }
            if str6compare(command, 'L', 'I', 'N', 'D', 'E', 'X') { return true; }
            if str6compare(command, 'L', 'P', 'U', 'S', 'H', 'X') { return true; }
            if str6compare(command, 'L', 'R', 'A', 'N', 'G', 'E') { return true; }
            if str6compare(command, 'R', 'P', 'U', 'S', 'H', 'X') { return true; }
            if str6compare(command, 'Z', 'C', 'O', 'U', 'N', 'T') { return true; }
            if str6compare(command, 'Z', 'R', 'A', 'N', 'G', 'E') { return true; }
            if str6compare(command, 'Z', 'S', 'C', 'O', 'R', 'E') { return true; }
            if str6compare(command, 'G', 'E', 'O', 'A', 'D', 'D') { return true; }
            if str6compare(command, 'G', 'E', 'O', 'P', 'O', 'S') { return true; }
            return false;
        }
        7 => {
            if str7compare(command, 'P', 'E', 'R', 'S', 'I', 'S', 'T') { return true; }
            if str7compare(command, 'P', 'E', 'X', 'P', 'I', 'R', 'E') { return true; }
            if str7compare(command, 'R', 'E', 'S', 'T', 'O', 'R', 'E') { return true; }
            if str7compare(command, 'H', 'E', 'X', 'I', 'S', 'T', 'S') { return true; }
            if str7compare(command, 'H', 'G', 'E', 'T', 'A', 'L', 'L') { return true; }
            if str7compare(command, 'H', 'I', 'N', 'C', 'R', 'B', 'Y') { return true; }
            if str7compare(command, 'H', 'S', 'T', 'R', 'L', 'E', 'N') { return true; }
            if str7compare(command, 'L', 'I', 'N', 'S', 'E', 'R', 'T') { return true; }
            if str7compare(command, 'Z', 'I', 'N', 'C', 'R', 'B', 'Y') { return true; }
            if str7compare(command, 'Z', 'P', 'O', 'P', 'M', 'A', 'X') { return true; }
            if str7compare(command, 'Z', 'P', 'O', 'P', 'M', 'I', 'N') { return true; }
            if str7compare(command, 'P', 'F', 'C', 'O', 'U', 'N', 'T') { return true; }
            if str7compare(command, 'G', 'E', 'O', 'D', 'I', 'S', 'T') { return true; }
            if str7compare(command, 'G', 'E', 'O', 'H', 'A', 'S', 'H') { return true; }
            return false;
        }
        8 => {
            if str8compare(command, 'E', 'X', 'P', 'I', 'R', 'E', 'A', 'T') { return true; }
            if str8compare(command, 'B', 'I', 'T', 'F', 'I', 'E', 'L', 'D') { return true; }
            if str8compare(command, 'B', 'I', 'T', 'C', 'O', 'U', 'N', 'T') { return true; }
            if str8compare(command, 'G', 'E', 'T', 'R', 'A', 'N', 'G', 'E') { return true; }
            if str8compare(command, 'S', 'E', 'T', 'R', 'A', 'N', 'G', 'E') { return true; }
            if str8compare(command, 'S', 'M', 'E', 'M', 'B', 'E', 'R', 'S') { return true; }
            if str8compare(command, 'B', 'Z', 'P', 'O', 'P', 'M', 'A', 'X') { return true; }
            if str8compare(command, 'B', 'Z', 'P', 'O', 'P', 'M', 'I', 'N') { return true; }
            if str8compare(command, 'Z', 'R', 'E', 'V', 'R', 'A', 'N', 'K') { return true; }
            return false;
        }
        9 => {
            if str9compare(command, 'P', 'E', 'X', 'P', 'I', 'R', 'E', 'A', 'T') { return true; }
            if str9compare(command, 'S', 'I', 'S', 'M', 'E', 'M', 'B', 'E', 'R') { return true; }
            if str9compare(command, 'Z', 'L', 'E', 'X', 'C', 'O', 'U', 'N', 'T') { return true; }
            if str9compare(command, 'Z', 'R', 'E', 'V', 'R', 'A', 'N', 'G', 'E') { return true; }
            if str9compare(command, 'G', 'E', 'O', 'R', 'A', 'D', 'I', 'U', 'S') { return true; }
            return false;
        }
        10 => {
            return false;
        }
        11 => {
            if str11compare(command, 'I', 'N', 'C', 'R', 'B', 'Y', 'F', 'L', 'O', 'A', 'T') { return true; }
            if str11compare(command, 'S', 'R', 'A', 'N', 'D', 'M', 'E', 'M', 'B', 'E', 'R') { return true; }
            if str11compare(command, 'Z', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'L', 'E', 'X') { return true; }
            return false;
        }
        12 => {
            if str12compare(command, 'H', 'I', 'N', 'C', 'R', 'B', 'Y', 'F', 'L', 'O', 'A', 'T') { return true; }
            return false;
        }
        13 => {
            if str13compare(command, 'Z', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'S', 'C', 'O', 'R', 'E') { return true; }
            return false;
        }
        14 => {
            if str14compare(command, 'Z', 'R', 'E', 'M', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'L', 'E', 'X') { return true; }
            if str14compare(command, 'Z', 'R', 'E', 'V', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'L', 'E', 'X') { return true; }
            return false;
        }
        15 => {
            if str15compare(command, 'Z', 'R', 'E', 'M', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'R', 'A', 'N', 'K') { return true; }
            return false;
        }
        16 => {
            if str16compare(command, 'Z', 'R', 'E', 'M', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'S', 'C', 'O', 'R', 'E') { return true; }
            if str16compare(command, 'Z', 'R', 'E', 'V', 'R', 'A', 'N', 'G', 'E', 'B', 'Y', 'S', 'C', 'O', 'R', 'E') { return true; }
            return false;
        }
        17 => {
            if str17compare(command, 'G', 'E', 'O', 'R', 'A', 'D', 'I', 'U', 'S', 'B', 'Y', 'M', 'E', 'M', 'B', 'E', 'R') { return true; }
            return false;
        }
        _ => {
            return false;
        }
    }
}

fn str3compare2(byte: &[u8], c1: &'static u8, c2: &'static u8, c3: &'static u8) -> bool {
    unsafe {
        return (byte.get_unchecked(0) == c1 || &(byte.get_unchecked(0) + 0x20u8) == c1) &&
            (byte.get_unchecked(1) == c2 || &(byte.get_unchecked(1) + 0x20u8) == c2) &&
            (byte.get_unchecked(2) == c3 || &(byte.get_unchecked(2) + 0x20u8) == c3);
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

pub fn extract_key(command: &String) -> Result<String, RedisProtocolError> {
    let mut iter = command.split_whitespace();
    iter.next();
    iter.next();
    let first_word = match iter.next() {
        Some(first_word) => first_word,
        None => "",
    };

    debug!("First word: {}", first_word);
    let maybe_key = match first_word.to_uppercase().as_str() {
    // Keys
        "DEL"              => { iter.next(); iter.next() }
        "DUMP"             => { iter.next(); iter.next() }
        "EXISTS"           => { iter.next(); iter.next() }
        "EXPIRE"           => { iter.next(); iter.next() }
        "EXPIREAT"         => { iter.next(); iter.next() }
        "PERSIST"          => { iter.next(); iter.next() }
        "PEXPIRE"          => { iter.next(); iter.next() }
        "PEXPIREAT"        => { iter.next(); iter.next() }
        "PTTL"             => { iter.next(); iter.next() }
        "RESTORE"          => { iter.next(); iter.next() }
        "SORT"             => { iter.next(); iter.next() }
        "TOUCH"            => { iter.next(); iter.next() }
        "TTL"              => { iter.next(); iter.next() }
        "TYPE"             => { iter.next(); iter.next() }
        "UNLINK"           => { iter.next(); iter.next() }
    // Strings
        "APPEND"           => { iter.next(); iter.next() }
        "BITFIELD"         => { iter.next(); iter.next() }
        "BITCOUNT"         => { iter.next(); iter.next() }
        "BITPOS"           => { iter.next(); iter.next() }
        "DECR"             => { iter.next(); iter.next() }
        "DECRBY"           => { iter.next(); iter.next() }
        "GET"              => { iter.next(); iter.next() }
        "GETBIT"           => { iter.next(); iter.next() }
        "GETRANGE"         => { iter.next(); iter.next() }
        "GETSET"           => { iter.next(); iter.next() }
        "INCR"             => { iter.next(); iter.next() }
        "INCRBY"           => { iter.next(); iter.next() }
        "INCRBYFLOAT"      => { iter.next(); iter.next() }
        "PSETEX"           => { iter.next(); iter.next() }
        "SET"              => { iter.next(); iter.next() }
        "SETBIT"           => { iter.next(); iter.next() }
        "SETEX"            => { iter.next(); iter.next() }
        "SETNX"            => { iter.next(); iter.next() }
        "SETRANGE"         => { iter.next(); iter.next() }
        "STRLEN"           => { iter.next(); iter.next() }
    // Hashes
        "HDEL"             => { iter.next(); iter.next() }
        "HEXISTS"          => { iter.next(); iter.next() }
        "HGET"             => { iter.next(); iter.next() }
        "HGETALL"          => { iter.next(); iter.next() }
        "HINCRBY"          => { iter.next(); iter.next() }
        "HINCRBYFLOAT"     => { iter.next(); iter.next() }
        "HKEYS"            => { iter.next(); iter.next() }
        "HLEN"             => { iter.next(); iter.next() }
        "HMGET"            => { iter.next(); iter.next() }
        "HMSET"            => { iter.next(); iter.next() }
        "HSCAN"            => { iter.next(); iter.next() }
        "HSET"             => { iter.next(); iter.next() }
        "HSETNX"           => { iter.next(); iter.next() }
        "HSTRLEN"          => { iter.next(); iter.next() }
        "HVALS"            => { iter.next(); iter.next() }
    // Lists
        "BLPOP"            => { iter.next(); iter.next() }
        "BRPOP"            => { iter.next(); iter.next() }
        //"BRPOPLPUSH"
        "LINDEX"           => { iter.next(); iter.next() }
        "LINSERT"          => { iter.next(); iter.next() }
        "LLEN"             => { iter.next(); iter.next() }
        "LPOP"             => { iter.next(); iter.next() }
        "LPUSH"            => { iter.next(); iter.next() }
        "LPUSHX"           => { iter.next(); iter.next() }
        "LRANGE"           => { iter.next(); iter.next() }
        "LREM"             => { iter.next(); iter.next() }
        "LSET"             => { iter.next(); iter.next() }
        "LTRIM"            => { iter.next(); iter.next() }
        "RPOP"             => { iter.next(); iter.next() }
        //"RPOPLPUSH"
        "RPUSH"            => { iter.next(); iter.next() }
        "RPUSHX"           => { iter.next(); iter.next() }
    // Sets
        "SADD"             => { iter.next(); iter.next() }
        "SCARD"            => { iter.next(); iter.next() }
        //"SDIFF"
        //"SDIFFSTORE"
        //"SINTER"
        //"SINTERSTORE"
        "SISMEMBER"        => { iter.next(); iter.next() }
        "SMEMBERS"         => { iter.next(); iter.next() }
        //"SMOVE"            => { iter.next(); iter.next() }
        "SPOP"             => { iter.next(); iter.next() }
        "SRANDMEMBER"      => { iter.next(); iter.next() }
        "SREM"             => { iter.next(); iter.next() }
        "SSCAN"            => { iter.next(); iter.next() }
        //"SUNION
        //"SUNIONSTORE"
    // Sorted sets
        "BZPOPMAX"         => { iter.next(); iter.next() }
        "BZPOPMIN"         => { iter.next(); iter.next() }
        "ZADD"             => { iter.next(); iter.next() }
        "ZCARD"            => { iter.next(); iter.next() }
        "ZCOUNT"           => { iter.next(); iter.next() }
        "ZINCRBY"          => { iter.next(); iter.next() }
        //"ZINTERSTORE"
        "ZLEXCOUNT"        => { iter.next(); iter.next() }
        "ZPOPMAX"          => { iter.next(); iter.next() }
        "ZPOPMIN"          => { iter.next(); iter.next() }
        "ZRANGE"           => { iter.next(); iter.next() }
        "ZRANGEBYLEX"      => { iter.next(); iter.next() }
        "ZRANGEBYSCORE"    => { iter.next(); iter.next() }
        "ZRANK"            => { iter.next(); iter.next() }
        "ZREM"             => { iter.next(); iter.next() }
        "ZREMRANGEBYLEX"   => { iter.next(); iter.next() }
        "ZREMRANGEBYRANK"  => { iter.next(); iter.next() }
        "ZREMRANGEBYSCORE" => { iter.next(); iter.next() }
        "ZREVRANGE"        => { iter.next(); iter.next() }
        "ZREVRANGEBYLEX"   => { iter.next(); iter.next() }
        "ZREVRANGEBYSCORE" => { iter.next(); iter.next() }
        "ZREVRANK"         => { iter.next(); iter.next() }
        "ZSCAN"            => { iter.next(); iter.next() }
        "ZSCORE"           => { iter.next(); iter.next() }
        //"ZUNIONSTORE"
    // Hyperloglog
        "PFADD"            => { iter.next(); iter.next() }
        "PFCOUNT"          => { iter.next(); iter.next() }
        //"PFMERGE"
    // Geo
        "GEOADD"           => { iter.next(); iter.next() }
        "GEODIST"          => { iter.next(); iter.next() }
        "GEOHASH"          => { iter.next(); iter.next() }
        "GEOPOS"           => { iter.next(); iter.next() }
        "GEORADIUS"        => { iter.next(); iter.next() }
        "GEORADIUSBYMEMBER"=> { iter.next(); iter.next() }

    // Proxy
        "COMMAND"          => { Some("key0") }
        "PING"             => { Some("key0") }
        _ => {
            error!("Unrecognized command: {}", first_word);
            None
        }
    };
    match maybe_key {
        Some(key) => Ok(key.to_string()),
        None => Err(RedisProtocolError {}),
    }
}

fn supported_keys(command: &str) -> bool {
    match command {
    // Keys
        "DEL"              => true,
        "DUMP"             => true,
        "EXISTS"           => true,
        "EXPIRE"           => true,
        "EXPIREAT"         => true,
        "PERSIST"          => true,
        "PEXPIRE"          => true,
        "PEXPIREAT"        => true,
        "PTTL"             => true,
        "RESTORE"          => true,
        "SORT"             => true,
        "TOUCH"            => true,
        "TTL"              => true,
        "TYPE"             => true,
        "UNLINK"           => true,
    // Strings
        "APPEND"           => true,
        "BITFIELD"         => true,
        "BITCOUNT"         => true,
        "BITPOS"           => true,
        "DECR"             => true,
        "DECRBY"           => true,
        "GET"              => true,
        "GETBIT"           => true,
        "GETRANGE"         => true,
        "GETSET"           => true,
        "INCR"             => true,
        "INCRBY"           => true,
        "INCRBYFLOAT"      => true,
        "PSETEX"           => true,
        "SET"              => true,
        "SETBIT"           => true,
        "SETEX"            => true,
        "SETNX"            => true,
        "SETRANGE"         => true,
        "STRLEN"           => true,
    // Hashes
        "HDEL"             => true,
        "HEXISTS"          => true,
        "HGET"             => true,
        "HGETALL"          => true,
        "HINCRBY"          => true,
        "HINCRBYFLOAT"     => true,
        "HKEYS"            => true,
        "HLEN"             => true,
        "HMGET"            => true,
        "HMSET"            => true,
        "HSCAN"            => true,
        "HSET"             => true,
        "HSETNX"           => true,
        "HSTRLEN"          => true,
        "HVALS"            => true,
    // Lists
        "BLPOP"            => true,
        "BRPOP"            => true,
        //"BRPOPLPUSH"
        "LINDEX"           => true,
        "LINSERT"          => true,
        "LLEN"             => true,
        "LPOP"             => true,
        "LPUSH"            => true,
        "LPUSHX"           => true,
        "LRANGE"           => true,
        "LREM"             => true,
        "LSET"             => true,
        "LTRIM"            => true,
        "RPOP"             => true,
        //"RPOPLPUSH"
        "RPUSH"            => true,
        "RPUSHX"           => true,
    // Sets
        "SADD"             => true,
        "SCARD"            => true,
        //"SDIFF"
        //"SDIFFSTORE"
        //"SINTER"
        //"SINTERSTORE"
        "SISMEMBER"        => true,
        "SMEMBERS"         => true,
        //"SMOVE"            => true,
        "SPOP"             => true,
        "SRANDMEMBER"      => true,
        "SREM"             => true,
        "SSCAN"            => true,
        //"SUNION
        //"SUNIONSTORE"
    // Sorted sets
        "BZPOPMAX"         => true,
        "BZPOPMIN"         => true,
        "ZADD"             => true,
        "ZCARD"            => true,
        "ZCOUNT"           => true,
        "ZINCRBY"          => true,
        //"ZINTERSTORE"
        "ZLEXCOUNT"        => true,
        "ZPOPMAX"          => true,
        "ZPOPMIN"          => true,
        "ZRANGE"           => true,
        "ZRANGEBYLEX"      => true,
        "ZRANGEBYSCORE"    => true,
        "ZRANK"            => true,
        "ZREM"             => true,
        "ZREMRANGEBYLEX"   => true,
        "ZREMRANGEBYRANK"  => true,
        "ZREMRANGEBYSCORE" => true,
        "ZREVRANGE"        => true,
        "ZREVRANGEBYLEX"   => true,
        "ZREVRANGEBYSCORE" => true,
        "ZREVRANK"         => true,
        "ZSCAN"            => true,
        "ZSCORE"           => true,
        //"ZUNIONSTORE"
    // Hyperloglog
        "PFADD"            => true,
        "PFCOUNT"          => true,
        //"PFMERGE"
    // Geo
        "GEOADD"           => true,
        "GEODIST"          => true,
        "GEOHASH"          => true,
        "GEOPOS"           => true,
        "GEORADIUS"        => true,
        "GEORADIUSBYMEMBER"=> true,
        _ => false,
    }
}

#[test]
fn test_parsing_speed() {
    let num_runs = 10000000;
    init_logging_info();
    let a = "*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n".to_string();
    // Using this test function to test how fast hashing can be.
    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = extract_key(&a);
    }
    info!("Time spent with default: {:?}", Instant::now() - start);
    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = extract_key2(&a);
    }
    info!("Time spent with extract_key2: {:?}", Instant::now() - start);
    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = extract_key3(&a);
    }
    info!("Time spent with extract_key3: {:?}", Instant::now() - start);
    let start = Instant::now();
    for _ in 1..num_runs {
        let _ = extract_key2(&a);
    }
    info!("Time spent with extract_key2: {:?}", Instant::now() - start);
}
