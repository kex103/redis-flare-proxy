use std::hash::Hasher;
use std::default::Default;
use crc::{crc16, crc32};
use fasthash::*;
use hashers::jenkins::spooky_hash;

// Reading: https://probablydance.com/2017/02/26/i-wrote-the-fastest-hashtable/
// Benchmarks: https://github.com/rurban/smhasher/
// Hsieh = not implemented. See http://www.azillionmonkeys.com/qed/hash.html


#[derive(Deserialize, Clone, Serialize, Eq, PartialEq, Hash)]
pub enum HashFunction {
    Crc16,
    Crc32,
    Fnv1a64,
    Murmur,
    Jenkins,
}

pub fn hash(hash_function: &HashFunction, key: &[u8]) -> usize {
    match hash_function {
        HashFunction::Crc16 => {
            crc16::checksum_x25(key) as usize
        }
        HashFunction::Crc32 => {
            crc32::checksum_ieee(key) as usize
        }
        HashFunction::Fnv1a64 => {
            // todo: Verify this is fnv1a_64, and not fnv1_64
            let mut hasher = FnvHasher::default();
            hasher.write(key);
            hasher.finish() as usize
        }
        HashFunction::Murmur => {
            let mut s: MurmurHasher = Default::default();
            s.write(key);
            s.finish() as usize
        }
        HashFunction::Jenkins => {
            spooky_hash::spooky(key) as usize
        }
    }
}

/// An implementation of the Fowler–Noll–Vo hash function.
///
/// See the [crate documentation](index.html) for more details.
#[allow(missing_copy_implementations)]
pub struct FnvHasher(u64);

impl Default for FnvHasher {
    #[inline]
    fn default() -> FnvHasher {
        FnvHasher(0xcbf29ce484222325)
    }
}

impl Hasher for FnvHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let FnvHasher(mut hash) = *self;

        for byte in bytes.iter() {
            hash = hash ^ (*byte as u64);
            hash = hash.wrapping_mul(0x100000001b3);
        }

        *self = FnvHasher(hash);
    }
}


#[cfg(test)]
use std::time::Instant;
#[cfg(test)]
use cluster_backend::init_logging;


#[test]
fn test_hashing_speed() {
    init_logging();
    let a = "key1".to_string();
    // Using this test function to test how fast hashing can be.
    let start = Instant::now();

    for _ in 1..2000000 {
        hash(&HashFunction::Fnv1a64, &a.as_bytes());
    }
    info!("Time spent with default: {:?}", Instant::now() - start);
}