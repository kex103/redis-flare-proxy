use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::hash::Hash;
use std::result::Result;
use std::error::Error;
use std::fmt;

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

pub fn determine_modula_shard(key: &String, count: usize) -> Result<usize, RedisProtocolError> {
    Ok(hash(key) % count)
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
    let maybe_key = match first_word.to_owned().to_uppercase().as_str() {
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

pub fn hash(key: &String) -> usize {
    //debug!("Hashing: {}", key);
    //let mut hasher = DefaultHasher::new();
    //key.hash(&mut hasher);
    //hasher.finish() as usize


    let mut hasher = FnvHasher::default();
    hasher.write(key.as_bytes());
    hasher.finish() as usize
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
        hash(&a);
    }
    info!("Time spent with default: {:?}", Instant::now() - start);
    let start = Instant::now();
    for _ in 1..2000000 {
        fnv1a(a.clone());
    }
    info!("Time spent with fnv1a: {:?}", Instant::now() - start);
    let start = Instant::now();
    for _ in 1..2000000 {
        fnv1a2(a.clone());
    }
    info!("Time spent with fnv1a2: {:?}", Instant::now() - start);
}

use std::default::Default;
use std::hash::{BuildHasherDefault};
use std::collections::{HashMap, HashSet};

fn fnv1a(bytes: String) -> usize {
    let mut hasher = FnvHasher::default();
    hasher.write(bytes.as_bytes());
    hasher.finish() as usize
}

fn fnv1a2(key: String) -> usize {
    let mut hash = 0xcbf29ce484222325;
    for byte in key.as_bytes().iter() {
        hash = hash ^ (*byte as u64);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash as usize
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

impl FnvHasher {
    /// Create an FNV hasher starting with a state corresponding
    /// to the hash `key`.
    #[inline]
    pub fn with_key(key: u64) -> FnvHasher {
        FnvHasher(key)
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

/// A builder for default FNV hashers.
pub type FnvBuildHasher = BuildHasherDefault<FnvHasher>;

/// A `HashMap` using a default FNV hasher.
pub type FnvHashMap<K, V> = HashMap<K, V, FnvBuildHasher>;

/// A `HashSet` using a default FNV hasher.
pub type FnvHashSet<T> = HashSet<T, FnvBuildHasher>;