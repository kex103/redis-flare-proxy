use std::collections::BTreeMap;
use toml;
use std::fs::File;
use std::io::{Read};
use hash::HashFunction;

#[derive(Deserialize, Clone, Serialize, Eq, PartialEq, Hash)]
pub enum Distribution {
    Modula,
    Ketama,
    Random,
}

#[derive(Deserialize, Clone, Serialize, Eq, PartialEq)]
pub struct RedFlareProxyConfig {
    pub admin: AdminConfig,
    pub pools: BTreeMap<String, BackendPoolConfig>,
}

fn default_retry_timeout() -> usize {
    return 1000;
}
fn default_distribution() -> Distribution {
    return Distribution::Modula;
}
fn default_hash_function() -> HashFunction {
    return HashFunction::Fnv1a64;
}

#[derive(Deserialize, Clone, Serialize, Eq, PartialEq, Hash)]
pub struct BackendPoolConfig {
    pub listen: String,

    pub servers: Vec<BackendConfig>,

    #[serde(default)]
    pub timeout: usize,

    #[serde(default)]
    pub failure_limit: usize,

    #[serde(default = "default_retry_timeout")]
    pub retry_timeout: usize,

    #[serde(default)]
    pub auto_eject_hosts: bool,

    #[serde(default = "default_distribution")]
    pub distribution: Distribution,

    #[serde(default = "default_hash_function")]
    pub hash_function: HashFunction,

    #[serde(default)]
    pub hash_tag: String,
}
#[derive(Deserialize, Clone, Serialize, Eq, PartialEq, Hash)]
pub struct BackendConfig {
    pub host: Option<String>,

    // How to handle RedisCluster list of hosts?

    pub weight: usize,

    #[serde(default)]
    pub db: usize,
    #[serde(default)]
    pub auth: String,

    // Used for redis cluster.
    #[serde(default)]
    pub use_cluster: bool,

    #[serde(default)]
    pub cluster_hosts: Vec<String>,
}

#[derive(Deserialize, Clone, Serialize, Eq, PartialEq)]
pub struct AdminConfig {
    pub listen: String,
}

pub fn load_config(full_config_path: String) -> Result<RedFlareProxyConfig, String> {
    // TODO: Change to result
    // TOOD: trim config_path
    let config_path = full_config_path.trim();
    let mut file = match File::open(&config_path) {
        Ok(file) => file,
        Err(err) => {
            return Err(format!("Really, I Failed to open file {}: {:?}", config_path, err));
        }
    };
    let mut file_contents = String::new();
    match file.read_to_string(&mut file_contents) {
        Ok(_) => (),
        Err(err) => {
            return Err(format!("Failed to read file {}: {:?}", config_path, err));
        }
    };
    debug!("Config contents: {}", file_contents);
    let config: RedFlareProxyConfig = match toml::from_str(&file_contents) {
        Ok(config) => config,
        Err(err) => {
            return Err(format!("Failed to convert file to toml {}: {:?}", config_path, err));
        }
    };
    Ok(config)
}