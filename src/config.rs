use std::net::SocketAddr;
use std::collections::BTreeMap;
use toml;
use std::fs::File;
use std::io::{Read};
use hash::HashFunction;
use redflareproxy::ProxyError;

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

    #[serde(default)]
    pub enable_advanced_commands: bool,
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
fn default_warm_sockets() -> bool {
    return true;
}

#[derive(Deserialize, Clone, Serialize, Eq, PartialEq, Hash)]
pub struct BackendPoolConfig {
    pub listen: SocketAddr,

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

    #[serde(default = "default_warm_sockets")]
    pub warm_sockets: bool,
}
#[derive(Deserialize, Clone, Serialize, Eq, PartialEq, Hash)]
pub struct BackendConfig {
    #[serde(default)]
    pub host: Option<SocketAddr>,

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
    pub cluster_name: Option<String>,

    #[serde(default)]
    pub cluster_hosts: Vec<SocketAddr>,
}

#[derive(Deserialize, Clone, Serialize, Eq, PartialEq)]
pub struct AdminConfig {
    pub listen: String,
}

pub fn load_config(full_config_path: String) -> Result<RedFlareProxyConfig, ProxyError> {
    // TOOD: trim config_path
    let config_path = full_config_path.trim();
    let mut file = match File::open(&config_path) {
        Ok(file) => file,
        Err(err) => {
            return Err(ProxyError::ConfigFileFailure(config_path.to_string(), err));
        }
    };
    let mut file_contents = String::new();
    match file.read_to_string(&mut file_contents) {
        Ok(_) => (),
        Err(err) => {
            return Err(ProxyError::ConfigFileFormatFailure(config_path.to_string(), err));
        }
    };
    debug!("Config contents: {}", file_contents);
    let config: RedFlareProxyConfig = match toml::from_str(&file_contents) {
        Ok(config) => config,
        Err(err) => {
            return Err(ProxyError::ParseConfigFailure(config_path.to_string(), err));
        }
    };

    // Verify that cluster-associated configs should only be used when use_cluster is true, and verify that host is there when use_cluster is false.
    for (ref pool_name, ref pool_config) in &config.pools {
        for ref backend_config in &pool_config.servers {
            if !backend_config.use_cluster {
                if backend_config.host.is_none() {
                    return Err(ProxyError::ParseConfigFailure(config_path.to_string(), serde::de::Error::custom(format!("Non-cluster backend requires a 'host' in pool {}. {}", pool_name, config_path))));
                }
                if backend_config.cluster_hosts.len() > 0 {
                    return Err(ProxyError::ParseConfigFailure(config_path.to_string(), serde::de::Error::custom(format!("Non-cluster backend cannot have any 'cluster_hosts' in pool {}. {}", pool_name, config_path))));
                }
                if backend_config.cluster_name.is_some() {
                    return Err(ProxyError::ParseConfigFailure(config_path.to_string(), serde::de::Error::custom(format!("Non-cluster backend cannot have a 'cluster_name' in pool {}. {}", pool_name, config_path))));
                }
            } else {
                if backend_config.host.is_some() {
                    return Err(ProxyError::ParseConfigFailure(config_path.to_string(), serde::de::Error::custom(format!("Cluster backend cannot have a 'host' in pool {}. {}", pool_name, config_path))));
                }
                if backend_config.cluster_hosts.len() == 0 {
                    return Err(ProxyError::ParseConfigFailure(config_path.to_string(), serde::de::Error::custom(format!("Cluster backend requires 'cluster_hosts' in pool {}. {}", pool_name, config_path))));
                }
                if backend_config.cluster_name.is_none() {
                    return Err(ProxyError::ParseConfigFailure(config_path.to_string(), serde::de::Error::custom(format!("Cluster backend requires a 'cluster_name' in pool {}. {}", pool_name, config_path))));
                }

            }
        }
    }
    
    Ok(config)
}