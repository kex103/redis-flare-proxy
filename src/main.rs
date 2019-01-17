extern crate mio;
extern crate mio_more;
extern crate toml;
#[macro_use]
extern crate log;
extern crate log4rs;
extern crate env_logger;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate clap;
use redflareproxy::ProxyError;
use clap::{Arg, App};
extern crate daemonize;
extern crate conhash;
extern crate rand;
extern crate crc16;
extern crate bufstream;
extern crate fxhash;
extern crate crc;
extern crate fasthash;
extern crate hashers;
extern crate hashbrown;
extern crate memchr;
use log::LogLevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::config::{Appender, Config, Root};

mod admin;
mod redflareproxy;
mod config;
mod backend;
mod cluster_backend;
mod backendpool;
mod redisprotocol;
mod hash;
mod client;
mod stats;

mod bufreader;

#[cfg(test)]
pub fn init_logging() {
    let stdout = ConsoleAppender::builder().build();
    let config = 
            Config::builder()
                .appender(Appender::builder().build("stdout", Box::new(stdout)))
                .build(Root::builder().appender("stdout").build(LogLevelFilter::Debug))
                .unwrap();

    match log4rs::init_config(config) {
        Ok(_) => {},
        Err(logger_error) => {
            println!("Logging error: {:?}", logger_error);
            return;
        }
    };
}
#[cfg(test)]
pub fn init_logging_info() {
    let stdout = ConsoleAppender::builder().build();
    let config = 
            Config::builder()
                .appender(Appender::builder().build("stdout", Box::new(stdout)))
                .build(Root::builder().appender("stdout").build(LogLevelFilter::Info))
                .unwrap();

    match log4rs::init_config(config) {
        Ok(_) => {},
        Err(logger_error) => {
            println!("Logging error: {:?}", logger_error);
            return;
        }
    };
}


/*
Entrypoint for redflareproxy.
*/
fn main() -> Result<(), ProxyError> {
    // Take in args.
    let matches = App::new("RedFlareProxy")
                    .version("0.1")
                    .author("Kevin X. <xiaok10003@gmail.com>")
                    .about("Fast, light-weight redis proxy")
                    .arg(Arg::with_name("config")
                        .short("c")
                        .long("config")
                        .value_name("FILE")
                        .default_value("conf/config.toml")
                        .help("Sets a custom config file")
                        .takes_value(true))
                    .arg(Arg::with_name("log_file")
                            .short("o")
                            .long("log_file")
                            .value_name("LOG_FILE")
                            .takes_value(true)
                        .help("Sets the log file to output to"))
                    .arg(Arg::with_name("log_level")
                        .short("l")
                        .long("log_level")
                        .value_name("LOG_LEVEL")
                        .default_value("INFO")
                        .help("Sets the level of verbosity: DEBUG/INFO/WARNING/ERROR"))
                    .get_matches();

    // initialize logging
    let log_level = match matches.value_of("log_level").unwrap().to_uppercase().as_str().trim() {
        "DEBUG" => LogLevelFilter::Debug,
        "INFO" => LogLevelFilter::Info,
        "WARNING" => LogLevelFilter::Warn,
        "ERROR" => LogLevelFilter::Error,
        level => {
            return Err(ProxyError::InvalidLogLevel(level.to_string()));
        }
    };

    let stdout = ConsoleAppender::builder().build();

    let log_file = matches.value_of("log_file");
    let config = match log_file {
        Some(file_path) => {
            let requests: log4rs::append::file::FileAppender = match FileAppender::builder()
                .encoder(Box::new(PatternEncoder::new("{d} - {m}{n}"))).build(file_path) {
                Ok(a) => a,
                Err(err) => {
                    return Err(ProxyError::LogFileFailure(file_path.to_string(), err));
                }
            };

            try!(Config::builder()
                .appender(Appender::builder().build("stdout", Box::new(stdout)))
                .appender(Appender::builder().build("logfile", Box::new(requests)))
                .build(Root::builder().appender("stdout").appender("logfile").build(log_level)))
        }
        None => {
            try!(Config::builder()
                .appender(Appender::builder().build("stdout", Box::new(stdout)))
                .build(Root::builder().appender("stdout").build(log_level)))
        }
    };

    try!(log4rs::init_config(config));

    let config_path = matches.value_of("config").unwrap();
    
    // Start proxy.
    debug!("Starting up");

    let mut redflareproxy = try!(redflareproxy::RedFlareProxy::new(config_path.to_owned()));
    try!(redflareproxy.run());
    debug!("Finished.");
    return Ok(());
}

impl std::convert::From<log::SetLoggerError> for redflareproxy::ProxyError {
    fn from(error: log::SetLoggerError) -> Self {
        return ProxyError::SetLoggerError(error);
    }
}

impl std::convert::From<log4rs::config::Errors> for redflareproxy::ProxyError {
    fn from(error: log4rs::config::Errors) -> Self {
        return ProxyError::InvalidParams(error);
    }
}