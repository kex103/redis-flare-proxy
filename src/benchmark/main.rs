extern crate mio;
extern crate mio_more;
#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate clap;
use clap::{Arg, App};
extern crate bufstream;

mod proxy_benchmarker;
fn main() {
    let matches = App::new("RustProxy Benchmarking Tool")
                    .version("0.1")
                    .author("Kevin X. <xiaok10003@gmail.com>")
                    .about("Used for benching redis proxies")
                    .arg(Arg::with_name("host")
                        .short("h")
                        .long("host")
                        .value_name("HOST")
                        .default_value("127.0.0.1")
                        .help("Host of proxy entrypoint")
                        .takes_value(true))
                    .arg(Arg::with_name("port")
                            .short("p")
                            .long("port")
                            .value_name("PORT")
                            .default_value("1531")
                            .takes_value(true)
                        .help("Port of proxy entrypoint"))
                    .arg(Arg::with_name("mock_host")
                        .short("m")
                        .long("mock_host")
                        .value_name("MOCK_HOST")
                        .default_value("127.0.0.1")
                        .takes_value(true)
                        .help("Host of proxy backend"))
                    .arg(Arg::with_name("mock_port")
                        .short("n")
                        .long("mock_port")
                        .value_name("MOCK_PORT")
                        .default_value("6380")
                        .takes_value(true)
                        .help("Port of proxy backend"))
                    .get_matches();

    let _ = env_logger::init();
    debug!("Starting up");

    let mut benchmarker = proxy_benchmarker::ProxyBenchmarker::new(
        matches.value_of("host").unwrap().to_string(),
        value_t!(matches, "port", usize).unwrap_or(1531),
        matches.value_of("mock_host").unwrap().to_string(),
        value_t!(matches, "mock_port", usize).unwrap_or(6379),
    ).unwrap();
    benchmarker.run();
    // structure should be... 
    // 1. benchmark intializes and listens for backend connections from proxy.
    // 2. start up proxy.
    // 3. send signal to benchmarkerr to begin.
}