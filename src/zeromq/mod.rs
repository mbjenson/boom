use std::env;

pub fn get_uri() -> String {
    let host = env::var("ZEROMQ_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("ZEROMQ_PORT").unwrap_or_else(|_| "5555".to_string());
    format!("tcp://{}:{}", host, port)
}
