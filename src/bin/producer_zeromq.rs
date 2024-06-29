use std::time::Duration;
use tokio::time::sleep;
use zmq::{Context, PUB};

extern crate boom;
use boom::zeromq;

#[tokio::main]
async fn main() {
    if let Err(e) = run_publisher().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn run_publisher() -> Result<(), zmq::Error> {
    let uri = zeromq::get_uri();
    println!("Connecting to server at {}", &uri);

    let context = Context::new();
    let socket = context.socket(PUB).unwrap();

    // Connect to the server
    socket.bind(&uri).unwrap();

    let files = std::fs::read_dir("data/ztf_alerts").unwrap();
    for (i, f) in files.enumerate() {
        let f = f.unwrap();
        let msg = std::fs::read(f.path()).unwrap();
        let message = "Hello, World!";
        socket.send(&msg, 0)?;
        println!("Sent message: {}", i);

        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
