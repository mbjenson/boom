use apache_avro::Reader;
use std::io::BufReader;
use zmq::{Context, Result, SUB};

extern crate boom;
use boom::alerts;
use boom::zeromq;

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new();
    let mut socket = context.socket(SUB).unwrap();

    let uri = zeromq::get_uri();
    socket.connect(&uri).unwrap();

    // Subscribe to all topics
    socket.set_subscribe(b"").unwrap();

    println!("Subscriber connected to {}", uri);

    loop {
        // Receive message
        let msg = match socket.recv_msg(0) {
            Ok(msg) => msg,
            Err(err) => {
                eprintln!("Error receiving message: {}", err);
                continue; // Continue to next iteration if receive fails
            }
        };

        let reader = Reader::new(BufReader::new(&msg[..])).unwrap();
        for record in reader {
            let record = record.unwrap();
            // we can now process the alert
            alerts::process_record(record).await;
        }
    }
}
