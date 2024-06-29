use apache_avro::Reader;
use std::env;
use std::io::BufReader;

extern crate boom;

use boom::alerts;
use boom::rabbitmq;

// callback returns an async future with a Result type
async fn callback(content: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
    let reader = Reader::new(BufReader::new(&content[..])).unwrap();
    for record in reader {
        let record = record.unwrap();
        // we can now process the alert
        alerts::process_record(record).await;
    }
    Ok(())
}
#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    let uri = rabbitmq::get_uri();
    let connection = rabbitmq::connect(&uri).await;
    let channel = rabbitmq::create_channel(&connection).await;
    rabbitmq::declare_queue(&channel, "ztf_alerts").await;

    let mut consumer = rabbitmq::create_consumer(&channel, "ztf_alerts").await;

    let args: Vec<String> = env::args().collect();
    let max_messages: Option<usize> = args.get(1).and_then(|s| s.parse().ok());

    match max_messages {
        Some(value) => println!("Max messages to consume: {}", value),
        None => println!("Max messages to consume: None"),
    }

    let max_messages = if max_messages.is_some() {
        Some(max_messages.unwrap())
    } else {
        None
    };

    if let Some(max) = max_messages {
        println!("Consuming up to {} messages", max);
        rabbitmq::consume_with_max(&mut consumer, callback, Some(max)).await;
    } else {
        println!("Consuming all messages");
        rabbitmq::consume(&mut consumer, callback).await;
    }
}
