use lapin::{
    message::DeliveryResult,
    options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
    Connection, ConnectionProperties,
};
use futures_lite::stream::StreamExt;
use apache_avro::from_value;
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

    // simulate processing time
    // tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // we are done processing the alert
    // println!("Processed alert: {}", alert_packet.candid);
}

// #[tokio::main]
// async fn main() {
//     let uri = "amqp://localhost:5672";
//     let options = ConnectionProperties::default()
//         // Use tokio executor and reactor.
//         // At the moment the reactor is only available for unix.
//         .with_executor(tokio_executor_trait::Tokio::current())
//         .with_reactor(tokio_reactor_trait::Tokio);

//     let connection = Connection::connect(uri, options).await.unwrap();
//     let channel = connection.create_channel().await.unwrap();

//     let _queue = channel
//         .queue_declare(
//             "queue_test",
//             QueueDeclareOptions::default(),
//             FieldTable::default(),
//         )
//         .await
//         .unwrap();

//     let mut consumer = channel
//         .basic_consume(
//             "queue_test",
//             "tag_foo",
//             BasicConsumeOptions::default(),
//             FieldTable::default(),
//         )
//         .await
//         .unwrap();

//     // consumer.set_delegate() will allow the consumer to spawn processes to handle the messages
//     // TODO: limit the number of concurrent processes that tokin can spawn
//     // otherwise, we process things too fast and can run out of memory
//     // consumer.set_delegate(move |delivery: DeliveryResult| async move {
//     //     let delivery = match delivery {
//     //         // Carries the delivery alongside its channel
//     //         Ok(Some(delivery)) => {
//     //             let content = delivery.data.clone();
//     //             let reader = Reader::new(BufReader::new(&content[..])).unwrap();
//     //             for record in reader {
//     //                 let record = record.unwrap();
//     //                 // we can now process the alert
//     //                 process_record(record).await;
//     //             }
//     //             delivery
//     //         }
//     //         // The consumer got canceled
//     //         Ok(None) => return,
//     //         // Carries the error and is always followed by Ok(None)
//     //         Err(error) => {
//     //             dbg!("Failed to consume queue message {}", error);
//     //             return;
//     //         }
//     //     };

//     //     // Do something with the delivery data (The message payload)

//     //     delivery
//     //         .ack(BasicAckOptions::default())
//     //         .await
//     //         .expect("Failed to ack send_webhook_event message");
//     // });

//     while let Some(delivery) = consumer.next().await {
//         let delivery = delivery.unwrap();
//         let content = delivery.data.clone();
//         let reader = Reader::new(BufReader::new(&content[..])).unwrap();
//         for record in reader {
//             let record = record.unwrap();
//             // we can now process the alert
//             alerts::process_record(record).await;
//         }
//         delivery
//             .ack(BasicAckOptions::default())
//             .await
//             .expect("Failed to ack send_webhook_event message");
//     }
// }
