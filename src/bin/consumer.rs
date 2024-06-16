use apache_avro::from_value;
use apache_avro::Reader;
use std::env;
use std::io::BufReader;

extern crate boom;

use boom::database;
use boom::rabbitmq;
use boom::structs;

async fn process_record(record: apache_avro::types::Value) {
    let alert_packet: structs::AlertPacket = from_value(&record).unwrap();
    if database::alert_exists(alert_packet.candid).await {
        println!("Alert already exists: {}, skipping...", alert_packet.candid);
        return;
    }
    // print the objectId and candid of the alert
    println!(
        "Processing objectId: {}, candid: {}",
        alert_packet.objectId, alert_packet.candid
    );

    let coordinates = alert_packet.get_coordinates();

    let alert = structs::AlertWithCoords {
        schemavsn: alert_packet.schemavsn.clone(),
        publisher: alert_packet.publisher.clone(),
        candid: alert_packet.candid,
        objectId: alert_packet.objectId.clone(),
        candidate: alert_packet.candidate.clone(),
        cutoutScience: alert_packet.cutoutScience.clone(),
        cutoutTemplate: alert_packet.cutoutTemplate.clone(),
        cutoutDifference: alert_packet.cutoutDifference.clone(),
        coordinates: Some(coordinates),
    };

    // we can now save the alert to the database
    database::save_alert(&alert).await;

    let prv_candidates = alert_packet.prv_candidates.clone();

    if database::alert_aux_exists(&alert_packet.objectId.to_owned()).await {
        database::update_alert_aux(&alert_packet.objectId.to_owned(), prv_candidates).await;
    } else {
        let alert_aux = structs::AlertAux {
            _id: alert_packet.objectId.clone(),
            prv_candidates,
        };
        database::save_alert_aux(alert_aux).await;
    }

    // simulate processing time
    // tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // we are done processing the alert
    // println!("Processed alert: {}", alert_packet.candid);
}

// callback returns an async future with a Result type
async fn callback(content: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
    let reader = Reader::new(BufReader::new(&content[..])).unwrap();
    for record in reader {
        let record = record.unwrap();
        // we can now process the alert
        process_record(record).await;
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
