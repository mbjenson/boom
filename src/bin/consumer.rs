use lapin::{
    message::DeliveryResult,
    options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
    Connection, ConnectionProperties,
};
use futures_lite::stream::StreamExt;
use apache_avro::from_value;
use apache_avro::Reader;
use std::io::BufReader;

mod structs;
mod database;
mod utils;

async fn process_record(record: apache_avro::types::Value) {
    let alert_packet: structs::AlertPacket = from_value(&record).unwrap();
    if database::alert_exists(alert_packet.candid).await {
        println!("Alert already exists: {}, skipping...", alert_packet.candid);
        return;
    }
    // print the objectId and candid of the alert
    println!("Processing objectId: {}, candid: {}", alert_packet.objectId, alert_packet.candid);

    let coordinates = alert_packet.get_coordinates();

    let alert = structs::AlertWithCoords {
        schemavsn: alert_packet.schemavsn.clone(),
        publisher: alert_packet.publisher.clone(),
        candid: alert_packet.candid.clone(),
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
            prv_candidates: prv_candidates,
        };
        database::save_alert_aux(alert_aux).await;
    }

    // simulate processing time
    // tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // we are done processing the alert
    // println!("Processed alert: {}", alert_packet.candid);
}

#[tokio::main]
async fn main() {
    let uri = "amqp://localhost:5672";
    let options = ConnectionProperties::default()
        // Use tokio executor and reactor.
        // At the moment the reactor is only available for unix.
        .with_executor(tokio_executor_trait::Tokio::current())
        .with_reactor(tokio_reactor_trait::Tokio);

    let connection = Connection::connect(uri, options).await.unwrap();
    let channel = connection.create_channel().await.unwrap();

    let _queue = channel
        .queue_declare(
            "queue_test",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let mut consumer = channel
        .basic_consume(
            "queue_test",
            "tag_foo",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    // consumer.set_delegate() will allow the consumer to spawn processes to handle the messages
    // TODO: limit the number of concurrent processes that tokin can spawn
    // otherwise, we process things too fast and can run out of memory
    // consumer.set_delegate(move |delivery: DeliveryResult| async move {
    //     let delivery = match delivery {
    //         // Carries the delivery alongside its channel
    //         Ok(Some(delivery)) => {
    //             let content = delivery.data.clone();
    //             let reader = Reader::new(BufReader::new(&content[..])).unwrap();
    //             for record in reader {
    //                 let record = record.unwrap();
    //                 // we can now process the alert
    //                 process_record(record).await;
    //             }
    //             delivery
    //         }
    //         // The consumer got canceled
    //         Ok(None) => return,
    //         // Carries the error and is always followed by Ok(None)
    //         Err(error) => {
    //             dbg!("Failed to consume queue message {}", error);
    //             return;
    //         }
    //     };

    //     // Do something with the delivery data (The message payload)

    //     delivery
    //         .ack(BasicAckOptions::default())
    //         .await
    //         .expect("Failed to ack send_webhook_event message");
    // });

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.unwrap();
        let content = delivery.data.clone();
        let reader = Reader::new(BufReader::new(&content[..])).unwrap();
        for record in reader {
            let record = record.unwrap();
            // we can now process the alert
            process_record(record).await;
        }
        delivery
            .ack(BasicAckOptions::default())
            .await
            .expect("Failed to ack send_webhook_event message");
    }
}
