use amqprs::{channel::QueueBindArguments, connection::Connection, FieldTable};
use apache_avro::from_value;
use apache_avro::Reader;
use std::io::BufReader;

mod structs;
mod database;
mod utils;

struct RabbitConnect {
    host: String,
    port: u16,
    username: String,
    password: String,
}

async fn connect_rabbitmq(
    connection_details: &RabbitConnect,
) -> Connection {
    let connection = Connection::open(&amqprs::connection::OpenConnectionArguments::new(
        &connection_details.host,
        connection_details.port,
        &connection_details.username,
        &connection_details.password,
    ).virtual_host("/").connection_name("ztf_alerts_consumer"))
    .await
    .unwrap();
    connection.register_callback(amqprs::callbacks::DefaultConnectionCallback)
        .await
        .unwrap();
    connection
}

async fn channel_rabbitmq(connection: &mut Connection) -> amqprs::channel::Channel {
    // let channel = connection.open_channel(Some(1)).await.unwrap();
    // the line above skips errors, we can use the line below to handle errors
    let channel = match connection.open_channel(None).await {
        Ok(channel) => channel,
        Err(e) => {
            println!("Error opening channel: {}", e);
            panic!("Error opening channel");
        }
    };
    channel.register_callback(amqprs::callbacks::DefaultChannelCallback)
        .await
        .unwrap();
    channel
}

async fn bind_queue_to_exchange(
    connection: &mut Connection,
    channel: &mut amqprs::channel::Channel,
    connection_details: &RabbitConnect,
    queue: &str,
) {
    if !connection.is_open() {
        println!("Connection is closed. Reconnecting...");
        *connection = connect_rabbitmq(connection_details).await;
        *channel = channel_rabbitmq(connection).await;
        println!("Reconnected: {}", connection);
    }

    let args = FieldTable::default();

    let qparams = amqprs::channel::QueueDeclareArguments::default()
        .queue(queue.to_owned())
        .auto_delete(false)
        .durable(true)
        .arguments(args)
        .finish();

    let (queue, _, _) = channel.queue_declare(qparams).await.unwrap().unwrap();

    if !channel.is_open() {
        println!("Channel is closed. Reopening...");
        *channel = channel_rabbitmq(connection).await;
        println!("Reopened: {}", channel);
    }

    // channel.basic_qos(amqprs::channel::BasicQosArguments::new(
    //     1,
    //     0,
    //     false,
    // )).await.unwrap();

    // bind the queue to the exchange
    channel.queue_bind(
        QueueBindArguments::new(&queue, "ztf_alerts", "ztf_alerts_workers")
    ).await.unwrap();
    
}

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
    

async fn receive(
    connection: &mut Connection,
    channel: &mut amqprs::channel::Channel,
    connection_details: &RabbitConnect,
) {
    if !connection.is_open() {
        println!("Connection is closed. Reconnecting...");
        *connection = connect_rabbitmq(connection_details).await;
        *channel = channel_rabbitmq(connection).await;
        println!("Reconnected: {}", connection);
    }

    if !channel.is_open() {
        println!("Channel is closed. Reopening...");
        *channel = channel_rabbitmq(connection).await;
        println!("Reopened: {}", channel);
    }

    let queue = "ztf_alerts_workers";
    // create a random consumer tag using uuid
    let consumer_tag = uuid::Uuid::new_v4().to_string();
    bind_queue_to_exchange(connection, channel, connection_details, queue).await;

    // let args = amqprs::channel::BasicConsumeArguments::new(
    //     queue,
    //     consumer_tag.as_str()
    // ).auto_ack(false).exclusive(false).no_local(false).no_wait(false).finish();

    let args = amqprs::channel::BasicGetArguments::new(
        queue,
    ).no_ack(false).finish();


    let mut i = 0;
    while let Some(message) = channel.basic_get(args.clone()).await.unwrap() {
        let content = message.2;
        // content is a Vec<u8>, representing the content of an avro file
        let reader = Reader::new(BufReader::new(&content[..])).unwrap();
        for record in reader {
            let record = record.unwrap();
            // print the objectId and candid of the alert
            // println!("{}: objectId: {}, candid: {}", i, alert_packet.objectId, alert_packet.candid);

            // we can now process the alert
            process_record(record).await;
            // fake sleep
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        let args = amqprs::channel::BasicAckArguments::new(message.0.delivery_tag(), false);
        // acknowledge the message
        channel.basic_ack(args).await.unwrap();
        i += 1;
    }
}


#[tokio::main]
async fn main() {
    let connection_details = RabbitConnect {
        host: "localhost".to_string(),
        port: 5672,
        username: "guest".to_string(),
        password: "guest".to_string(),
    };

    let mut connection = connect_rabbitmq(&connection_details).await;
    let mut channel = channel_rabbitmq(&mut connection).await;

    receive(
        &mut connection,
        &mut channel,
        &connection_details,
    ).await;
}