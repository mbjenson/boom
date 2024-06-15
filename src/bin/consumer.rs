use amqprs::{channel::QueueBindArguments, connection::Connection, FieldTable};
use apache_avro::from_value;
use apache_avro::Reader;
use std::io::BufReader;

mod structs;

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
    ).virtual_host("/"))
    .await
    .unwrap();
    connection.register_callback(amqprs::callbacks::DefaultConnectionCallback)
        .await
        .unwrap();
    connection
}

async fn channel_rabbitmq(connection: &mut Connection) -> amqprs::channel::Channel {
    let channel = connection.open_channel(None).await.unwrap();
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

    // bind the queue to the exchange
    channel.queue_bind(
        QueueBindArguments::new(&queue, "ztf_alerts", "")
    ).await.unwrap();
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
    let args = amqprs::channel::BasicConsumeArguments::new(
        queue,
        "",
    );
    bind_queue_to_exchange(connection, channel, connection_details, queue).await;

    let (_ctag, mut message_rx) = channel.basic_consume_rx(args.clone()).await.unwrap();
    println!("Consuming messages from queue: ztf_alerts");

    let mut i = 0;
    while let Some(message) = message_rx.recv().await {
        let content = message.content.unwrap();
        // content is a Vec<u8>, representing the content of an avro file
        let reader = Reader::new(BufReader::new(&content[..])).unwrap();
        for record in reader {
            let record = record.unwrap();
            let alert: structs::AlertPacket = from_value(&record).unwrap();
            // print the objectId and candid of the alert
            println!("{}: objectId: {}, candid: {}", i, alert.objectId, alert.candid);
        }
        let args = amqprs::channel::BasicAckArguments::new(message.deliver.unwrap().delivery_tag(), false);
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