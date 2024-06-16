use std::future::Future;

use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};
use tokio_stream::StreamExt;

pub async fn connect(uri: &str) -> Connection {
    let options = ConnectionProperties::default()
        // Use tokio executor and reactor.
        // At the moment the reactor is only available for unix.
        .with_executor(tokio_executor_trait::Tokio::current())
        .with_reactor(tokio_reactor_trait::Tokio);

    Connection::connect(uri, options).await.unwrap()
}

pub async fn create_channel(connection: &Connection) -> lapin::Channel {
    connection.create_channel().await.unwrap()
}

pub async fn declare_queue(channel: &lapin::Channel, queue_name: &str) {
    channel
        .queue_declare(
            queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
}

pub async fn publish_message(
    channel: &lapin::Channel,
    exchange: &str,
    queue_name: &str,
    message: &[u8],
) {
    channel
        .basic_publish(
            exchange,
            queue_name,
            BasicPublishOptions::default(),
            message,
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
}

pub async fn create_consumer(channel: &lapin::Channel, queue_name: &str) -> lapin::Consumer {
    // randomly generate a tag for the consumer
    let tag = format!("tag_{}", uuid::Uuid::new_v4());

    channel.basic_qos(1, Default::default()).await.unwrap();
    channel
        .basic_consume(
            queue_name,
            &tag,
            lapin::options::BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap()
}

// next is a function that takes a consumer + a callback function to run on each message
// the callback is an async function that returns a Result type
pub async fn consume<F, Fut>(consumer: &mut lapin::Consumer, callback: F)
where
    F: Fn(Vec<u8>) -> Fut,
    Fut: Future<Output = Result<(), Box<dyn std::error::Error>>> + 'static,
{
    while let Some(delivery) = consumer.next().await {
        if let Ok(delivery) = delivery {
            let content = delivery.data.clone();
            callback(content).await.unwrap();
            delivery
                .ack(lapin::options::BasicAckOptions::default())
                .await
                .expect("basic_ack");
        }
    }
}