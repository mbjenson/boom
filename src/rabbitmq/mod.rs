use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};
use std::future::Future;
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
pub async fn consume<F, Fut>(
    consumer: &mut lapin::Consumer,
    callback: F,
    max_messages: Option<usize>,
) where
    F: Fn(Vec<u8>) -> Fut,
    Fut: Future<Output = Result<(), Box<dyn std::error::Error>>> + 'static,
{
    let mut should_continue = true;
    let mut message_count = 0;

    while should_continue {
        if let Some(max) = max_messages {
            if message_count >= max {
                should_continue = false;
            }
        }

        match consumer.next().await {
            Some(Ok(delivery)) => {
                let content = delivery.data.clone();
                if let Err(e) = callback(content).await {
                    eprintln!("Error processing message: {:?}", e);
                } else {
                    if let Err(e) = delivery
                        .ack(lapin::options::BasicAckOptions::default())
                        .await
                    {
                        eprintln!("Failed to ack delivery: {:?}", e);
                    }
                    message_count += 1;
                }
            }
            Some(Err(e)) => {
                eprintln!("Error receiving delivery: {:?}", e);
            }
            None => {
                println!("No more messages in queue. Exiting consumer loop.");
                break;
            }
        }
    }
}

// this function creates the uri based on the host and port if provided as env variables
pub fn get_uri() -> String {
    let host = std::env::var("RABBITMQ_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("RABBITMQ_PORT").unwrap_or_else(|_| "5672".to_string());
    format!("amqp://{}:{}", host, port)
}

// add a basic test to check if we can publish and consume a message
#[tokio::test]
async fn test_publish_consume() {
    let uri = get_uri();
    let connection = connect(&uri).await;
    let channel = create_channel(&connection).await;
    declare_queue(&channel, "queue_test").await;

    let message = b"Hello, world!";
    publish_message(&channel, "", "queue_test", message).await;

    let mut consumer = create_consumer(&channel, "queue_test").await;

    let delivery = consumer.next().await.unwrap().unwrap();
    delivery
        .ack(lapin::options::BasicAckOptions::default())
        .await
        .expect("basic_ack");

    assert_eq!(delivery.data, message);

    // close the channel and connection
    channel.close(200, "Bye").await.unwrap();
    connection.close(200, "Good Bye").await.unwrap();
}
