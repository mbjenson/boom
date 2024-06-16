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

pub async fn consume_with_max<F, Fut>(
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

        if let Some(max) = max_messages {
            if message_count >= max {
                should_continue = false;
            }
        }
    }
}

pub async fn consume<F, Fut>(
    consumer: &mut lapin::Consumer,
    callback: F,
) where
    F: Fn(Vec<u8>) -> Fut,
    Fut: Future<Output = Result<(), Box<dyn std::error::Error>>> + 'static,
{
    loop {
        match consumer.next().await {
            Some(Ok(delivery)) => {
                let content = delivery.data.clone();
                if let Err(e) = callback(content).await {
                    if e.to_string() == "Received exit message" {
                        println!("Received exit message. Exiting consumer loop.");
                        break;
                    }
                    eprintln!("Error processing message: {:?}", e);
                } else {
                    if let Err(e) = delivery
                        .ack(lapin::options::BasicAckOptions::default())
                        .await
                    {
                        eprintln!("Failed to ack delivery: {:?}", e);
                    }
                }
            }
            Some(Err(e)) => {
                eprintln!("Error receiving delivery: {:?}", e);
            }
            None => {
                println!("No more messages in queue. Exiting consumer loop.");

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
    // create a random queue name with a uuid
    let queue_name = format!("queue_test_{}", uuid::Uuid::new_v4());
    let uri = get_uri();
    let connection = connect(&uri).await;
    let channel = create_channel(&connection).await;
    declare_queue(&channel, &queue_name).await;

    let message = b"Hello, world!";
    publish_message(&channel, "", &queue_name, message).await;

    let mut consumer = create_consumer(&channel, &queue_name).await;

    consume_with_max(&mut consumer, |content| async move {
        assert_eq!(content, message.to_vec());
        Ok(())
    }, Some(1)).await;

    // this is somewhat of a hack, but to test the consume function which is an infinite loop
    // we send 2 messages, one hello word and one goodbye world, that the callback will check
    // and then we close the connection to break the loop
    let exit_message = b"Goodbye, world!";
    publish_message(&channel, "", &queue_name, exit_message).await;

    consume(&mut consumer, |content| async move {
        println!("Received message: {:?}", content);
        if content == exit_message.to_vec() {
            Err("Received exit message".into())
        } else {
            assert!(false);
            Ok(())
        }
    }).await;

    // close the channel and connection
    channel.close(200, "Bye").await.unwrap();
    connection.close(200, "Good Bye").await.unwrap();
}
