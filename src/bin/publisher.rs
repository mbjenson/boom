use amqprs::{connection::Connection, BasicProperties, DELIVERY_MODE_PERSISTENT};

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

async fn send(
    connection: &mut Connection,
    channel: &mut amqprs::channel::Channel,
    connection_details: &RabbitConnect,
    result: &[u8],
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

    let args = amqprs::channel::BasicPublishArguments::new(
        "ztf_alerts",
        "",
    );
    channel.basic_publish(
        BasicProperties::default().with_delivery_mode(DELIVERY_MODE_PERSISTENT).finish(),
        result.into(),
        args,
    ).await.unwrap();

    println!("Sent message to RabbitMQ");
}


#[tokio::main]
async fn main() {
    let connection_details = RabbitConnect {
        host: "localhost".to_string(),
        port: 5672,
        username: "guest".to_string(),
        password: "guest".to_string(),
    };

    // read the bytes from data/alert.avro and send them to RabbitMQ
    let msg = std::fs::read("data/alert.avro").unwrap();

    let mut connection = connect_rabbitmq(&connection_details).await;
    let mut channel = channel_rabbitmq(&mut connection).await;

    loop {
        send(
            &mut connection,
            &mut channel,
            &connection_details,
            &msg,
        )
        .await;
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
    }
}