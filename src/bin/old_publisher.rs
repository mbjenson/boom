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

async fn create_queue(
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

    if !channel.is_open() {
        println!("Channel is closed. Reopening...");
        *channel = channel_rabbitmq(connection).await;
        println!("Reopened: {}", channel);
    }

    let args = amqprs::channel::QueueDeclareArguments::new(
        queue,
    )
    .durable(true)
    .auto_delete(false)
    .exclusive(false)
    .no_wait(false)
    .arguments(Default::default())
    .finish();

    channel.queue_declare(args).await.unwrap();
    
}

async fn purge_queue(
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

    if !channel.is_open() {
        println!("Channel is closed. Reopening...");
        *channel = channel_rabbitmq(connection).await;
        println!("Reopened: {}", channel);
    }

    let args = amqprs::channel::QueuePurgeArguments::new(
        queue,
    );
    channel.queue_purge(args).await.unwrap();
}

async fn send(
    connection: &mut Connection,
    channel: &mut amqprs::channel::Channel,
    connection_details: &RabbitConnect,
    queue: &str,
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
        &queue,
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

    // find the list of files in the data directory
    let files = std::fs::read_dir("data/ztf_public_20240611").unwrap();

    // // read the bytes from data/alert.avro and send them to RabbitMQ
    // let msg = std::fs::read("data/alert.avro").unwrap();

    let mut connection = connect_rabbitmq(&connection_details).await;
    let mut channel = channel_rabbitmq(&mut connection).await;

    let queue = "ztf_alerts_workers";

    create_queue(&mut connection, &mut channel, &connection_details, queue).await;

    // remove all the messages from the queue
    purge_queue(&mut connection, &mut channel, &connection_details, queue).await;


    let mut i = 0;
    for file in files {
        let file = file.unwrap();
        // open the file
        let msg = std::fs::read(file.path()).unwrap();
        send(
            &mut connection,
            &mut channel,
            &connection_details,
            queue,
            &msg,
        )
        .await;
        // tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        i += 1;
    }
    println!("Sent {} messages", i);
    // now stay alive
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}