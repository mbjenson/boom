use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};

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

    // find the list of files in the data directory
    // let files = std::fs::read_dir("data/ztf_public_20240611").unwrap();
    let files = std::fs::read_dir("data/alerts").unwrap();
    let mut i = 0;
    for f in files {
        let f = f.unwrap();
        let msg = std::fs::read(f.path()).unwrap();
        channel
            .basic_publish(
                "",
                "queue_test",
                BasicPublishOptions::default(),
                &msg,
                BasicProperties::default(),
            )
            .await
            .unwrap()
            .await
            .unwrap();

        println!("Sent message: {}", i);
        i += 1;
        // tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        
}
