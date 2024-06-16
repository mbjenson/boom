extern crate boom;

use boom::rabbitmq;

#[tokio::main]
async fn main() {
    let uri = "amqp://localhost:5672";
    let connection = rabbitmq::connect(uri).await;
    let channel = rabbitmq::create_channel(&connection).await;
    rabbitmq::declare_queue(&channel, "queue_test").await;

    let files = std::fs::read_dir("data/ztf_alerts").unwrap();
    let mut i = 0;
    for f in files {
        let f = f.unwrap();
        let msg = std::fs::read(f.path()).unwrap();
        rabbitmq::publish_message(&channel, "", "queue_test", &msg).await;
        println!("Sent message: {}", i);
        i += 1;
    }
}
