extern crate boom;

use boom::rabbitmq;

#[tokio::main]
async fn main() {
    let uri = rabbitmq::get_uri();
    let connection = rabbitmq::connect(&uri).await;
    let channel = rabbitmq::create_channel(&connection).await;
    rabbitmq::declare_queue(&channel, "ztf_alerts").await;

    let files = std::fs::read_dir("data/ztf_alerts").unwrap();
    for (i, f) in files.enumerate() {
        let f = f.unwrap();
        let msg = std::fs::read(f.path()).unwrap();
        rabbitmq::publish_message(&channel, "", "ztf_alerts", &msg).await;
        println!("Sent message: {}", i);
    }
}
