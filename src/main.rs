mod structs;
mod utils;
mod alerts;

use std::{
    sync::Arc,
    sync::Mutex,
    thread,
    error::Error,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let nb_workers = 10;
    let max_queue_length = 1000;

    // parsed config file
    let conf = utils::build_config("./config.yaml")?;
    
    // location of alert files
    let alert_path = String::from("./data/sample_alerts");

    // create the alerts queue
    let queue = Arc::new(Mutex::new(Vec::new()));

    // fire and forget the threaded workers
    for _ in 0..nb_workers {
        let queue = Arc::clone(&queue);
        let conf = conf.clone();
        thread::spawn(move || {
            let _ = tokio::runtime::Runtime::new().unwrap().block_on(alerts::worker(conf, queue));
        });
    }

    let now = std::time::Instant::now();

    let _ = alerts::process_files(alert_path, Arc::clone(&queue), max_queue_length).await;

    println!("all files processed in {}s, stopping the app", now.elapsed().as_secs_f64());
    Ok(())
}
