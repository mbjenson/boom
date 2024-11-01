use std::{sync::{Arc, Mutex}, thread, env};
use boom::{worker_util::WorkerType, worker_util, scheduling::ThreadPool};

#[tokio::main]
async fn main() {
    // get env::args for stream_name and config_path
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        print!("usage: scheduler <stream_name> <config_path>, where `config_path` is optional");
        return;
    }

    let stream_name = args[1].to_string();
    let config_path = if args.len() > 2 {
        &args[2]
    } else {
        println!("No config file provided, using `config.yaml`");
        "./config.yaml"
    }.to_string();

    // setup signal handler thread
    let interrupt = Arc::new(Mutex::new(false));
    worker_util::sig_int_handler(Arc::clone(&interrupt)).await;
    
    println!("creating alert, ml, and filter workers...");
    // note: maybe use &str instead of String for stream_name and config_path to reduce clone calls
    let alert_pool = ThreadPool::new(WorkerType::Alert, 3, stream_name.clone(), config_path.clone());
    let ml_pool = ThreadPool::new(WorkerType::ML, 3, stream_name.clone(), config_path.clone());
    let filter_pool = ThreadPool::new(WorkerType::Filter, 3, stream_name.clone(), config_path.clone());
    println!("created workers");

    loop {
        let exit = worker_util::check_flag(Arc::clone(&interrupt));
        println!("heart beat (MAIN)");

        // sleep for 1 second
        thread::sleep(std::time::Duration::from_secs(1));
        if exit {
            println!("killed thread(s)");
            drop(alert_pool);
            drop(ml_pool);
            drop(filter_pool);
            break;
        }
    }
    println!("reached the end sir");
    std::process::exit(0);
}
