use std::{
    borrow::Borrow, collections::HashMap, fmt, sync::{mpsc, Arc, Mutex}, thread::{self, JoinHandle}, error::Error,
};
use futures::executor;
use futures;
use boom::{types::Alert, worker_util};
use rdkafka::message::Header;
use tokio::{self, sync::{broadcast::error, watch::Receiver}};
use redis::AsyncCommands;
use std::env;
use boom::{conf, alert, types::ztf_alert_schema};


// alert worker as a standalone function which is run by the scheduler
#[tokio::main]
async fn alert_worker(id: String, receiver: Arc<Mutex<mpsc::Receiver<WorkerCmd>>>) {
    // let args: Vec<String> = env::args().collect();
    // user can pass the path to a config file, but it is optional.
    // if not provided, we use the default config.default.yaml
    // the user also needs to pass the name of the alert stream to process
    // stream name comes first, optional config file comes second
    // if args.len() < 2 {
    //     println!("Usage: alert_worker <stream_name> <config_file>, where config_file is optional");
    //     return;
    // }

    // let interrupt_flag = Arc::new(Mutex::new(false));
    // worker_util::sig_int_handler(Arc::clone(&interrupt_flag)).await;

    // let stream_name = &args[1];

    // let config_file = if args.len() > 2 {
    //     conf::load_config(&args[2]).unwrap()
    // } else {
    //     println!("No config file provided, using config.yaml");
    //     conf::load_config("./config.yaml").unwrap()
    // };
    

    let config_file = conf::load_config("./config.yaml").unwrap();
    let stream_name = "ZTF";

    // XMATCH CONFIGS
    let xmatch_configs = conf::build_xmatch_configs(&config_file, stream_name);

    // DATABASE
    let db: mongodb::Database = conf::build_db(&config_file).await;
    if let Err(e) = db.list_collection_names().await {
        println!("Error connecting to the database: {}", e);
        return;
    }

    let alert_collection = db.collection(&format!("{}_alerts", stream_name));
    let alert_aux_collection = db.collection(&format!("{}_alerts_aux", stream_name));

    // create index for alert collection
    let alert_candid_index = mongodb::IndexModel::builder()
        .keys(mongodb::bson::doc! { "candid": -1 })
        .options(mongodb::options::IndexOptions::builder().unique(true).build())
        .build();
    match alert_collection.create_index(alert_candid_index).await {
        Err(e) => {
            println!("Error when creating index for candidate.candid in collection {}: {}", 
                format!("{}_alerts", stream_name), e);
        },
        Ok(_x) => {}
    }

    // REDIS
    let client_redis = redis::Client::open(
        "redis://localhost:6379".to_string()
    ).unwrap();
    let mut con = client_redis.get_multiplexed_async_connection().await.unwrap();
    let queue_name = format!("{}_alerts_packet_queue", stream_name);
    let queue_temp_name = format!("{}_alerts_packet_queuetemp", stream_name);
    let classifer_queue_name = format!("{}_alerts_classifier_queue", stream_name);

    // ALERT SCHEMA (for fast avro decoding)
    let schema = ztf_alert_schema().unwrap();
    let mut count = 0;
    let start = std::time::Instant::now();
    loop {
        // check for command from threadpool
        if let Ok(command) = receiver.lock().unwrap().try_recv() {
            match command {
                WorkerCmd::TERM => {
                    println!("alert worker {} received termination command", id);
                    return;
                },
            }
        }
        // retrieve candids from redis
        let result: Option<Vec<Vec<u8>>> = con.rpoplpush(&queue_name, &queue_temp_name).await.unwrap();
        match result {
            Some(value) => {
                let candid = alert::process_alert(value[0].clone(), &xmatch_configs, &db, &alert_collection, &alert_aux_collection, &schema).await;
                match candid {
                    Ok(Some(candid)) => {
                        println!("Processed alert with candid: {}, queueing for classification", candid);
                        // queue the candid for processing by the classifier
                        con.lpush::<&str, i64, isize>(&classifer_queue_name, candid).await.unwrap();
                        con.lrem::<&str, Vec<u8>, isize>(&queue_temp_name, 1, value[0].clone()).await.unwrap();
                    }
                    Ok(None) => {
                        println!("Alert already exists");
                        // remove the alert from the queue
                        con.lrem::<&str, Vec<u8>, isize>(&queue_temp_name, 1, value[0].clone()).await.unwrap();
                    }
                    Err(e) => {
                        println!("Error processing alert: {}, requeueing", e);
                        // put it back in the alertpacketqueue, to the left (pop from the right, push to the left)
                        con.lrem::<&str, Vec<u8>, isize>(&queue_temp_name, 1, value[0].clone()).await.unwrap();
                        con.lpush::<&str, Vec<u8>, isize>(&queue_name, value[0].clone()).await.unwrap();
                    }
                }
                if count > 1 && count % 100 == 0 {
                    let elapsed = start.elapsed().as_secs();
                    println!("\nProcessed {} {} alerts in {} seconds, avg: {:.4} alerts/s\n", count, stream_name, elapsed, count as f64 / elapsed as f64);
                }
                count += 1;
            }
            None => {
                println!("Queue is empty");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }

}

#[derive(Debug, PartialEq, Eq)]
pub enum WorkerCmd {
    TERM,
}

impl fmt::Display for WorkerCmd {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let enum_str;
        match self {
            WorkerCmd::TERM => { enum_str = "TERM"; },
            _ => { enum_str = "'display not implemented for this WorkerCmd type'"; }
        }
        write!(f, "{}", enum_str)
    }
}


#[derive(Clone, Debug)]
pub enum WorkerType {
    Alert,
    Filter,
    ML,
}

impl Copy for WorkerType {}

impl fmt::Display for WorkerType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let enum_str;
        match self {
            WorkerType::Alert => { enum_str = "Alert"; },
            WorkerType::Filter => { enum_str = "Filter" },
            WorkerType::ML => { enum_str = "ML" },
            _ => { enum_str = "'display not implemented for this worker type'"; }
        }
        write!(f, "{}", enum_str)
    }
}


// Thread pool
// allows spawning, killing, and managing of various worker threads through
// the use of a messages
pub struct ThreadPool {
    pub worker_type: WorkerType,
    pub workers: HashMap<String, Worker>,
    pub senders: HashMap<String, Option<mpsc::Sender<WorkerCmd>>>,
}

// pub type Message = WorkerCmd;

impl ThreadPool {
    pub fn new(worker_type: WorkerType, size: usize) -> ThreadPool {
        assert!(size > 0);

        let mut workers = HashMap::new();
        let mut senders = HashMap::new();

        for _ in 0..size {
            let id = uuid::Uuid::new_v4().to_string();
            let (sender, receiver) = mpsc::channel();
            let receiver = Arc::new(Mutex::new(receiver));
            workers.insert(id.clone(), Worker::new(worker_type, id.clone(), Arc::clone(&receiver)));
            senders.insert(id.clone(), Some(sender));
        }

        ThreadPool {
            worker_type,
            workers,
            senders,
        }
    }

    pub fn remove_worker(&mut self, id: String) {
        if let Some(sender) = &self.senders[&id] {
            sender.send(WorkerCmd::TERM).unwrap();
            self.senders.remove(&id);

            // if let Some(worker) = self.workers.get_mut(&id) {
            //     if let Some(thread) = worker.thread.take() {
            //     thread.join().unwrap();
            // }

        }
    }

    pub fn add_worker(&mut self) {
        let id = uuid::Uuid::new_v4().to_string();
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        self.workers.insert(
            id.clone(),
            Worker::new(self.worker_type, id.clone(),
            Arc::clone(&receiver)));
        self.senders.insert(id.clone(), Some(sender));
        println!("Added worker with id: {}", &id);
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        println!("Sending terminate message to all workers.");

        // get the ids of all workers
        let ids: Vec<String> = self.senders.keys().cloned().collect();
        
        for id in ids {
            self.remove_worker(id);
        }

        println!("Shutting down all workers.");

        for (id, worker) in &mut self.workers {
            println!("Shutting down worker {}", &id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}
    

/*
Worker Struct
When creating a worker a `WorkerType` must be specified.
The worker of that type is then created. A worker will
run a specific function whichs is the worker.
*/

pub struct Worker {
    pub id: String,
    pub thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(
        worker_type: WorkerType, id: String, 
        receiver: Arc<Mutex<mpsc::Receiver<WorkerCmd>>>
    ) -> Worker {
        let id_copy = id.clone();
        let thread = match worker_type {
            WorkerType::Alert => {
                thread::spawn(|| {
                    alert_worker(id, receiver);
                })
            },
            _ => {
                panic!("worker type not yet implemnted");
            }
        };

        Worker {
            id: id_copy,
            thread: Some(thread),
        }
    }
}



// some pseudocode
/*
pools = define_pools()
while true {
    stats = evaluate_boom_statistics()

    / using boom io statistics, check for updates for the workers
    alert_pool.update_workers_based_on_stats(stats)
    filter_pool.update_workers_based_on_stats(stats)
    ml_pool.update_workers_based_on_stats(stats)

    / update the filters that are being used based on database table
    update_filters(filter_pool)
}   
*/


fn print_pool(pool: &ThreadPool) {
    println!("thread pool of type: {:?}", pool.worker_type);
    println!("num workers: {}", pool.workers.values().len());
}


#[tokio::main]
async fn main() {
    // setup signal handler thread
    let interrupt = Arc::new(Mutex::new(false));
    worker_util::sig_int_handler(Arc::clone(&interrupt)).await;
    
    let alert_pool = ThreadPool::new(WorkerType::Alert, 3);
    
    // println!("spawned in 1 worker of each type");
    loop {
        let exit = worker_util::check_flag(Arc::clone(&interrupt));
        println!("heart beat (MAIN)");

        // sleep for 1 second
        thread::sleep(std::time::Duration::from_secs(1));
        if exit {
            println!("killed thread(s)");
            drop(alert_pool);
            // let _ = v.into_iter().map(|x| x.join());
            break;
        }
    }
    println!("reached the end sir");

    // get the id of the first worker in the pool
    // let id = pool.senders.keys().next().unwrap().clone();

    // shutdown worker with id
    // pool.remove_worker(id);

    // add another worker
    // pool.add_worker();    

    // sleep for 1 seconds
    // thread::sleep(std::time::Duration::from_secs(1));

    std::process::exit(0);
}

// some notes about tokio tasks and other things

// BASIC
// let handle = thread::spawn(|| {
//     alert_worker();
// });

// GOOD
// let mut v = Vec::new();
// for i in 0..4 {
//     let handle = thread::spawn(|| {
//         alert_worker();
//     });
//     v.push(handle);
// }

// aborting threads in vec
// abort the threads
// let _= v.into_iter().map(|x| x.abort());
// let _ = futures::future::try_join_all(v.into_iter().map(tokio::spawn)).await;