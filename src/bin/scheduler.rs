use std::{
    borrow::Borrow, collections::HashMap, fmt, sync::{mpsc, Arc, Mutex}, thread::{self, JoinHandle}
};
use boom::{types::Alert, worker_util};


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


// each type of worker has it's own thread pool
pub struct ThreadPool {
    pub worker_type: WorkerType,
    pub workers: HashMap<String, Worker>,
    pub senders: HashMap<String, Option<mpsc::Sender<Message>>>,
}

pub type Message = WorkerCmd;

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
    fn new(worker_type: WorkerType, id: String, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let id_copy = id.clone();
        let thread = match worker_type {
            WorkerType::Alert => {
                thread::spawn(move || {
                    fake_alert_worker(id, receiver);
                })
            },
            WorkerType::Filter => {
                thread::spawn(move || {
                    fake_filter_worker(id, receiver);
                })
            },
            WorkerType::ML => {
                thread::spawn(move || {
                    fake_ml_worker(id, receiver);
                })
            },
            _ => {
                thread::spawn(move || loop {
                    println!("worker type not yet implemented");
                    thread::sleep(std::time::Duration::from_secs(1));
                })
            }
        };

        Worker {
            id: id_copy,
            thread: Some(thread),
        }
    }
}

///
/// FAKE WORKERS
/// 

// TODO: figure out a way to have shared functionality between workers
//      for things like responding to worker commands, etc.


fn fake_alert_worker(id: String, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) {
    println!("created alert_worker with id {}", id);
    loop {
        if let Ok(message) = receiver.lock().unwrap().try_recv() {
            match message {
                WorkerCmd::TERM => {
                    println!("received WorkerCmd {}, quitting", message);
                    return;
                }
            }
        }
        println!("processing an alert");
        thread::sleep(std::time::Duration::from_secs(1));
    }
}

// note: all decisions about which filters to run are done within the filter worker in 
// communication with the database, not in communication with the thread pool
fn fake_filter_worker(id: String, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) {
    println!("created a filter_worker with id {}", id);
    let filter_id = 222;
    loop {
        if let Ok(message) = receiver.lock().unwrap().try_recv() {
            match message {
                WorkerCmd::TERM => {
                    println!("received WorkerCmd {}, quitting", message);
                    return;
                }
            }
        }
        println!("running filter {} on alerts", filter_id);
        thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn fake_ml_worker(id: String, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) {
    println!("created a ml_worker with id {}", id);
    loop {
        if let Ok(message) = receiver.lock().unwrap().try_recv() {
            match message {
                WorkerCmd::TERM => {
                    println!("received WorkerCmd {}, quitting", message);
                    return;
                }
            }
        }
        println!("running ml on the alerts");
        thread::sleep(std::time::Duration::from_secs(1));
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
    
    // let mut pool = ThreadPool::new(1);
    let mut alert_pool = ThreadPool::new(WorkerType::Alert, 1);
    let mut filter_pool = ThreadPool::new(WorkerType::Filter, 1);
    let mut ml_pool = ThreadPool::new(WorkerType::ML, 1);

    let mut count = 0;
    loop {
        let exit = worker_util::check_flag(Arc::clone(&interrupt));
        println!("heart beat");
        // sleep for 1 second
        thread::sleep(std::time::Duration::from_secs(2));
        print_pool(&alert_pool);
        print_pool(&filter_pool);
        print_pool(&ml_pool);
        // count += 1;
        // if count == 5 || exit {
            // break;
        // }
        if exit {
            break;
        }
    }

    // get the id of the first worker in the pool
    // let id = pool.senders.keys().next().unwrap().clone();

    // shutdown worker with id
    // pool.remove_worker(id);

    // add another worker
    // pool.add_worker();

    // kill all workers in the pools
    drop(alert_pool);
    drop(filter_pool);
    drop(ml_pool);

    // sleep for 1 seconds
    thread::sleep(std::time::Duration::from_secs(1));
}