use std::{
    collections::HashMap,
    sync::{mpsc, Arc, Mutex}, 
    thread,
};
use crate::{worker_util::{WorkerType, WorkerCmd}, alert_worker, filter_worker, fake_ml_worker};

// Thread pool
// allows spawning, killing, and managing of various worker threads through
// the use of a messages
pub struct ThreadPool {
    pub worker_type: WorkerType,
    pub stream_name: String,
    pub config_path: String,
    pub workers: HashMap<String, Worker>,
    pub senders: HashMap<String, Option<mpsc::Sender<WorkerCmd>>>,
}

impl ThreadPool {
    pub fn new(
        worker_type: WorkerType, 
        size: usize, 
        stream_name: String,
        config_path: String
    ) -> ThreadPool {
        assert!(size > 0);

        let mut workers = HashMap::new();
        let mut senders = HashMap::new();

        for _ in 0..size {
            let id = uuid::Uuid::new_v4().to_string();
            let (sender, receiver) = mpsc::channel();
            let receiver = Arc::new(Mutex::new(receiver));
            workers.insert(id.clone(), Worker::new(
                worker_type, 
                id.clone(), 
                Arc::clone(&receiver),
                stream_name.clone(),
                config_path.clone()
            ));
            senders.insert(id.clone(), Some(sender));
        }

        ThreadPool {
            worker_type,
            stream_name,
            config_path,
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
            Worker::new(
                self.worker_type, id.clone(), 
                Arc::clone(&receiver),
                self.stream_name.clone(),
                self.config_path.clone(),
            ));
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
        worker_type: WorkerType, 
        id: String, 
        receiver: Arc<Mutex<mpsc::Receiver<WorkerCmd>>>,
        stream_name: String,
        config_path: String
    ) -> Worker {
        let id_copy = id.clone();
        let thread = match worker_type {
            WorkerType::Alert => {
                thread::spawn(|| {
                    alert_worker::alert_worker(id, receiver, stream_name, config_path);
                })
            },
            WorkerType::Filter => {
                thread::spawn(|| {
                    filter_worker::filter_worker(id, receiver, stream_name, config_path);
                })
            },
            WorkerType::ML => {
                thread::spawn(|| {
                    fake_ml_worker::fake_ml_worker(id, receiver, stream_name, config_path);
                })
            }
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
