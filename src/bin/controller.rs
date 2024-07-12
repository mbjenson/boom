use std::collections::HashMap;
use std::error::Error;
use zeromq::{self, Socket, SocketRecv, SocketSend, ZmqMessage};

struct Job {
    id: String,
    payload: String,
}

// create the job by passing it a payload, and it will generate a unique id
impl Job {
    fn new(payload: &str) -> Job {
        Job {
            id: uuid::Uuid::new_v4().to_string(),
            payload: payload.to_string(),
        }
    }
}

struct Worker {
    id: String,
    jobs: Vec<Job>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct WorkerMessage {
    message: String,
    result: String,
    // optional job_id
    job_id: Option<String>,
}

struct Controller {
    socket : zeromq::RouterSocket,
    workers: HashMap<String, Worker>,
    max_jobs_per_worker: usize,
}

// Now, to track our workers and do some basic load balancing
// we’ll add a method that returns the id of the next available
// worker (the next step will be to handle worker registration
// and disconnection, but that’s a bit more involved
// so we’ll get this out of the way first):
impl Controller {
    fn next_worker(&mut self) -> Option<String> {
        // sort the workers by the number of jobs they have (ascending)
        let mut workers = self
            .workers
            .values()
            .collect::<Vec<&Worker>>();
        workers.sort_by_key(|w| w.jobs.len());

        // return the id of the first worker if it has less than the max jobs
        if let Some(worker) = workers.first() {
            if worker.jobs.len() < self.max_jobs_per_worker {
                return Some(worker.id.clone());
            }
        }
        // otherwise, return None
        None
    }

    // add a msg that handles receiving messages on the socket
    // - connect msgs from workers
    // - disconnect msgs from workers
    // - job completion msgs from workers
    fn handle_worker_msg(&mut self, worker_id: String, msg: WorkerMessage) {
        match msg.message.as_str() {
            "connect" => {
                // verify that the worker is not already connected
                if self.workers.contains_key(&worker_id) {
                    println!("Worker {} already connected", worker_id);
                    return;
                }
                self.workers.insert(
                    worker_id.clone(),
                    Worker {
                        id: worker_id.clone(),
                        jobs: Vec::new(),
                    },
                );
                println!("Worker {} connected", worker_id);
            }
            "disconnect" => {
                // verify that the worker is connected
                if !self.workers.contains_key(&worker_id) {
                    println!("Worker {} not connected", worker_id);
                    return;
                }
                self.workers.remove(&worker_id);
                println!("Worker {} disconnected", worker_id);
            }
            "job_done" => {
                if let Some(worker) = self.workers.get_mut(&worker_id) {
                    if let Some(job_id) = msg.job_id {
                        if let Some(job) = worker.jobs.iter().position(|j| j.id == job_id) {
                            worker.jobs.remove(job);
                            println!("Worker {} completed job {}", worker_id, job_id);
                        }
                    }
                } else {
                    println!("Worker {} with job_done is not connected", worker_id);
                }
            }
            _ => {
                println!("Unknown message from worker {}: {}", worker_id, msg.message);
            }
        }
    }

    // implement the "run" main method, that in a loop of 1000 items will:
    // - open another loop to wait for a free worker by polling the socket
    // - call the "handle_worker_msg" method to handle the message

    async fn run(&mut self) {
    // to simulare the job we use an iterator that returns integers from 0 to 1000
        for i in 0..1000 {
            let job = Job::new(&i.to_string());
            let mut next_worker_id = None;
            loop {
                let mut worker_id = None;
                while let Ok(msg) = self.socket.recv().await {
                    let worker_id = msg.get(0).unwrap().to_vec();
                    let worker_id = String::from_utf8(worker_id).unwrap();
                    let _ = msg.get(1).unwrap().to_vec();
                    let msg = msg.get(2).unwrap().to_vec();
                    let msg = String::from_utf8(msg).unwrap();
                    let msg: Message = serde_json::from_str(&msg).unwrap();
                    self.handle_worker_msg(worker_id, msg);
                }
                next_worker_id = self.next_worker();
                // if there is no worker available, wait for 100ms
                if next_worker_id.is_none() {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    continue;
                } else{
                    break;
                }
            }
            // if we have a job and a next_worker_id, send the job to the worker
            if next_worker_id.is_some() {
                let worker_id = next_worker_id.unwrap();
                let worker = self.workers.get_mut(&worker_id).unwrap();
                worker.jobs.push(job);
                let msg = Message {
                    message: "job".to_string(),
                    result: job.payload,
                    job_id: Some(job.id),
                };
                let zmq_msg = ZmqMessage::from(serde_json::to_string(&msg).unwrap());
                // send takes a zeromq message as input
                self.socket.send(zmq_msg).await.unwrap();
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut controller = Controller {
        socket: zeromq::RouterSocket::new(),
        workers: HashMap::new(),
        max_jobs_per_worker: 10,
    };

    controller.socket.bind("tcp://127.0.0.1:5559").await?;

    controller.run();

    Ok(())
}