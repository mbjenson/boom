use core::time;
use std::env;
use std::collections::HashMap;
use bytes::Bytes;

use zeromq::{RouterSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

fn get_uri() -> String {
    let host = env::var("ZEROMQ_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("ZEROMQ_PORT").unwrap_or_else(|_| "5555".to_string());
    format!("tcp://{}:{}", host, port)
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Message {
    msgtype: String,
    data: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct MessageWithId {
    #[serde(with = "serde_bytes")]
    id: Vec<u8>,
    message: Message,
}

impl MessageWithId {
    fn new(zmq_message: ZmqMessage) -> Self {
        // the id is at index 0
        // everything else is the message
        let id = zmq_message.get(0).unwrap().to_vec();
        let decoded_message = zmq_message.get(1).unwrap();
        let serde_message: Message = serde_json::from_slice(&decoded_message).unwrap();
        Self {
            id,
            message: serde_message,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
struct Job {
    id: String,
    data: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct JobDone {
    id: String,
    result: String,
}

// JobZmq heritates from ZmqMessage. Takes a Job as an input, and creates a ZmqMessage with the worker_id as the first part of the message, and the Job as the second part of the message.
fn job_to_zmq_msg(worker_id: &Vec<u8>, job: &Job) -> ZmqMessage {
    let message = Message {
        msgtype: "job".into(),
        data: serde_json::to_string(&job).unwrap(),
    };
    let mut message = ZmqMessage::from(serde_json::to_string(&message).unwrap());
    // conver the String worker_id to a Vec<u8> and then to a Bytes
    message.push_front(Bytes::from(worker_id.clone()));
    message
}

async fn find_next_worker_id(jobs: &mut HashMap<Vec<u8>, Vec<Job>>, socket: &mut RouterSocket) -> Vec<u8> {
    // poll the socket for messages
    let mut next_worker_id = Vec::new();
    while next_worker_id.is_empty() {
        // wait maximum 1 second for a message. To do so, wrap the recv() call in a tokio::time::timeout()
        // let message = tokio::time::timeout(time::Duration::from_secs(1), socket.recv()).await;
        // if message.is_err() {
        //     // if the message is an error, ignore it and continue the loop
        //     println!("No message received");
        //     continue;
        // }
        if let Ok(message) = tokio::time::timeout(time::Duration::from_millis(10), socket.recv()).await {
            if message.is_err() {
                // if the message is an error, ignore it and continue the loop
                // println!("No message received");
                continue;
            }
            let message_with_id = MessageWithId::new(message.unwrap());
            let msgtype = message_with_id.message.msgtype;
            // msg is of type "auth", add the worker id to the hashmap
            if msgtype == "auth" {
                let worker_id = message_with_id.id.clone();
                jobs.insert(worker_id.clone(), Vec::new());
                println!("Worker with id {:?} authenticated", worker_id);
            } else if msgtype == "job_done" {
                let jobdone: JobDone = serde_json::from_str(&message_with_id.message.data).unwrap();
                // remove the job from the worker's list of jobs using its id
                let worker_id = message_with_id.id.clone();
                // if the worker_id is not in the hashmap, ignore it but print the warning
                if !jobs.contains_key(&worker_id) {
                    println!("Unknown worker with id {:?} tried to send a job_done message", worker_id);
                    continue;
                } else {
                    println!("Worker with id {:?} sent a job_done message", worker_id);
                }
                let worker_jobs = jobs.get_mut(&worker_id).unwrap();
                // let job_index = worker_jobs.iter().position(|j| j.id == jobdone.id).unwrap();
                // the above does not handle the case where we cannot find the job
                let job_index = worker_jobs.iter().position(|j| j.id == jobdone.id);
                if job_index.is_none() {
                    println!("Unknown job with id {} from worker with id {:?}", jobdone.id, worker_id);
                    // print all the ids of the jobs in the worker's list
                    println!("Jobs in worker with id {:?}: {:?}", worker_id, worker_jobs.iter().map(|j| j.id.clone()).collect::<Vec<String>>());
                    continue;
                }
                worker_jobs.remove(job_index.unwrap());
            } else {
                println!("Unknown message type: {}", msgtype);
            }
        }
        // } else {
        //     // println!("No message received");
        // }
        // now that we've updated the jobs hashmap, look for a worker that has less than 10 jobs
        // find the worker with the least number of jobs
        let next_worker_id_options: Vec<&Vec<u8>> = jobs.iter().filter(|(_, v)| v.len() < 10).map(|(k, _)| k).collect();
        if next_worker_id_options.is_empty() {
            // println!("No worker available, waiting for a worker to authenticate");
            continue;
        }
        next_worker_id = next_worker_id_options[0].clone();
    }
    next_worker_id
}

#[tokio::main]
async fn main() {
    let uri = get_uri();
    println!("Connecting to {}", uri);

    let mut socket = RouterSocket::new();
    socket.bind(&uri).await.unwrap();

    // create a hashmap from id (of type [u8; 16]) to jobs (of type Vec<Job>)
    let mut jobs: HashMap<Vec<u8>, Vec<Job>> = HashMap::new();

    // in a loop, send a job to the worker every N seconds
    let mut counter = 0;
    loop {
        // send a message to the router
        let worker_id = find_next_worker_id(&mut jobs, &mut socket).await;
        let job = Job {
            id: counter.to_string(),
            data: "Hello, World!".into(),
        };
        // socket.send(job_to_zmq_msg(&worker_id, &job)).await.unwrap();
        // the above could error out, so run it but catch the error
        if let Err(e) = socket.send(job_to_zmq_msg(&worker_id, &job)).await {
            println!("Error sending job: {:?}", e);
            // if the error is like "Destination client not found by identity", remove the worker from the jobs hashmap
            if e.to_string().contains("Destination client not found by identity") {
                let pending_jobs = jobs.remove(&worker_id).unwrap();
                println!("Removed worker with id {:?}, lost {} jobs", worker_id, pending_jobs.len());
            }
            continue;
        }
        // add it to the worker's list of jobs
        let worker_jobs = jobs.get_mut(&worker_id).unwrap();
        worker_jobs.push(job.clone());
        counter += 1;
    }

}