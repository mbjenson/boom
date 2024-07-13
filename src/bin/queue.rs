use core::time;
use std::env;
use std::collections::HashMap;
use bytes::Bytes;
use base64::{engine::general_purpose, Engine as _};

use zeromq::{RouterSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

fn create_logger() -> impl tracing::Subscriber {
    // create a log directory if it does not exist
    let basedir = std::env::current_dir().unwrap();
    let logdir = basedir.join("log");
    if !logdir.exists() {
        std::fs::create_dir(&logdir).unwrap();
    }
    // Start configuring a `fmt` subscriber
    let subscriber = tracing_subscriber::fmt()
    // Use a more compact, abbreviated log format
    .compact()
    // Display source code file paths
    .with_file(false)
    // Display source code line numbers
    .with_line_number(false)
    // Display the thread ID an event was recorded on
    .with_thread_ids(false)
    // Don't display the event's target (module path)
    .with_target(false)
    // log to a file called `worker.log`
    .with_writer(move || {
        let filename = logdir.join("queue.log");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(filename)
            .unwrap();
        file
    })
    // Build the subscriber
    .finish();
    subscriber
}

fn get_uri() -> String {
    let host = env::var("ZEROMQ_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("ZEROMQ_PORT").unwrap_or_else(|_| "5555".to_string());
    format!("tcp://{}:{}", host, port)
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
struct Message {
    msgtype: String,
    data: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
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

#[tracing::instrument(
    name = "handle_job_done",
    skip(jobs, worker_id, jobdone),
    fields(worker_id = general_purpose::STANDARD.encode(&worker_id), job_id = %jobdone.id)
)]
async fn handle_job_done(jobs: &mut HashMap<Vec<u8>, Vec<Job>>, worker_id: Vec<u8>, jobdone: JobDone) {
    // remove the job from the worker's list of jobs using its id
    // if the worker_id is not in the hashmap, ignore it but print the warning
    if !jobs.contains_key(&worker_id) {
        tracing::info!("Unknown worker tried to send a job_done message");
        return;
    }
    let worker_jobs = jobs.get_mut(&worker_id).unwrap();
    // the above does not handle the case where we cannot find the job
    let job_index = worker_jobs.iter().position(|j| j.id == jobdone.id);
    if job_index.is_none() {
        tracing::info!("Unknown job with id {}", jobdone.id);
        return;
    }
    worker_jobs.remove(job_index.unwrap());
    tracing::info!("Job done.");
}

#[tracing::instrument(
    name = "handle_worker_msg",
    skip(jobs, message_with_id),
    fields(worker_id = general_purpose::STANDARD.encode(&message_with_id.id), msgtype = %message_with_id.message.msgtype)
)]
async fn handle_worker_msg(jobs: &mut HashMap<Vec<u8>, Vec<Job>>, message_with_id: &MessageWithId) {
    let worker_id = message_with_id.id.clone();
    let msgtype = message_with_id.message.msgtype.clone();
    if msgtype == "auth" {
        let worker_id = message_with_id.id.clone();
        jobs.insert(worker_id.clone(), Vec::new());
        tracing::info!("Worker authenticated");
    } else if msgtype == "job_done" {
        if message_with_id.message.data.is_empty() {
            tracing::info!("Empty job_done message");
            return;
        }
        let jobdone: JobDone = serde_json::from_str(&message_with_id.message.data).unwrap();
        handle_job_done(jobs, worker_id, jobdone).await;
    } else {
        tracing::warn!("Unknown message type");
    }
}

#[tracing::instrument(
    name = "find_next_worker_id",
    skip(jobs, socket),
)]
async fn find_next_worker_id(jobs: &mut HashMap<Vec<u8>, Vec<Job>>, socket: &mut RouterSocket) -> Vec<u8> {
    // poll the socket for messages
    let mut next_worker_id = Vec::new();
    while next_worker_id.is_empty() {
        // wait maximum 1 second for a message. To do so, wrap the recv() call in a tokio::time::timeout()
        if let Ok(message) = tokio::time::timeout(time::Duration::from_millis(10), socket.recv()).await {
            if message.is_err() {
                continue;
            }
            let message_with_id = MessageWithId::new(message.unwrap());
            handle_worker_msg(jobs, &message_with_id).await;
        }
        // now that we've updated the jobs hashmap, look for a worker that has less than 10 jobs
        // find the worker with the least number of jobs
        let next_worker_id_options: Vec<&Vec<u8>> = jobs.iter().filter(|(_, v)| v.len() < 10).map(|(k, _)| k).collect();
        if next_worker_id_options.is_empty() {
            continue;
        }
        next_worker_id = next_worker_id_options[0].clone();
    }
    next_worker_id
}

// worker_id is a reference to a Vec<u8> which does not implement display, so we convert it to a String ot the bytes (base64 not utf-8)
#[tracing::instrument(
    name = "send_job_to_worker",
    skip(worker_id, job, socket, jobs),
    fields(worker_id = general_purpose::STANDARD.encode(&worker_id), job_id = %job.id),
)]
async fn send_job_to_worker(worker_id: &Vec<u8>, job: &Job, socket: &mut RouterSocket, jobs: &mut HashMap<Vec<u8>, Vec<Job>>) {
    // send a message to the router
    // socket.send(job_to_zmq_msg(&worker_id, &job)).await.unwrap();
    // the above could error out, so run it but catch the error
    if let Err(e) = socket.send(job_to_zmq_msg(&worker_id, &job)).await {
        tracing::error!("Error sending job: {:?}", e);
        // if the error is like "Destination client not found by identity", remove the worker from the jobs hashmap
        if e.to_string().contains("Destination client not found by identity") {
            tracing::warn!("Worker not found, removing it from the list of workers ({} pending jobs were lost)", jobs.get(&worker_id.clone()).unwrap().len());
        }
        return;
    }
    // add the job to the worker's list of jobs
    // add it to the worker's list of jobs
    let worker_jobs = jobs.get_mut(&worker_id.clone()).unwrap();
    worker_jobs.push(job.clone());
    tracing::info!("Job sent to worker");
}

async fn run () {
    let uri = get_uri();
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
        send_job_to_worker(&worker_id, &job, &mut socket, &mut jobs).await;
        counter += 1;
    }
}

#[tokio::main]
async fn main() {
    let subscriber = create_logger();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    run().await;
}