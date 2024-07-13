use std::env;

use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

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
        let filename = logdir.join("worker.log");
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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Message {
    msgtype: String,
    data: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Job {
    id: String,
    data: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct JobDone {
    id: String,
    result: String,
}

// with tracing, log every time we do work, logging the job_id and data
#[tracing::instrument(
    name = "do_work",
    skip(job_id, data),
    fields(job_id = %job_id)
)]
async fn do_work(job_id: String, data: String) -> String {
    // simulate some work
    // check if the data is a non empty string
    if data.is_empty() {
        tracing::warn!("Data is empty");
    }
    // randomize a sleep between 50 and 1000 milliseconds
    std::thread::sleep(std::time::Duration::from_millis(rand::random::<u64>() % 950 + 50));
    tracing::info!("Completed");
    // return a fake result, like Hello back!
    "Hello, back!".into()
}

#[tracing::instrument(
    name = "handle_queue_msg",
    skip(message, socket),
    fields(msgtype = %message.msgtype)
)]
async fn handle_queue_msg(message: Message, socket: &mut DealerSocket) {
    let msgtype = message.msgtype;
    if msgtype == "job" {
        let job: Job = serde_json::from_str(&message.data).unwrap();
        tracing::info!("Received job: {}", job.id);
        // do the work
        let result = do_work(job.id.clone(), job.data).await;
        let job_done = JobDone {
            id: job.id,
            result,
        };
        let message = Message {
            msgtype: "job_done".into(),
            data: serde_json::to_string(&job_done).unwrap(),
        };
        let message = ZmqMessage::from(serde_json::to_string(&message).unwrap());
        socket.send(message).await.unwrap();
    }
}

#[tokio::main]
async fn main() {
    let subscriber = create_logger();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let uri = get_uri();

    let mut socket = DealerSocket::new();
    socket.connect(&uri).await.unwrap();

    let message = Message {
        msgtype: "auth".into(),
        data: "Hello, World!".into(),
    };
    let message = ZmqMessage::from(serde_json::to_string(&message).unwrap());
    socket.send(message).await.unwrap();

    loop {
        // receive the message
        let message = socket.recv().await.unwrap();
        let decoded_message: Message = serde_json::from_slice(message.get(0).unwrap()).unwrap();

        handle_queue_msg(decoded_message, &mut socket).await;
    }

}