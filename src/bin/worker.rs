use std::env;

use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

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

fn do_work(job_id: String, data: String) -> String {
    println!("Working on job {} with data: {}", job_id, data);
    // simulate some work
    std::thread::sleep(std::time::Duration::from_secs(1));
    // return a fake result, like Hello back!
    "Hello, back!".into()
}

#[tokio::main]
async fn main() {
    let uri = get_uri();
    println!("Connecting to {}", uri);

    let mut socket = DealerSocket::new();
    socket.connect(&uri).await.unwrap();

    let message = Message {
        msgtype: "auth".into(),
        data: "Hello, World!".into(),
    };
    let message = ZmqMessage::from(serde_json::to_string(&message).unwrap());

    println!("Sending message: {:?}", message);
    socket.send(message).await.unwrap();

    loop {
        // receive the message
        let message = socket.recv().await.unwrap();
        let decoded_message: Message = serde_json::from_slice(message.get(0).unwrap()).unwrap();

        let msgtype = decoded_message.msgtype;
        if msgtype == "job" {
            let job: Job = serde_json::from_str(&decoded_message.data).unwrap();
            // do the work
            let result = do_work(job.id.clone(), job.data);
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

}