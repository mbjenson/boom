use std::process;
use tokio::time;

use redis::AsyncCommands;

use std::{
    error::Error,
    sync::{Arc, Mutex},
    collections::HashMap,
};
use nix;

use boom::worker_util;

/*
> multi-worker-manager
spawn and kill workers based on throughput

>> determining throughput
Manager will get length of redis queue and use that metric to 
determine worker need. Use llen to get length of redis queue.
I suspect an average should be taken, over a certain period of time,
of the incoming alerts against the outgoing alerts for a stage of
the pipeline. I think this will give an accurate representation of
how the workers in that stage might need to be scaled.

>>> deteremining worker need for alert_worker
The manager might get the length of the `<stream_name>_alerts_packet_queue` 
and compare it with `<stream_name>_alerts_classifier_queue` to get an
accurate representation of the current incoming data load and if
the workers are able to deal with it properly

>>> Determining worker need for ml_worker
the manager can read from the `<stream_name>_alerts_classifier_queue` to see
alerts coming into ml_worker. Since the ml worker does not put things into queues
and instead uses streams, the previous approach cannnot be used. (1) Instead we could
have the ML worker dump some kind of metric into a file which can be read to determine
data throughput and adjust the worker count accordingly. Or (2) the ml_workers
outputted data streams could be queried for data throughput metrics somehow.

> Additions that need to be made
1. add a place to display how many workers are currently active and of which types

*/


// total_outgoing += con.llen::<&str, isize>("ZTF_alerts_classifier_queue").await.unwrap() as i64;


// send 
async fn exit_workers(workers: &mut HashMap<(), std::process::Child>) -> Result<(), Box<dyn Error>> {
    for worker in workers.values_mut() {
        worker.kill().expect("worker could not be killed")
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let interrupt = Arc::new(Mutex::new(false));
    worker_util::sig_int_handler(Arc::clone(&interrupt), "manager".to_string()).await;
    // REDIS
    let client_redis = redis::Client::open(
        "redis://localhost:6379".to_string()
    ).unwrap();
    let mut con = client_redis.get_multiplexed_async_connection().await.unwrap();

    let mut worker_table: HashMap<&str, Vec<process::Child>> = HashMap::from([
        ("alert", Vec::new()),
        ("ml", Vec::new()),
        ("filter", Vec::new())
    ]);
    // let alert_worker = process::Command::new("./target/debug/alert_worker")
    //     .arg("ZTF")
    //     .spawn()
    //     .expect("failed to start alert worker");
    // worker_table.entry("alert").and_modify(|workers| workers.push(alert_worker));
    
    // let ml_worker = process::Command::new("python ./py_workers/ml_worker.py")
    //     .spawn()
    //     .expect("failed to start ml_worker");
    // worker_table.entry("ml").and_modify(|workers| workers.push(ml_worker));

    let filter_worker = process::Command::new("./target/debug/filter_worker")
        .args(["2", "3", "4"])
        .spawn()
        .expect("failed to start filter worker");
    worker_table.entry("filter").and_modify(|workers| workers.push(filter_worker));
    
    loop {
        // match interrupt.try_lock() {
        //     Ok(flag) => {
        //         if *flag {
        //             exit_children()
        //         }
        //     },
        //     _ => {}
        // }

        
        
        match interrupt.try_lock() {
            Ok(stop) => {
                if *stop {
                    for (_, worker_vec) in worker_table {
                        
                        for worker in worker_vec {
                            nix::sys::signal::kill(
                                nix::unistd::Pid::from_raw(worker.id() as i32),
                                nix::sys::signal::Signal::SIGINT
                            ).expect("cannot send ctrl-c");
                            // worker.kill().unwrap();
                        }
                    }
                    return Ok(());
                }
            },
            _ => {}
        };
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        // start one of each worker as a child process
    }
}