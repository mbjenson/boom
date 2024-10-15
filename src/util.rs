use std::sync::{Mutex, Arc};

// spawns a thread which listens for interrupt signal. Sets flag to true upon signal interruption
pub async fn sig_int_handler(flag: Arc<Mutex<bool>>) {
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        println!("Filter worker interrupted. Finishing up...");
        let mut flag = flag.try_lock().unwrap();
        *flag = true;
    });
}

// checks if a flag is set to true and, if so, exits the program
pub fn check_exit(flag: Arc<Mutex<bool>>) {
    match flag.try_lock() {
        Ok(x) => {
            if *x { std::process::exit(0) }
        },
        _ => {}
    }
}