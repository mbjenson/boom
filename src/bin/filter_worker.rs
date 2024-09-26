use redis::AsyncCommands;
use redis::streams::{StreamReadOptions,StreamReadReply};
use std::{
    env, 
    error::Error,
    sync::{Arc, Mutex},
    collections::HashMap,
};

use boom::{conf, filter};

async fn get_candids_from_stream(con: &mut redis::aio::MultiplexedConnection, stream: &str, options: &StreamReadOptions) -> Vec<i64> {
    let result: Option<StreamReadReply> = con.xread_options(
        &[stream.to_owned()], &[">"], options).await.unwrap();
    let mut candids: Vec<i64> = Vec::new();
    if let Some(reply) = result {
        for stream_key in reply.keys {
            let xread_ids = stream_key.ids;
            for stream_id in xread_ids {
                let candid = stream_id.map.get("candid").unwrap();
                // candid is a Value type, so we need to convert it to i64
                match candid {
                    redis::Value::BulkString(x) => {
                        // then x is a Vec<u8> type, so we need to convert it an i64
                        let x = String::from_utf8(x.to_vec()).unwrap().parse::<i64>().unwrap();
                        // append to candids
                        candids.push(x);
                    },
                    _ => {
                        println!("Candid unknown type: {:?}", candid);
                    }
                }
            }
        }
    }
    candids
}

// intercepts interrupt signals and sets boolean to true
async fn sig_int_handler(v: Arc<Mutex<bool>>) {
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        print!("Filter worker interrupted. Finishing up...");
        let mut v = v.try_lock().unwrap();
        *v = true;
    });
}

/*
    TODO: figure out how the streams work with permissions
        i.e. does the stream for permission level 3 contain everything in and under permission level 3
             or, does a stream for permission level 3 contain just the data which is specifically made
             for permission level 3.
*/

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    // let mut filter_id = 1;
    // get all the filter_ids
    let mut filter_ids: Vec<i32> = Vec::new();
    for i in 1..args.len() {
        filter_ids.push(args[i].parse::<i32>().unwrap());
    }
    println!("filter ids: {:?}", filter_ids);
    // if args.len() > 1 {
    //     for i in 0..args.len()
    //     filter_id = args[1].parse::<i32>().unwrap();
    // }

    // setup signal handler thread
    let interrupt = Arc::new(Mutex::new(false));
    sig_int_handler(Arc::clone(&interrupt)).await;

    // connect to mongo and redis
    let config_file = conf::load_config("./config.yaml").unwrap();
    let db = conf::build_db(&config_file, true).await;
    let client_redis = redis::Client::open(
        "redis://localhost:6379".to_string()).unwrap();
    let mut con = client_redis
        .get_multiplexed_async_connection().await.unwrap();

    // build filter(s)
    let mut filters: Vec<filter::Filter> = Vec::new();
    for id in filter_ids.clone() {
        let filter = filter::Filter::build(id, &db).await;
        match filter {
            Ok(filter) => {
                filters.push(filter);
            },
            Err(e) => {
                println!("got error when trying to build filter {}: {}", id, e);
                return Err(e);
            }
        }
    }
    println!("filters.len(): {}", filters.len());

    // build filter
    // let mut filter = match filter::Filter::build(filter_id, &db).await {
    //     Ok(filter) => {
    //         filter
    //     },
    //     Err(e) => {
    //         println!("got error: {}", e);
    //         return Err(e);
    //     }
    // };
    
    println!("Starting filter worker for filters {:?}", filter_ids);
    // println!("Starting filter worker for filter {}", filter_id);

    let mut redis_streams = HashMap::new();
    for filter in &filters {
        let stream = format!("{stream}_programid_{programid}_filter_stream",
            stream = filter.catalog, programid = filter.permissions.iter().max().unwrap());
        if !redis_streams.contains_key(filter.permissions.iter().max().unwrap()) {
            redis_streams.insert(filter.permissions.iter().max().unwrap().clone(), stream);
        }
    }
    println!("redis streams: {:?}", redis_streams);
    
    // let redis_stream = format!("{stream}_programid_{programid}_filter_stream", 
    //     stream=filter.catalog, programid=filter.permissions.iter().max().unwrap());

    // this uses consumer groups to more easily "resume" reading from where we might have stopped
    // as well as to allow multiple workers to read from the same stream for a given filter,
    // which let's us scale the number of workers per filter if needed in the future
    // https://medium.com/redis-with-raphael-de-lio/understanding-redis-streams-33aa96ca7206
    
    let mut filter_results_queues = HashMap::new();

    let mut consumer_groups = HashMap::new();
    for filter in &filters {
        let consumer_group = format!("filter_{filter_id}_group", filter_id = filter.id);
        let consumer_group_res: Result<(), redis::RedisError> = con.xgroup_create(
            &redis_streams[filter.permissions.iter().max().unwrap()], &consumer_group, "0").await;
        match consumer_group_res {
            Ok(()) => {
                println!("Created consumer group for filter {}", filter.id);
            },
            Err(e) => {
                println!("Consumer group already exists for filter {}: {:?}", filter.id, e);
            }
        }
        consumer_groups.insert(filter.id, consumer_group);
        filter_results_queues.insert(filter.id, format!("filter_{filter_id}_results", filter_id = filter.id));
    }

    println!("consumer groups: {:?}", consumer_groups);
    println!("filter_results_queues: {:?}", filter_results_queues);

    // let consumer_group = format!("filter_{filter_id}_group", filter_id=filter_id);
    // let consumer_group_res: Result<(), redis::RedisError> = con.xgroup_create(
    //     &redis_stream, &consumer_group, "0").await;

    // let filter_results_queue = format!("filter_{filter_id}_results", filter_id=filter_id);

    // match consumer_group_res {
    //     Ok(_) => {
    //         println!("Created consumer group for filter {}", filter_id);
    //     },
    //     Err(e) => {
    //         println!("Consumer group already exists for filter {}: {:?}", filter_id, e);
    //     }
    // }

    let mut read_options = HashMap::new();
    for filter_id in consumer_groups.keys() {
        let opts = StreamReadOptions::default()
            .group(&consumer_groups[filter_id], "worker_1")
            .count(100);
        read_options.insert(filter_id, opts);
    }

    println!("read options: {:?}", read_options);
    // let opts = StreamReadOptions::default()
    //     .group(&consumer_group, "worker_1")
    //     .count(100);

    // keep track of how many streams are empty in order to take breaks
    let mut empty_stream_counter: usize = 0;

    loop {
        // check if worker has been interrupted
        match interrupt.try_lock() {
            Ok(x) => {
                if *x { return Ok(()); }
            },
            _ => {}
        };

        for filter in &mut filters {
            let candids = get_candids_from_stream(
                &mut con,
                &redis_streams[filter.permissions.iter().max().unwrap()], 
                &read_options[&filter.id]).await;
            if candids.len() == 0 {
                println!("Stream empty");
                // tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                empty_stream_counter += 1;
                continue;
            }

            let in_count = candids.len();

            println!("got {} candids from a redis stream for filter {}", in_count, redis_streams[filter.permissions.iter().max().unwrap()]);
            println!("running filter with id {} on {} alerts", filter.id, in_count);

            let start = std::time::Instant::now();

            let out_documents = filter.run(candids, &db).await.unwrap();
            if out_documents.len() == 0 {
                continue;
            }

            // convert the documents to a format that any other worker (even in python) can read
            // for that we can deserialize the Document to json
            let out_documents: Vec<String> = out_documents.iter().map(|doc| {
                let json = serde_json::to_string(doc).unwrap();
                json
            }).collect();
            con.lpush::<&str, &Vec<String>, isize>(
                &filter_results_queues[&filter.id],
                 &out_documents).await.unwrap();

            println!(
                "{}/{} alerts passed filter {} in {}s", 
                out_documents.len(), in_count, filter.id, start.elapsed().as_secs_f64());

            println!("finished with filter {}", filter.id);
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            
        }
        if empty_stream_counter == filters.len() {
            println!("All streams empty");
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        }
        empty_stream_counter = 0;
    }
    
}
