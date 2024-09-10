use boom::filter;
use boom::conf;
use redis::AsyncCommands;
mod testing_util;
use testing_util as tu;
use std::num::NonZero;

const TEST_FILTER_ID: i32 = -1;
const TEST_QUEUE_NAME: &str = "testalertqueue";

#[tokio::test]
async fn test_filter_run() -> () {

    tu::insert_test_filter_good().await;

    let config_file = conf::load_config("./config.yaml").unwrap();
    let db = conf::build_db(&config_file, true).await;
    let mut filter = filter::Filter::build(TEST_FILTER_ID, &db).await.unwrap();

    let _ = tu::setup_alert_queue(TEST_QUEUE_NAME).await;
    
    let client_redis = redis::Client::open(
        "redis://localhost:6379".to_string()).unwrap();
    let mut con = client_redis
        .get_multiplexed_async_connection().await.unwrap();

    let res: Result<Vec<i64>, redis::RedisError> = con.rpop::<&str, Vec<i64>>(
        TEST_QUEUE_NAME, NonZero::new(1000)).await;

    match res {
        Ok(candids) => {
            if candids.len() == 0 {
                println!("test_filter_run failed: no candids in queue");
                return ()
            }

            let out_candids = filter.run(candids.clone(), &db).await;
            match out_candids {
                Ok(out) => {
                    assert_eq!(out.len(), 194);
                },
                Err(e) => {
                    println!("test_filter_run failed: {}", e);
                }
            }
        },
        Err(e) => {
            println!("Got error: {}", e);
        }
    }
    tu::remove_test_filter_good().await;
}

#[tokio::test]
async fn test_filter_build() {
    tu::insert_test_filter_good().await;
    let config_file = conf::load_config("./config.yaml").unwrap();
    let db = conf::build_db(&config_file, true).await;
    let filter = filter::Filter::build(TEST_FILTER_ID, &db).await;
    match filter {
        Err(e) => {
            println!("test failed got error: {}", e);
        },
        _ => {}
    }
    tu::remove_test_filter_good().await;
}
