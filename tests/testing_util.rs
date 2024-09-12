use boom::conf;
use boom::alert;
use mongodb::bson::doc;
use redis::AsyncCommands;

// insert a test filter into the database
pub async fn insert_test_filter_good() {

    let filter_obj: mongodb::bson::Document = doc!{
        "_id": mongodb::bson::oid::ObjectId::new(),
        // "_id": {``
        //   "$oid": "66cc9b384e3210074763c611"
        // },
        "group_id": 41,
        "filter_id": -1,
        "catalog": "ZTF_alerts",
        "permissions": [
          1
        ],
        "active": true,
        "active_fid": "v2e0fs",
        "fv": [
          {
            "fid": "v2e0fs",
            "pipeline": "[{\"$project\": {\"cutoutScience\": 0, \"cutoutDifference\": 0, \"cutoutTemplate\": 0, \"publisher\": 0, \"schemavsn\": 0}}, {\"$lookup\": {\"from\": \"alerts_aux\", \"localField\": \"objectId\", \"foreignField\": \"_id\", \"as\": \"aux\"}}, {\"$project\": {\"objectId\": 1, \"candid\": 1, \"candidate\": 1, \"classifications\": 1, \"coordinates\": 1, \"prv_candidates\": {\"$arrayElemAt\": [\"$aux.prv_candidates\", 0]}, \"cross_matches\": {\"$arrayElemAt\": [\"$aux.cross_matches\", 0]}}}, {\"$match\": {\"candidate.drb\": {\"$gt\": 0.5}, \"candidate.ndethist\": {\"$gt\": 1.0}, \"candidate.magpsf\": {\"$lte\": 18.5}}}]",
            "created_at": {
              "$date": "2020-10-21T08:39:43.693Z"
            }
          }
        ],
        "autosave": false,
        "update_annotations": true,
        "created_at": {
          "$date": "2021-02-20T08:18:28.324Z"
        },
        "last_modified": {
          "$date": "2023-05-04T23:39:07.090Z"
        }
      };

    let config_file = conf::load_config("./config.yaml").unwrap();
    let db = conf::build_db(&config_file, true).await;
    let x = db.collection::<mongodb::bson::Document>("filters").insert_one(filter_obj).await;
    match x {
        Ok(x) => {
            println!("successfully inserted filter obj into database");
        },
        Err(e) => {
            println!("error inserting filter obj: {}", e);
        }
    }
    
}

// remove test filter from the database
pub async fn remove_test_filter_good() {
    let config_file = conf::load_config("./config.yaml").unwrap();
    let db = conf::build_db(&config_file, true).await;
    let _ = db.collection::<mongodb::bson::Document>("filters").delete_one(doc!{"filter_id": -1}).await;
}

pub async fn setup_alert_queue(queue_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    drop_alert_collections().await?;
    fake_kafka_consumer("20240617").await?;
    println!("processing alerts...");
    alert_worker(queue_name).await;
    println!("candids successfully placed into redis queue '{}'", queue_name);
    Ok(())
}

pub async fn drop_alert_collections() -> Result<(), Box<dyn std::error::Error>> {
    let config_file = conf::load_config("./config.yaml").unwrap();
    let db = conf::build_db(&config_file, true).await;
    db.collection::<mongodb::bson::Document>("alerts").drop().await?;
    db.collection::<mongodb::bson::Document>("alerts_aux").drop().await?;
    println!("dropped collections: 'alerts' & 'alerts_aux'");
    Ok(())
}

pub async fn alert_worker(output_queue_name: &str) {

    let config_file = conf::load_config("./config.yaml").unwrap();
    let stream_name = "ZTF";
    let xmatch_configs = conf::build_xmatch_configs(&config_file, stream_name);
    let db: mongodb::Database = conf::build_db(&config_file, true).await;

    if let Err(e) = db.list_collection_names().await {
        println!("Error connecting to the database: {}", e);
        return;
    }
    let alert_collection = db.collection(&format!("{}_alerts", stream_name));
    let alert_aux_collection = db.collection(&format!("{}_alerts_aux", stream_name));

    let packet_queue = "testalertpacketqueue";
    let packet_queue_temp = "testalertpacketqueuetemp";

    let client_redis = redis::Client::open(
        "redis://localhost:6379".to_string()
    ).unwrap();
    let mut con = client_redis.get_multiplexed_async_connection().await.unwrap();

    loop {
        let result: Option<Vec<Vec<u8>>> = con.rpoplpush(packet_queue, packet_queue_temp).await.unwrap();
        match result {
            Some(value) => {
                let candid = alert::process_alert(
                    value[0].clone(), 
                    &xmatch_configs, 
                    &db,
                    &alert_collection,
                    &alert_aux_collection,
                ).await;
                match candid {
                    Ok(Some(candid)) => {
                        // println!("Processed alert with candid: {}, queueing for classification", candid);
                        // queue the candid for processing by the classifier
                        con.lpush::<&str, i64, isize>(output_queue_name, candid).await.unwrap();
                        con.lrem::<&str, Vec<u8>, isize>(packet_queue_temp, 1, value[0].clone()).await.unwrap();
                    }
                    Ok(None) => {
                        println!("Alert already exists");
                        // remove the alert from the queue
                        con.lrem::<&str, Vec<u8>, isize>(packet_queue_temp, 1, value[0].clone()).await.unwrap();
                    }
                    Err(e) => {
                        // println!("Error processing alert: {}, requeueing", e);
                        // put it back in the alertpacketqueue, to the left (pop from the right, push to the left)
                        con.lrem::<&str, Vec<u8>, isize>(packet_queue_temp, 1, value[0].clone()).await.unwrap();
                        con.lpush::<&str, Vec<u8>, isize>(packet_queue, value[0].clone()).await.unwrap();
                    }
                }
            }
            None => {
                return;
            }
        }
    }
}

pub fn download_alerts_from_archive(date: &str) -> Result<i64, Box<dyn std::error::Error>> {
    // given a date in the format YYYYMMDD, download public ZTF alerts from the archive
    // in this method we just validate the date format,
    // then use wget to download the alerts from the archive
    // and finally we extract the alerts to a folder

    // validate the date format
    if date.len() != 8 {
        return Err("Invalid date format".into());
    }

    // create the data folder if it doesn't exist, in data/alerts/ztf/<date>
    let data_folder = format!("data/alerts/ztf/{}", date);

    // if it already exists and has the alerts, we don't need to download them again
    if std::path::Path::new(&data_folder).exists() && std::fs::read_dir(&data_folder)?.count() > 0 {
        println!("Alerts already downloaded to {}", data_folder);
        let count = std::fs::read_dir(&data_folder)?.count();
        return Ok(count as i64);
    }

    std::fs::create_dir_all(&data_folder)?;

    println!("Downloading alerts for date {}", date);
    // download the alerts to data folder
    let url = format!("https://ztf.uw.edu/alerts/public/ztf_public_{}.tar.gz", date);
    let output = std::process::Command::new("wget")
        .arg(&url)
        .arg("-P")
        .arg(&data_folder)
        .output()?;
    if !output.status.success() {
        return Err("Failed to download alerts".into());
    } else {
        println!("Downloaded alerts to {}", data_folder);
    }

    // extract the alerts
    let output = std::process::Command::new("tar")
        .arg("-xzf")
        .arg(format!("{}/ztf_public_{}.tar.gz", data_folder, date))
        .arg("-C")
        .arg(&data_folder)
        .output()?;
    if !output.status.success() {
        return Err("Failed to extract alerts".into());
    } else {
        println!("Extracted alerts to {}", data_folder);
    }

    // remove the tar.gz file
    std::fs::remove_file(format!("{}/ztf_public_{}.tar.gz", data_folder, date))?;

    // count the number of alerts
    let count = std::fs::read_dir(&data_folder)?.count();

    Ok(count as i64)
}

pub async fn fake_kafka_consumer(alert_date: &str) -> Result<(), Box<dyn std::error::Error>> {
    
    let packet_queue = "testalertpacketqueue";

    let date = alert_date;
    let total_nb_alerts = download_alerts_from_archive(date)?;

    let client = redis::Client::open(
        "redis://localhost:6379".to_string()
    )?;
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // empty the queue
    con.del::<&str, usize>(packet_queue).await.unwrap();

    let mut total = 0;

    println!("Pushing {} alerts to the queue", total_nb_alerts);
    // start timer
    let start = std::time::Instant::now();
    // poll one message at a time
    for entry in std::fs::read_dir(format!("data/alerts/ztf/{}", date))? {
        let entry = entry?;
        let path = entry.path();
        let payload = std::fs::read(path)?;

        con.rpush::<&str, Vec<u8>, usize>(packet_queue, payload.to_vec()).await.unwrap();
        total += 1;
        if total % 1000 == 0 {
            println!("Pushed {} items since {:?}", total, start.elapsed());
        }
    }

    Ok(())
}