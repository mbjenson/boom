#![allow(non_snake_case)]
#![allow(dead_code)]



use std::{
    sync::Arc,
    sync::Mutex,
    thread,
    error::Error,
    fs::File,
    io::BufReader,
};

use mongodb::{
    bson::{Document, doc},
    options::ClientOptions, 
    Client, 
    Collection
};

// here we want to load the alerts from the avro files in the ./data directory
// we will use the apache-avro crate
use apache_avro::{
    from_value,
    Reader,
};

mod fits;
mod structs;
mod utils;


// grab the list of all files in the ./data/alerts directory
fn get_files() -> Vec<String> {
    let paths = std::fs::read_dir("./data/alerts/").unwrap();
    let mut files = Vec::new();
    for path in paths {
        let path = path.unwrap().path();
        let path = path.to_str().unwrap().to_string();
        files.push(path);
    }
    files
}


async fn process_files(
    queue: Arc<Mutex<Vec<apache_avro::types::Value>>>,
    max_queue_len: usize,
) -> Result<(), Box<dyn Error>> {
    // DEBUG ONLY: drop the alerts and alerts_aux collections
    let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
    let client = Client::with_options(client_options)?;

    let collection: Collection<structs::AlertWithCoords> =
        client.database("kowalski").collection("alerts");
    let collection_aux: Collection<structs::AlertAux> =
        client.database("kowalski").collection("alerts_aux");

    let _ = collection.drop(None).await;
    let _ = collection_aux.drop(None).await;

    // recreate the collections
    let _ = client.database("kowalski").create_collection("alerts", None).await?;
    let _ = client.database("kowalski").create_collection("alerts_aux", None).await?;

    let mut index = 0 as usize;
    let mut files = get_files();
    // DEBUG, only keep a fixed number of files
    // files = files
    //     .iter()
    //     .take(5000)
    //     .map(|x| x.to_string())
    //     .collect::<Vec<String>>();

    let total_nb_docs = files.len() as u64;

    while index < files.len() {
        // add the record to the queue
        let current_queue_len = queue.lock().unwrap().len();
        if current_queue_len < max_queue_len {
            let file_name = files[index].clone();
            let queue = queue.clone();
            thread::spawn(move || {
                let file = File::open(file_name).unwrap();
                let reader = Reader::new(BufReader::new(file)).unwrap();
                for record in reader {
                    let record = record.unwrap();
                    queue.lock().unwrap().push(record);
                }
            });
            index += 1;
        } else {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            // println!("current queue len: {}", current_queue_len);
        }
    }

    // wait for the queue to be empty
    while !queue.lock().unwrap().is_empty() {
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    }

    // wait for the collection to have the same number of documents as the number of files
    while collection.count_documents(None, None).await? < total_nb_docs {
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    }

    // sleep 10 seconds to let the workers finish
    tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;

    Ok(())
}

async fn process_record(
    record: apache_avro::types::Value,
    collection: &Collection<structs::AlertWithCoords>,
    collection_aux: &Collection<structs::AlertAux>,
    crossmatching_collections: &Vec<(&str, Collection<mongodb::bson::Document>)>,
    crossmatching_config: &Vec<(&str, structs::CrossmatchConfig)>,
) -> Result<(), Box<dyn Error>> {
    // process the record
    let mut alert: structs::AlertPacket = from_value(&record).unwrap();
    let objectId = alert.objectId.clone();
    let candid = alert.candid.clone();

    // if there is already an alert with that candid, we skip it
    let result = collection.find_one(
        mongodb::bson::doc! {
            "candid": candid
        },
        None,
    ).await?;
    if let Some(_) = result {
        println!("alert with candid {} already exists, skipping", candid);
        return Ok(());
    }

    let prv_candidates = alert.prv_candidates.clone();
    alert = alert.remove_prv_candidates();

    let coordinates = alert.get_coordinates();

    let alert_with_coords = structs::AlertWithCoords {
        schemavsn: alert.schemavsn.clone(),
        publisher: alert.publisher.clone(),
        candid: candid.clone(),
        objectId: objectId.clone(),
        candidate: alert.candidate.clone(),
        cutoutScience: alert.cutoutScience.clone(),
        cutoutTemplate: alert.cutoutTemplate.clone(),
        cutoutDifference: alert.cutoutDifference.clone(),
        coordinates: Some(coordinates),
    };

    collection.insert_one(alert_with_coords, None).await?;

    // if there is no prv_candidates for that alert in the DB, we add it
    // run a simly count to see if we have already added prv_candidates for that alert
    // it is possible that we run the count and then another thread adds the prv_candidates
    // so we catch the error and try to update the doc instead
    let result = collection_aux.count_documents(
        mongodb::bson::doc! {
            "_id": objectId.clone()
        },
        None,
    ).await?;
    let mut to_update = false;
    if result == 0 {
        let ra = alert.candidate.detection.ra.unwrap();
        let dec = alert.candidate.detection.dec.unwrap();
        // only ra needs to be edited to geojson
        let ra_geojson = ra - 180.0;
        let crossmatches: std::collections::HashMap<String, Vec<mongodb::bson::Document>> = utils::crossmatch_parallel(
            ra,
            ra_geojson,
            dec,
            &crossmatching_collections,
            &crossmatching_config,
        ).await?;

        let cross_matches = structs::AlertCrossmatches {
            milliquas_v8: crossmatches.get("milliquas_v8").unwrap().clone(),
            clu: crossmatches.get("CLU").unwrap().clone(),
            ned: crossmatches.get("NED").unwrap().clone(),
        };
        let alert_aux = structs::AlertAux {
            _id: objectId.clone(),
            prv_candidates: prv_candidates.clone(),
            cross_matches,
        };
        let result_insert = collection_aux.insert_one(alert_aux, None).await;
        if let Err(e) = result_insert {
            println!("error adding prv_candidates for alert {}: {:?}, will attempt to update doc instead", objectId, e);
            to_update = true;
        }
    } else {
        to_update = true;
    }

    if to_update {
        let prv_candidates_doc: Vec<mongodb::bson::Document> = prv_candidates
            .iter()
            .map(|x| mongodb::bson::to_document(x).unwrap())
            .collect();

        collection_aux.update_one(
            mongodb::bson::doc! {"_id": objectId.clone()},
            mongodb::bson::doc! {"$addToSet": {"prv_candidates": {"$each": prv_candidates_doc}}},
            None,
        ).await?;
    }

    Ok(())
}

async fn worker(queue: Arc<Mutex<Vec<apache_avro::types::Value>>>) -> Result<(), Box<dyn Error>> {

    let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
    let client = Client::with_options(client_options)?;

    let collection: Collection<structs::AlertWithCoords> =
        client.database("kowalski").collection("alerts");
    let collection_aux: Collection<structs::AlertAux> =
        client.database("kowalski").collection("alerts_aux");


    let milliquas_v8: Collection<mongodb::bson::Document> =
        client.database("kowalski").collection("milliquas_v8");
    let clu: Collection<mongodb::bson::Document> =
        client.database("kowalski").collection("CLU");
    let ned: Collection<mongodb::bson::Document> =
        client.database("kowalski").collection("NED");

    let crossmatching_collections =
        vec![("milliquas_v8", milliquas_v8), ("CLU", clu), ("NED", ned)];

    let milliquas_config = structs::CrossmatchConfig {
        radius: 2.0,
        use_distance: false,
        ..Default::default()
    };
    let clu_config = structs::CrossmatchConfig {
        radius: 10800.0, // 3 degrees in arcseconds
        use_distance: true,
        distance_key: "z".to_string(),
        distance_max: 30.0,       // 30 Kpc
        distance_max_near: 300.0, // 5 arcsec for objects that are too close (z < 0.01)
        distance_unit: "redshift".to_string(),
    };
    let ned_config = structs::CrossmatchConfig {
        radius: 10800.0, // 3 degrees in arcseconds
        use_distance: true,
        distance_key: "DistMpc".to_string(),
        distance_max: 30.0,       // 30 Kpc
        distance_max_near: 300.0, // 5 arcsec for objects that are too close (z < 0.01)
        distance_unit: "Mpc".to_string(),
    };
    // hashmap with the config per crossmatching collection
    let crossmatching_config = vec![
        ("milliquas_v8", milliquas_config),
        ("CLU", clu_config),
        ("NED", ned_config),
    ];

    loop {
        let record = queue.lock().unwrap().pop();
        match record {
            Some(record) => {
                // process the record
                process_record(
                    record,
                    &collection,
                    &collection_aux,
                    &crossmatching_collections,
                    &crossmatching_config,
                ).await?;
            }
            None => {
                // sleep and continue
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }

    Ok(())
}


#[tokio::main]
async fn main() {
    let nb_workers = 10;
    let max_queue_length = 1000;

    // create the alerts queue
    let queue = Arc::new(Mutex::new(Vec::new()));

    // fire and forget the threaded workers
    for _ in 0..nb_workers {
        let queue = Arc::clone(&queue);
        thread::spawn(move || {
            let _ = tokio::runtime::Runtime::new().unwrap().block_on(worker(queue));
        });
    }

    let now = std::time::Instant::now();

    let _ = process_files(Arc::clone(&queue), max_queue_length).await;

    println!("all files processed in {}s, stopping the app", now.elapsed().as_secs_f64());
}

// all files processed in 1068.59524525s, stopping the app



// Test main function for dev purposes
// #[tokio::main]
// async fn main() -> mongodb::error::Result<()> {
//     // Replace the placeholder with your Atlas connection string
//     let uri = "mongodb://localhost:27017";
//     // Create a new client and connect to the server
//     let client = Client::with_uri_str(uri).await?;
//     // Get a handle on the movies collection
//     let database = client.database("kowalski");
//     // let my_coll: Collection<Document> = database.collection("test");
//     let collection: Collection<Document> = database.collection("people");
    
//     let result = collection.find_one(doc! {"name": "Paul"}, None).await?;
    
//     collection.insert_one(doc! {"name": "Lewis", "city": "Lancaster"}, None).await?;

//     let result2 = collection.find_one(doc! {"name": "Lewis"}, None).await?;
    

//     println!("Found the first person:\n{:#?}", result);
//     println!("Found the second person:\n{:#?}", result2);

//     Ok(())
// }