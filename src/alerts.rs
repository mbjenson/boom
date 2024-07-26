#[allow(non_snake_case)]

use mongodb::{
    options::ClientOptions, 
    Client, 
    Collection
};
use std::{
    error::Error, fs::File, io::BufReader, sync::{Arc, Mutex}, thread
};
use apache_avro::{
    from_value,
    Reader,
};
use config::Config;

use crate::utils;
use crate::structs;

// push alerts onto queue which is then given to the workers to process asynchronously
pub async fn process_files(
    dir_path: String,
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
    let files = utils::get_file_names(String::from(dir_path));

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

// processes a record (alert) and sends result to local mongodb
pub async fn process_record(
    record: apache_avro::types::Value,
    collection: &Collection<structs::AlertWithCoords>,
    collection_aux: &Collection<structs::AlertAux>,
    crossmatching_collections: &Vec<(&str, Collection<mongodb::bson::Document>)>,
    crossmatching_config: &Vec<(&str, structs::CrossmatchConfig)>,
) -> Result<(), Box<dyn Error>> {
    // process the record
    let mut alert: structs::AlertPacket = from_value(&record).unwrap();
    let objectId: String = alert.objectId.clone();
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

// worker given records to process from queue
pub async fn worker(conf: Config, queue: Arc<Mutex<Vec<apache_avro::types::Value>>>) -> Result<(), Box<dyn Error>> {

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

    let milliquas_config = utils::get_milliquas_config(&conf).unwrap();
    let clu_config = utils::get_clu_config(&conf).unwrap();
    let ned_config = utils::get_ned_config(&conf).unwrap();

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
