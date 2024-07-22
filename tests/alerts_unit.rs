use boom::alerts;
use boom::structs;
use boom::utils;

use mongodb::{
    options::ClientOptions,
    Client,
    Collection,
    bson,    
};

use apache_avro::{
    from_value,
    Reader,
};
use std::{
    sync::Arc,
    sync::Mutex,
    thread,
    error::Error,
    path::Path,
};


#[cfg(test)]
mod test {
    use std::path;

    use super::*;
    use crossbeam::queue::SegQueue;
    use mongodb::bson::{doc, oid::ObjectId, Document};
    use tokio::test;

    /* Various testing utilities */


    fn get_test_crossmatching_vecs(client: mongodb::Client) -> (
        Vec<(&'static str, Collection<Document>)>, 
        Vec<(&'static str, structs::CrossmatchConfig)>
    ) {
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
        (crossmatching_collections, crossmatching_config)
    }

    async fn create_test_collections(
        client: mongodb::Client
    ) -> Result<(), Box<dyn Error>> {
        let _ = client.database("kowalski")
            .create_collection("test_alerts", None).await?;
        let _ = client.database("kowalski")
            .create_collection("test_alerts_aux", None).await?;
        Ok(())
    }

    fn get_test_collections(client: mongodb::Client) -> (
        mongodb::Collection<structs::AlertWithCoords>, 
        mongodb::Collection<structs::AlertAux>
    ) {
        let collection = client.database("kowalski").collection("test_alerts");
        let collection_aux = client.database("kowalski").collection("test_alerts_aux");
        (collection, collection_aux)
    }

    async fn drop_test_collections(
        client: mongodb::Client
    ) -> Result<(), Box<dyn Error>> {
        let _ = client.database("kowalski")
            .collection::<structs::AlertWithCoords>("test_alerts").drop(None).await;
        let _ = client.database("kowalski")
            .collection::<structs::AlertAux>("test_alerts_aux").drop(None).await;
        Ok(())
    }

    async fn setup_client() -> Result<mongodb::Client, mongodb::error::Error> {
        let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
        let client = Client::with_options(client_options)?;
        Ok(client)
    }

    // does the same as the regular alert worker but uses the test database collections
    async fn test_worker(
        queue: Arc<Mutex<Vec<apache_avro::types::Value>>>
    ) -> Result<(), Box<dyn Error>> {
        let client = setup_client().await?;
        let collections = get_test_collections(client.clone());
        let crossmatching_vecs = get_test_crossmatching_vecs(client.clone());
        
        let mut object_id: String;

        loop {
            let record = queue.lock().unwrap().pop();
            match record {
                Some(record) => {

                    let alert: structs::AlertPacket = from_value(&record).unwrap();
                    object_id = alert.objectId.clone();

                    alerts::process_record(
                        record,
                        &collections.0,
                        &collections.1,
                        &crossmatching_vecs.0,
                        &crossmatching_vecs.1,
                    ).await?;

                    let _ = client.database("kowalski")
                        .collection::<structs::AlertWithCoords>("test_alerts")
                        .find_one(doc!{ "_id": object_id }, None).await;
                }
                None => {
                    panic!("process_alert(record, ...) record was None inside test_worker")
                }
            }
        }
        Ok(())
    }


    // sketch function for testing process alerts function on sample data
    // #[tokio::test]
    // Trying to get this to work, but it is getting hung up on process_files for some reason
    async fn test_alerts_process_alerts() -> Result<(), Box<dyn Error>> {
        // let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
        // let client = Client::with_options(client_options)?;

        let nb_workers = 1;
        
        let client = setup_client().await?;

        let _ = create_test_collections(client.clone()).await?;

        let queue = Arc::new(Mutex::new(Vec::new()));

        // // get tuple of collections -> ("test_alert", "test_alert_aux")
        // let collections = get_test_collections(client.clone());

        // // get tuple of (crossmatching_collections: Vec<(&str, Collection<Document>)>, 
        // //               crossmatching_config: Vec<(&str, CrossmatchConfig)>)
        // let crossmatches = get_test_crossmatching_vecs(client.clone());
        

        for _ in 0..nb_workers {
            let queue = Arc::clone(&queue);
            thread::spawn(move || {
                let _ = tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(test_worker(queue));
            });
        }
        
        let _ = alerts::process_files(String::from("./data/sample_alerts"), Arc::clone(&queue), 10).await;

        // drop tables when done with test
        drop_test_collections(client.clone()).await?;

        // let _ = client.database("kowalski").collection::<structs::AlertWithCoords>("test_alerts").drop(None).await;
        // let _ = client.database("kowalski").collection::<structs::AlertAux>("test_alerts_aux").drop(None).await;

        Ok(())
    }
}

