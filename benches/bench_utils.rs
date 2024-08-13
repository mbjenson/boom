use boom::utils;
use boom::structs;
use boom::alerts;
use std::{
    error::Error,
    fs::File,
    io::BufReader,
};
use mongodb::{
    Client,
    Collection,
    options::ClientOptions,
};
use apache_avro::Reader;

pub async fn drop_test_alert_collections(
    client: mongodb::Client
) -> Result<(), Box<dyn Error>> {
    let _ = client.database("kowalski")
        .collection::<structs::AlertWithCoords>("test_alerts").drop(None).await;
    let _ = client.database("kowalski")
        .collection::<structs::AlertAux>("test_alerts_aux").drop(None).await;
    Ok(())
}

// returns a Vec containing apache alerts read from file
pub fn build_alert_queue(
    alert_path: String,
    num_alerts: usize,
) -> Result<Vec<apache_avro::types::Value>, Box<dyn Error>> {
    let mut index = 0 as usize;
    let files = utils::get_file_names(String::from(alert_path));
    let mut queue = Vec::<apache_avro::types::Value>::new();

    while index < files.len() && index < num_alerts {
        let file_name = files[index].clone();
        let file = File::open(file_name).unwrap();
        let reader = Reader::new(BufReader::new(file)).unwrap();
        for record in reader {
            let record = record.unwrap();
            queue.push(record);
        }
        index += 1;
    }
    Ok(queue)
}

// return mongodb client object, connected to local mongodb instance
pub async fn setup_mongo_client() -> Result<mongodb::Client, mongodb::error::Error> {
    let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
    let client = Client::with_options(client_options)?;
    Ok(client)
}

// retrieve two mongodb collection objects
pub fn get_alert_collections(
    client: mongodb::Client
) -> (mongodb::Collection<structs::AlertWithCoords>, mongodb::Collection<structs::AlertAux>) {
    let collection = client.database("kowalski").collection("test_alerts");
    let collection_aux = client.database("kowalski").collection("test_alerts_aux");
    (collection, collection_aux)
}

// get the required vecs to pass in for process_record
pub fn get_test_crossmatching_vecs(client: mongodb::Client) -> (
    Vec<(&'static str, Collection<mongodb::bson::Document>)>, 
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