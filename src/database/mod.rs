use mongodb::Collection;
use mongodb::{options::ClientOptions, Client, Database};
use serde_json::Value;

use crate::structs::{AlertAux, AlertWithCoords, Detection};

async fn connect_mongodb() -> Database {
    let client_options = ClientOptions::parse("mongodb://localhost:27017")
        .await
        .unwrap();
    let client = Client::with_options(client_options).unwrap();
    client.database("boom")
}

pub async fn alert_exists(candid: i64) -> bool {
    let db = connect_mongodb().await;
    let collection: Collection<AlertWithCoords> = db.collection("alerts");
    let result = collection
        .count_documents(
            mongodb::bson::doc! {
                "candid": candid,
            },
            None,
        )
        .await
        .unwrap();
    result > 0
}

pub async fn alert_aux_exists(object_id: &str) -> bool {
    let db = connect_mongodb().await;
    let collection: Collection<Value> = db.collection("alerts_aux");
    let result = collection
        .count_documents(
            mongodb::bson::doc! {
                "_id": object_id
            },
            None,
        )
        .await
        .unwrap();
    result > 0
}

pub async fn save_alert(alert: &AlertWithCoords) {
    let db = connect_mongodb().await;
    let collection: Collection<AlertWithCoords> = db.collection("alerts");
    collection.insert_one(alert.clone(), None).await.unwrap();
}

pub async fn save_alert_aux(alert_aux: AlertAux) {
    let db = connect_mongodb().await;
    let collection: Collection<AlertAux> = db.collection("alerts_aux");
    collection
        .insert_one(alert_aux.clone(), None)
        .await
        .unwrap();
}

pub async fn update_alert_aux(object_id: &str, prv_candidates: Vec<Detection>) {
    let prv_candidates_doc: Vec<mongodb::bson::Document> = prv_candidates
        .iter()
        .map(|x| mongodb::bson::to_document(x).unwrap())
        .collect();
    let db = connect_mongodb().await;
    let collection: Collection<Value> = db.collection("alerts_aux");
    collection
        .update_one(
            mongodb::bson::doc! {"_id": object_id},
            mongodb::bson::doc! {"$addToSet": {"prv_candidates": {"$each": prv_candidates_doc}}},
            None,
        )
        .await
        .unwrap();
}
