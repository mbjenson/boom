use boom::alerts;
use boom::structs;
use boom::utils;

use apache_avro::from_value;
mod testing_utils; // grab local testing utilities from this directory
use testing_utils as tu;
use std::error::Error;

#[cfg(test)]
mod test_alerts {
    use super::*;
    use alerts::process_record;

    // test process_record() on 1 alert
    // note: crossmatching does not happen due to current sample data size
    //       this should be fixed
    #[tokio::test]
    async fn test_alerts_process_record() -> Result<(), Box<dyn Error>> {

        // 1. setup
        let num_alerts = 1;

        let client = tu::setup_mongo_client().await?;
        let _ = tu::drop_test_alert_collections(client.clone()).await?;
        let _ = tu::create_test_alert_collections(client.clone()).await?;
        
        let mut records = tu::build_alert_queue(String::from("./data/sample_alerts"), num_alerts).unwrap();
        
        let x_match_tup = tu::get_test_crossmatching_vecs(client.clone());
        let test_col = tu::get_test_alert_collections(client.clone());

        let record = records.pop().unwrap();
        let mut alert: structs::AlertPacket = from_value(&record).unwrap();
        let obj_id: String = alert.objectId.clone();
        let candid = alert.candid.clone();

        // 2. run function
        let _ = process_record(
            record.clone(),
            &test_col.0,
            &test_col.1,
            &x_match_tup.0,
            &x_match_tup.1).await?;
        
        // grab result from db collection "test_alerts"
        let function_result = test_col.0.find_one(
            mongodb::bson::doc! {
                    "objectId": obj_id.clone()
            },
            None,
        ).await?;
        let function_result = function_result.unwrap();

        // reset the collection so there is no interference
        let _ = tu::drop_test_alert_collections(client.clone()).await;
        let _ = tu::create_test_alert_collections(client.clone()).await;
        let test_col = tu::get_test_alert_collections(client.clone());
        
        // manually perform process_record functionality
        
        
        let prv_candidates = alert.prv_candidates.clone();
        alert = alert.remove_prv_candidates();
        
        let coordinates = alert.get_coordinates();
        
        let alert_with_coords = structs::AlertWithCoords {
            schemavsn: alert.schemavsn.clone(),
            publisher: alert.publisher.clone(),
            candid: candid.clone(),
            objectId: obj_id.clone(),
            candidate: alert.candidate.clone(),
            cutoutScience: alert.cutoutScience.clone(),
            cutoutTemplate: alert.cutoutTemplate.clone(),
            cutoutDifference: alert.cutoutDifference.clone(),
            coordinates: Some(coordinates),
        };

        // check for if need to do crossmatching
        let res = test_col.1.count_documents(
            mongodb::bson::doc! {
                "_id": obj_id.clone()
            }, None
        ).await?;
        let mut to_update = false;
        if res == 0 {
            let ra = alert.candidate.detection.ra.unwrap();
            let dec = alert.candidate.detection.dec.unwrap();
            let ra_geojson = ra - 180.0;
            
            let crossmatches: std::collections::HashMap<String, Vec<mongodb::bson::Document>> = 
                utils::crossmatch_parallel(
                    ra, ra_geojson, dec, &x_match_tup.0, &x_match_tup.1).await?;
            
            let _ = tu::test_helper_crossmatch_parallel(
                ra, ra_geojson, dec, 
                &x_match_tup.0, &x_match_tup.1).await;

            let cross_matches = structs::AlertCrossmatches {
                milliquas_v8: crossmatches.get("milliquas_v8").unwrap().clone(),
                clu: crossmatches.get("CLU").unwrap().clone(),
                ned: crossmatches.get("NED").unwrap().clone(),
            };

            let alert_aux = structs::AlertAux {
                _id: obj_id.clone(),
                prv_candidates: prv_candidates.clone(),
                cross_matches
            };

            let result_insert = test_col.1.insert_one(
                alert_aux, None
            ).await;
            if let Err(e) = result_insert {
                println!("error adding prv_candidates for alert {}: {:?},
                        will attempt to update doc instead", obj_id, e);
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

            test_col.1.update_one(
                mongodb::bson::doc! {"_id": obj_id.clone()},
                mongodb::bson::doc! {"$addToSet": {"prv_candidates": {"$each": prv_candidates_doc}}},
                None,
            ).await?;
        }
        
        // 3. compare results
        // get result from db and compare with manually generated processed record up unto this point
        
        assert_eq!(alert_with_coords.candid, function_result.candid);
        tu::are_coordinates_eq(alert_with_coords.coordinates.unwrap(), function_result.coordinates.unwrap());
        tu::are_candidates_eq(alert_with_coords.candidate, function_result.candidate);
        tu::are_cutouts_eq(alert_with_coords.cutoutDifference, function_result.cutoutDifference);
        tu::are_cutouts_eq(alert_with_coords.cutoutScience, function_result.cutoutScience);
        tu::are_cutouts_eq(alert_with_coords.cutoutTemplate, function_result.cutoutTemplate);
        assert_eq!(alert_with_coords.objectId, function_result.objectId);
        assert_eq!(alert_with_coords.publisher, function_result.publisher);
        assert_eq!(alert_with_coords.schemavsn, function_result.schemavsn);

        Ok(())
    }

}
