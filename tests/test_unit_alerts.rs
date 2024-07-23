use boom::alerts;
use boom::structs;
use boom::utils;

// grab local testing utilities from this directory
mod testing_utils;
use testing_utils as tu;

use std::{
    sync::Arc,
    sync::Mutex,
    thread,
    error::Error,
};


#[cfg(test)]
mod test {
    use super::*;
    use tokio::test;
    
    // sketch function for testing process alerts function on sample data
    // #[tokio::test]
    // Trying to get this to work, but it is getting hung up on process_files for some reason
    async fn test_alerts_process_alerts() -> Result<(), Box<dyn Error>> {
        // let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
        // let client = Client::with_options(client_options)?;

        let nb_workers = 1;
        
        let client = tu::setup_kowalski_client().await?;

        let _ = tu::create_test_alert_collections(client.clone()).await?;

        let queue = Arc::new(Mutex::new(Vec::new()));

        // get tuple of collections -> ("test_alert", "test_alert_aux")
        // let collections = get_test_collections(client.clone());

        // get tuple of (crossmatching_collections: Vec<(&str, Collection<Document>)>, 
        //               crossmatching_config: Vec<(&str, CrossmatchConfig)>)
        // let crossmatches = get_test_crossmatching_vecs(client.clone());
        

        for _ in 0..nb_workers {
            let queue = Arc::clone(&queue);
            thread::spawn(move || {
                let _ = tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(tu::test_alert_worker(queue));
            });
        }
        
        let _ = alerts::process_files(String::from("./data/sample_alerts"), Arc::clone(&queue), 10).await;

        // drop tables when done with test
        tu::drop_test_alert_collections(client.clone()).await?;

        // let _ = client.database("kowalski").collection::<structs::AlertWithCoords>("test_alerts").drop(None).await;
        // let _ = client.database("kowalski").collection::<structs::AlertAux>("test_alerts_aux").drop(None).await;

        Ok(())
    }
}

