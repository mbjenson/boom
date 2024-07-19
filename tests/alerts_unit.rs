use boom::alerts;
use boom::db;
use mongodb::{
    options::ClientOptions,
    Client,
    Collection,
};
use apache_avro::{
    from_value,
    Reader,
};


#[cfg(test)]
mod test {
    use super::*;
    use tokio::test;

    #[tokio::test]
    async fn test_alerts_process_alerts() {
        let client = db::connect_to_db().await;
        // let record = 
        // alerts::process_record(.......).await;
    }
}