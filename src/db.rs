use mongodb::{
    options::ClientOptions, 
    Client, 
};

// return connected client object
pub async fn connect_to_db() -> Result<mongodb::Client, mongodb::error::Error> {
    let client = Client::with_options(ClientOptions::parse("mongodb://localhost:27017").await?);
    client
}
