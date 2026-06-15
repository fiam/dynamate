//! MongoDB connection setup. Builds a client + database from a connection URI;
//! the database name comes from the URI path (`mongodb://host:27017/mydb`).

use mongodb::{Client, options::ClientOptions};

use super::backend::MongoBackend;

pub async fn connect(uri: &str, read_only: bool) -> Result<MongoBackend, String> {
    let options = ClientOptions::parse(uri)
        .await
        .map_err(|err| err.to_string())?;
    let db_name = options.default_database.clone().ok_or_else(|| {
        "MongoDB connection string must include a database, e.g. mongodb://host:27017/mydb"
            .to_string()
    })?;
    let client = Client::with_options(options).map_err(|err| err.to_string())?;
    let db = client.database(&db_name);
    Ok(MongoBackend::new(db, read_only))
}
