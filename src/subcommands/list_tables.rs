use color_eyre::Result;
use dynamate::core::datastore::Datastore;

pub struct Options {
    pub json: bool,
}

pub async fn command(db: &dyn Datastore, options: Options) -> Result<()> {
    let table_names = db.list_collections().await.map_err(|err| eyre(&err))?;

    if options.json {
        println!("{}", serde_json::to_string(&table_names)?);
        return Ok(());
    }

    for table in table_names {
        println!("{table}");
    }
    Ok(())
}

fn eyre(err: &dynamate::core::error::DbError) -> color_eyre::eyre::Error {
    color_eyre::eyre::eyre!(err.to_string())
}
