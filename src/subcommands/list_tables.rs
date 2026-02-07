use color_eyre::Result;
use dynamate::dynamodb::send_dynamo_request;

pub struct Options {
    pub json: bool,
}

pub async fn command(client: &aws_sdk_dynamodb::Client, options: Options) -> Result<()> {
    // Collect all tables.table_names, because there might be multiple pages
    let mut table_names = Vec::new();
    let mut last_evaluated_table_name = None;

    loop {
        let span = tracing::trace_span!(
            "ListTables",
            start_table = ?last_evaluated_table_name.as_deref()
        );
        let result = send_dynamo_request(
            span,
            || {
                client
                    .list_tables()
                    .set_exclusive_start_table_name(last_evaluated_table_name)
                    .send()
            },
            |err| err.to_string(),
        )
        .await;
        let output = result?;
        table_names.extend(output.table_names().iter().cloned());

        if output.last_evaluated_table_name().is_none() {
            break;
        }
        last_evaluated_table_name = output.last_evaluated_table_name().map(|s| s.to_string());
    }

    if options.json {
        println!("{}", serde_json::to_string(&table_names)?);
        return Ok(());
    }

    for table in table_names {
        println!("{}", table);
    }
    Ok(())
}
