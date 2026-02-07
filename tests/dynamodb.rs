use std::time::Duration;

use assert_cmd::Command;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::config::{Credentials, Region};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, KeySchemaElement, KeyType, ProvisionedThroughput, ScalarAttributeType,
};
use color_eyre::Result;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

#[allow(dead_code)]
struct DynamoDBEnv {
    container: ContainerAsync<GenericImage>,
    endpoint_url: Option<String>,
}

const CREATE_TABLE_MAX_ATTEMPTS: u32 = 6;
const CREATE_TABLE_RETRY_DELAY_MS: u64 = 150;

fn is_transient_dispatch_failure(err: &impl std::fmt::Debug) -> bool {
    let rendered = format!("{err:?}");
    rendered.contains("DispatchFailure")
        || rendered.contains("TransientError")
        || rendered.contains("IncompleteMessage")
}

async fn create_table_with_retry(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
) -> Result<()> {
    for attempt in 1..=CREATE_TABLE_MAX_ATTEMPTS {
        let key_schema = KeySchemaElement::builder()
            .attribute_name("PK".to_string())
            .key_type(KeyType::Hash)
            .build()
            .unwrap();
        let attribute_def = AttributeDefinition::builder()
            .attribute_name("PK".to_string())
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap();
        let provisioned_throughput = ProvisionedThroughput::builder()
            .read_capacity_units(10)
            .write_capacity_units(5)
            .build()
            .unwrap();

        match client
            .create_table()
            .table_name(table_name)
            .key_schema(key_schema)
            .attribute_definitions(attribute_def)
            .provisioned_throughput(provisioned_throughput)
            .send()
            .await
        {
            Ok(_) => return Ok(()),
            Err(err)
                if attempt < CREATE_TABLE_MAX_ATTEMPTS && is_transient_dispatch_failure(&err) =>
            {
                let delay_ms = CREATE_TABLE_RETRY_DELAY_MS * u64::from(attempt);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(err) => {
                return Err(color_eyre::eyre::eyre!(
                    "failed to create table {table_name} after {attempt} attempt(s): {err:?}"
                ));
            }
        }
    }
    unreachable!("create_table_with_retry must return from loop")
}

async fn new_dynamodb_env() -> Result<DynamoDBEnv> {
    let container = GenericImage::new("amazon/dynamodb-local", "2.5.2")
        .with_exposed_port(8000.tcp())
        .with_wait_for(WaitFor::message_on_stdout("CorsParams"))
        .with_user("root")
        .with_cmd(vec!["-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"])
        .start()
        .await
        .expect("Failed to start DynamoDB");
    let port = container.get_host_port_ipv4(8000).await?;
    Ok(DynamoDBEnv {
        container,
        endpoint_url: Some(format!("http://127.0.0.1:{}", port)),
    })
}

async fn new_local_client(endpoint_url: &str) -> Result<aws_sdk_dynamodb::Client> {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("local", "local", None, None, "test"))
        .endpoint_url(endpoint_url)
        .load()
        .await;
    Ok(aws_sdk_dynamodb::Client::new(&config))
}

#[tokio::test]
async fn list_tables() {
    let mut cmd = Command::cargo_bin("dynamate").unwrap();
    let env = new_dynamodb_env().await.unwrap();
    let endpoint_url = env.endpoint_url.as_deref().unwrap();
    let table_names = vec![
        String::from("test-table1"),
        String::from("test-table2"),
        String::from("test-table3"),
    ];
    let client = new_local_client(endpoint_url).await.unwrap();
    // Create the tables
    for table_name in &table_names {
        create_table_with_retry(&client, table_name).await.unwrap();
    }
    let stdout = cmd
        .env("AWS_REGION", "us-east-1")
        .env("AWS_ACCESS_KEY_ID", "local")
        .env("AWS_SECRET_ACCESS_KEY", "local")
        .arg("--endpoint-url")
        .arg(endpoint_url)
        .arg("list-tables")
        .arg("--json")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let parsed: serde_json::Value = serde_json::from_slice(&stdout).expect("output is valid JSON");
    let arr = parsed
        .as_array()
        .ok_or_else(|| panic!("expected top-level JSON array"))
        .unwrap();

    let names: Vec<String> = arr
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();
    assert_eq!(names, table_names);
}
