use assert_cmd::Command;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, KeySchemaElement, KeyType, ProvisionedThroughput, ScalarAttributeType,
};
use color_eyre::{Result};
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

#[tokio::test]
async fn list_tables() {
    let mut cmd = Command::cargo_bin("dynamate").unwrap();
    let env = new_dynamodb_env().await.unwrap();
    let table_names = vec![
        String::from("test-table1"),
        String::from("test-table2"),
        String::from("test-table3"),
    ];
    let client = dynamate::aws::new_client(env.endpoint_url.as_deref())
        .await
        .unwrap();
    // Create the tables
    for table_name in &table_names {
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
        client
            .create_table()
            .table_name(table_name)
            .key_schema(key_schema)
            .attribute_definitions(attribute_def)
            .provisioned_throughput(provisioned_throughput)
            .send()
            .await
            .unwrap();
    }
    let stdout = cmd
        .arg("list-tables")
        .arg("--endpoint-url")
        .arg(env.endpoint_url.unwrap())
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
