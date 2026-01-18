use aws_config::BehaviorVersion;
use aws_config::environment::{
    credentials::EnvironmentVariableCredentialsProvider, region::EnvironmentVariableRegionProvider,
};
use aws_config::meta::region::ProvideRegion;
use aws_sdk_dynamodb::config::ProvideCredentials;
use color_eyre::eyre::{Context, Result, eyre};

pub async fn new_client(endpoint_url: Option<&str>) -> Result<aws_sdk_dynamodb::Client> {
    let region_provider = EnvironmentVariableRegionProvider::new();
    let region = region_provider
        .region()
        .await
        .ok_or_else(|| eyre!("AWS region not set. Use AWS_REGION or AWS_DEFAULT_REGION."))?;

    let credential_check = EnvironmentVariableCredentialsProvider::new();
    credential_check
        .provide_credentials()
        .await
        .map_err(|err| eyre!("AWS credentials not found in environment: {err}"))?;

    let credentials_provider = EnvironmentVariableCredentialsProvider::new();
    let mut loader = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .credentials_provider(credentials_provider);

    if let Some(url) = endpoint_url {
        loader = loader.endpoint_url(url);
    }

    let config = loader.load().await;
    Ok(aws_sdk_dynamodb::Client::new(&config))
}

pub async fn validate_connection(client: &aws_sdk_dynamodb::Client) -> Result<()> {
    client
        .list_tables()
        .limit(1)
        .send()
        .await
        .map(|_| ())
        .wrap_err("Failed to connect to DynamoDB")
}
