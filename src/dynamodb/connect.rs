//! DynamoDB connection setup.
//!
//! Builds an `aws_sdk_dynamodb::Client` from the environment (region +
//! credentials), optionally pointed at a custom endpoint. This is the only place
//! that constructs the SDK client; the rest of the app goes through
//! [`DynamoBackend`](super::DynamoBackend) and the `Datastore` trait.

use aws_config::BehaviorVersion;
use aws_config::environment::{
    credentials::EnvironmentVariableCredentialsProvider, region::EnvironmentVariableRegionProvider,
};
use aws_config::meta::region::ProvideRegion;
use aws_sdk_dynamodb::config::ProvideCredentials;

/// Construct a DynamoDB client, validating that region and credentials are
/// present in the environment.
pub async fn new_client(endpoint_url: Option<&str>) -> Result<aws_sdk_dynamodb::Client, String> {
    let region = EnvironmentVariableRegionProvider::new()
        .region()
        .await
        .ok_or_else(|| "AWS region not set. Use AWS_REGION or AWS_DEFAULT_REGION.".to_string())?;

    EnvironmentVariableCredentialsProvider::new()
        .provide_credentials()
        .await
        .map_err(|err| format!("AWS credentials not found in environment: {err}"))?;

    let mut loader = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .credentials_provider(EnvironmentVariableCredentialsProvider::new());

    if let Some(url) = endpoint_url {
        loader = loader.endpoint_url(url);
    }

    let config = loader.load().await;
    Ok(aws_sdk_dynamodb::Client::new(&config))
}
