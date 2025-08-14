use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::config::ProvideCredentials;
use color_eyre::Result;

pub async fn new_client(endpoint_url: Option<&str>) -> Result<aws_sdk_dynamodb::Client> {
    let base_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let has_credentials = match base_config.credentials_provider() {
        Some(provider) => provider.provide_credentials().await.is_ok(),
        None => false,
    };
    let mut builder = aws_sdk_dynamodb::config::Builder::from(&base_config);
    if let Some(url) = endpoint_url {
        builder = builder.endpoint_url(url);
        if !has_credentials {
            // Provide dummy credentials
            let provider = aws_credential_types::Credentials::from_keys("key", "secret", None);
            builder = builder.credentials_provider(provider);
        }
        if base_config.region().is_none() {
            // If no region is set, set a default one
            builder = builder.region(aws_sdk_dynamodb::config::Region::new("us-east-1"));
        }
    }
    let config = builder.build();
    let client = aws_sdk_dynamodb::Client::from_conf(config);
    Ok(client)
}
