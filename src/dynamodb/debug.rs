use std::{
    env,
    future::Future,
    sync::OnceLock,
    time::{Duration, Instant},
};

use aws_sdk_dynamodb::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::RequestId;

const DEBUG_DELAY_ENV: &str = "DYNAMATE_DEBUG_DYNAMO_DELAY_MS";

/// Format an AWS SDK error into a concise, human-readable summary.
///
/// Prefers the service error's code, message, and request id when available,
/// falling back to the SDK's [`DisplayErrorContext`] rendering otherwise.
pub fn format_sdk_error<E>(err: &SdkError<E>) -> String
where
    E: ProvideErrorMetadata + RequestId + std::error::Error + 'static,
{
    if let Some(service_err) = err.as_service_error() {
        let code = service_err.code().unwrap_or("ServiceError");
        let message = service_err.message().unwrap_or("").trim();
        let mut summary = if message.is_empty() {
            code.to_string()
        } else {
            format!("{code}: {message}")
        };
        if let Some(request_id) = service_err.request_id() {
            summary.push_str(&format!(" (request id: {request_id})"));
        }
        return summary;
    }
    DisplayErrorContext(err).to_string()
}

pub async fn send_dynamo_request<F, Fut, T, E, FE>(
    span: tracing::Span,
    send: F,
    format_error: FE,
) -> Result<T, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    FE: FnOnce(&E) -> String,
{
    let _enter = span.enter();
    debug_dynamo_delay().await;
    let started = Instant::now();
    let result = send().await;
    let duration = started.elapsed();
    match &result {
        Ok(_) => {
            tracing::trace!(
                duration_ms = duration.as_millis(),
                "DynamoDB request complete"
            );
        }
        Err(err) => {
            tracing::warn!(
                duration_ms = duration.as_millis(),
                error = %format_error(err),
                "DynamoDB request complete"
            );
        }
    }
    result
}

async fn debug_dynamo_delay() {
    if let Some(delay) = debug_dynamo_delay_duration() {
        tracing::trace!(
            delay_ms = delay.as_millis(),
            "Applying debug DynamoDB delay"
        );
        tokio::time::sleep(delay).await;
    }
}

fn debug_dynamo_delay_duration() -> Option<Duration> {
    static DELAY: OnceLock<Option<Duration>> = OnceLock::new();
    *DELAY.get_or_init(|| {
        let Ok(raw) = env::var(DEBUG_DELAY_ENV) else {
            return None;
        };
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        match raw.parse::<u64>() {
            Ok(0) => None,
            Ok(ms) => Some(Duration::from_millis(ms)),
            Err(_) => {
                tracing::warn!(
                    env = DEBUG_DELAY_ENV,
                    value = %raw,
                    "Invalid DynamoDB debug delay"
                );
                None
            }
        }
    })
}
