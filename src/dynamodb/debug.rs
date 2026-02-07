use std::{
    env,
    future::Future,
    sync::OnceLock,
    time::{Duration, Instant},
};

const DEBUG_DELAY_ENV: &str = "DYNAMATE_DEBUG_DYNAMO_DELAY_MS";

pub async fn send_dynamo_request<F, Fut, T, E>(send: F) -> (Result<T, E>, Duration)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    debug_dynamo_delay().await;
    let started = Instant::now();
    let result = send().await;
    (result, started.elapsed())
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
