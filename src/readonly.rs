use std::sync::OnceLock;
use std::time::Duration;

static READONLY: OnceLock<bool> = OnceLock::new();

pub fn set(value: bool) {
    let _ = READONLY.set(value);
}

pub fn is_enabled() -> bool {
    READONLY.get().copied().unwrap_or(false)
}

pub const REJECT_MESSAGE: &str = "Read-only mode: write operations are disabled";
pub const TOAST_DURATION: Duration = Duration::from_secs(3);
