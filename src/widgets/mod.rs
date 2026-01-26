use std::any::type_name;

use crossterm::event::Event;
use rand::{Rng, distributions::Alphanumeric};
use ratatui::{Frame, layout::Rect};
use theme::Theme;

pub mod error;
mod query;
mod table_picker;
pub mod theme;

pub use query::QueryWidget;
pub use table_picker::TablePickerWidget;

use crate::env::{AppBus, AppEvent, WidgetCtx, WidgetId};
use crate::help;

pub struct WidgetInner {
    id: WidgetId,
    parent: WidgetId,
    self_tx: tokio::sync::mpsc::UnboundedSender<AppEvent>,
    self_rx: std::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<AppEvent>>,
}

impl WidgetInner {
    pub fn new<T: 'static>(parent: WidgetId) -> Self {
        let type_name = type_basename(type_name::<T>());
        let suffix: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();
        let (self_tx, self_rx) = tokio::sync::mpsc::unbounded_channel::<AppEvent>();
        Self {
            id: WidgetId::new(type_name, &suffix),
            parent,
            self_tx,
            self_rx: std::sync::Mutex::new(self_rx),
        }
    }

    pub fn id(&self) -> WidgetId {
        self.id.clone()
    }

    pub fn parent(&self) -> WidgetId {
        self.parent.clone()
    }

    pub fn ctx(&self, bus: AppBus) -> WidgetCtx {
        WidgetCtx::new(self.id.clone(), self.parent(), bus, self.self_tx.clone())
    }

    pub fn drain_self_events(&self) -> Vec<AppEvent> {
        let mut rx = self.self_rx.lock().unwrap();
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }
}

fn type_basename(full: &str) -> &str {
    full.rsplit("::").next().unwrap_or(full)
}

pub trait Widget: Send + Sync {
    fn inner(&self) -> &WidgetInner;

    fn id(&self) -> WidgetId {
        self.inner().id()
    }

    /// Start any background work (use interior mutability for state).
    fn start(&self, _ctx: WidgetCtx) {}

    /// Render the widget's content.
    fn render(&self, _frame: &mut Frame, _area: Rect, _theme: &Theme) {}

    /// Handle input events. Returns true if the event was handled.
    fn handle_event(&self, _ctx: WidgetCtx, _event: &Event) -> bool {
        false
    }

    /// Optional help to display at the bottom while this widget is active.
    fn help(&self) -> Option<&[help::Entry<'_>]> {
        None
    }

    /// Whether global help should be suppressed (e.g., when capturing text input).
    fn suppress_global_help(&self) -> bool {
        false
    }

    /// Receive broadcast events sent by other widgets.
    fn on_app_event(&self, _ctx: WidgetCtx, _event: &AppEvent) {}

    /// Receive events emitted by this widget itself.
    fn on_self_event(&self, _ctx: WidgetCtx, _event: &AppEvent) {}
}

pub trait Popup: Widget {
    fn rect(&self, area: Rect) -> Rect;
}
