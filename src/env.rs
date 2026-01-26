use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::widgets::{Popup, Widget};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WidgetId(String);

impl WidgetId {
    pub fn new(type_name: &str, suffix: &str) -> Self {
        Self(format!("{type_name}-{suffix}"))
    }

    pub fn app() -> Self {
        Self("app".to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub enum AppCommand {
    Invalidate,
    ForceRedraw,
    PushWidget(Arc<dyn Widget>),
    PopWidget,
    SetPopup(Arc<dyn Popup>),
    DismissPopup,
    ShowToast(Toast),
}

#[derive(Clone)]
pub struct AppEvent {
    pub source: WidgetId,
    pub payload: Arc<dyn Any + Send + Sync>,
}

impl AppEvent {
    pub fn new<T: Any + Send + Sync>(source: WidgetId, payload: T) -> Self {
        Self {
            source,
            payload: Arc::new(payload),
        }
    }

    pub fn payload<T: Any>(&self) -> Option<&T> {
        self.payload.as_ref().downcast_ref::<T>()
    }
}

#[derive(Debug, Clone)]
pub enum WidgetEvent {
    Created { id: WidgetId, parent: WidgetId },
    Started { id: WidgetId },
    Closed { id: WidgetId },
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum ToastKind {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone)]
pub struct Toast {
    pub message: String,
    pub kind: ToastKind,
    pub duration: Duration,
}

#[derive(Clone)]
pub struct AppBus {
    cmd_tx: UnboundedSender<AppCommand>,
    event_tx: UnboundedSender<AppEvent>,
}

pub struct AppBusRx {
    pub cmd_rx: UnboundedReceiver<AppCommand>,
    pub event_rx: UnboundedReceiver<AppEvent>,
}

impl AppBus {
    pub fn new() -> (Self, AppBusRx) {
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel::<AppCommand>();
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<AppEvent>();
        (Self { cmd_tx, event_tx }, AppBusRx { cmd_rx, event_rx })
    }

    pub fn command(&self, cmd: AppCommand) {
        let _ = self.cmd_tx.send(cmd);
    }

    pub fn broadcast(&self, event: AppEvent) {
        let _ = self.event_tx.send(event);
    }
}

#[derive(Clone)]
pub struct WidgetCtx {
    pub id: WidgetId,
    pub parent: WidgetId,
    bus: AppBus,
    self_tx: UnboundedSender<AppEvent>,
}

impl WidgetCtx {
    pub(crate) fn new(
        id: WidgetId,
        parent: WidgetId,
        bus: AppBus,
        self_tx: UnboundedSender<AppEvent>,
    ) -> Self {
        Self {
            id,
            parent,
            bus,
            self_tx,
        }
    }

    pub fn invalidate(&self) {
        self.bus.command(AppCommand::Invalidate);
    }

    pub fn force_redraw(&self) {
        self.bus.command(AppCommand::ForceRedraw);
    }

    pub fn push_widget(&self, widget: Arc<dyn Widget>) {
        self.bus.command(AppCommand::PushWidget(widget));
    }

    pub fn pop_widget(&self) {
        self.bus.command(AppCommand::PopWidget);
    }

    pub fn set_popup(&self, popup: Arc<dyn Popup>) {
        self.bus.command(AppCommand::SetPopup(popup));
    }

    pub fn dismiss_popup(&self) {
        self.bus.command(AppCommand::DismissPopup);
    }

    pub fn show_toast(&self, toast: Toast) {
        self.bus.command(AppCommand::ShowToast(toast));
    }

    pub fn emit_self<T: Any + Send + Sync>(&self, payload: T) {
        let event = AppEvent::new(self.id.clone(), payload);
        let _ = self.self_tx.send(event);
    }

    pub fn broadcast_event<T: Any + Send + Sync>(&self, payload: T) {
        let event = AppEvent::new(self.id.clone(), payload);
        self.bus.broadcast(event);
    }
}
