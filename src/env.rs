use std::{sync::Arc, time::Duration};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::widgets::{Popup, Widget};

pub enum Message {
    // Invalidate the current frame and request a redraw
    Invalidate,
    ForceRedraw,
    PushWidget(Arc<dyn Widget>),
    PopWidget,
    SetPopup(Arc<dyn Popup>),
    DismissPopup,
    ShowToast(Toast),
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

pub struct Env {
    tx: Arc<EnvTx>,
    rx: UnboundedReceiver<Message>,
}

struct EnvTx {
    tx: UnboundedSender<Message>,
}

impl EnvTx {
    fn new(tx: UnboundedSender<Message>) -> Self {
        EnvTx { tx }
    }

    fn send(&self, msg: Message) {
        let _ = self.tx.send(msg);
    }
}

impl crate::widgets::Env for EnvTx {
    fn invalidate(&self) {
        self.send(Message::Invalidate);
    }

    fn force_redraw(&self) {
        self.send(Message::ForceRedraw);
    }

    fn push_widget(&self, widget: Arc<dyn Widget>) {
        self.send(Message::PushWidget(widget));
    }

    fn pop_widget(&self) {
        self.send(Message::PopWidget);
    }

    fn set_popup(&self, popup: Arc<dyn Popup>) {
        self.send(Message::SetPopup(popup));
    }

    fn dismiss_popup(&self) {
        self.send(Message::DismissPopup);
    }

    fn show_toast(&self, toast: Toast) {
        self.send(Message::ShowToast(toast));
    }

}

impl Env {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        Env {
            tx: Arc::new(EnvTx::new(tx)),
            rx,
        }
    }

    pub fn tx(&self) -> Arc<dyn crate::widgets::Env + Send + Sync> {
        self.tx.clone()
    }

    pub fn rx(&mut self) -> &mut UnboundedReceiver<Message> {
        &mut self.rx
    }
}
