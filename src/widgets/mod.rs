use std::sync::Arc;

use crossterm::event::Event;
use ratatui::{Frame, layout::Rect};
use theme::Theme;

mod query;
mod table_picker;
pub mod theme;

pub use query::QueryWidget;
pub use table_picker::TablePickerWidget;

use crate::help;

pub trait Env {
    fn invalidate(&self);
    fn force_redraw(&self);
    fn push_widget(&self, widget: Arc<dyn Widget>);
    fn pop_widget(&self);
    fn set_popup(&self, popup: Arc<dyn Popup>);
    fn dismiss_popup(&self);
}

pub type EnvHandle = Arc<dyn Env + Send + Sync>;

pub trait Widget: Send + Sync
// where
//     for<'a> &'a Self: ratatui::widgets::Widget, // <-- “references to this type implement ratatui::Widget”
{
    /// Start any background work (make it &self for Arc; use interior mutability for state)
    fn start(&self, _env: EnvHandle) {
        // Start any background tasks or initialization here
    }

    /// Render the widget's content.
    fn render(&self, _frame: &mut Frame, _area: Rect, _theme: &Theme) {
        //frame.render_widget(self, area);
    }

    /// Handle input events. Returns true if the event was handled.
    fn handle_event(&self, _env: EnvHandle, _event: &Event) -> bool {
        false
    }

    /// Optional help to display at the bottom while this widget is active
    fn help(&self) -> Option<&[help::Entry<'_>]> {
        None
    }
}

pub trait Popup: Widget {
    fn rect(&self, area: Rect) -> Rect;
}
