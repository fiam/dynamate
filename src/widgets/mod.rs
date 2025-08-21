use std::fmt::Debug;

use crossterm::event::Event;
use ratatui::{layout::Rect, Frame};

mod pull_requests;

pub use pull_requests::PullRequestListWidget;

pub trait Widget: Debug + Send + Sync
// where
//     for<'a> &'a Self: ratatui::widgets::Widget, // <-- “references to this type implement ratatui::Widget”
{
    /// Start any background work (make it &self for Arc; use interior mutability for state)
    fn start(&self) {
        // Start any background tasks or initialization here
    }

    /// Render the widget's content.
    fn render(&self, frame: &mut Frame, area: Rect);

    /// Handle input events. Returns true if the event was handled.
    fn handle_event(&self, _event: &Event) -> bool {
        false
    }
}
