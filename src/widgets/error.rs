use anyhow::Error;
use ratatui::layout::Rect;

use crate::{util::fill_bg, widgets::{self, theme::Theme}};

struct ErrorPopup {
    err: anyhow::Error,
}

impl widgets::Widget for ErrorPopup {
    fn render(&self, frame: &mut ratatui::Frame, area: Rect, theme: &Theme) {
        fill_bg(frame.buffer_mut(), area, theme.neutral());
        let layout = Layout::vertical([Constraint::Fill(1), Constraint::Length(1)]);
        let [msg_area, button_area] = area.layout(&layout);
        //frame.render_widget(self, area);
    }
}

impl widgets::Popup for ErrorPopup {
    fn rect(&self, area: Rect) -> Rect {
        Rect {
            x: area.x + area.width / 4,
            y: area.y + area.height / 4,
            width: area.width / 2,
            height: area.height / 2,
        }
    }
}

pub fn Popup(err: anyhow::Error) -> impl widgets::Popup {
    ErrorPopup {
        err: err,
    }
}
