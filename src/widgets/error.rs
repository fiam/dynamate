use ratatui::{
    Frame,
    layout::{Margin, Rect},
    style::{Modifier, Style},
    text::Line,
    widgets::{Block, BorderType, Paragraph, Wrap},
};

use crate::{
    util::{fill_bg, pad},
    widgets::{self, theme::Theme},
};

pub struct ErrorPopup {
    inner: std::sync::Arc<widgets::WidgetInner>,
    title: String,
    message: String,
}

impl ErrorPopup {
    pub fn new(
        title: impl Into<String>,
        message: impl Into<String>,
        parent: crate::env::WidgetId,
    ) -> Self {
        Self {
            inner: std::sync::Arc::new(widgets::WidgetInner::new::<Self>(parent)),
            title: title.into(),
            message: message.into(),
        }
    }
}

impl widgets::Widget for ErrorPopup {
    fn inner(&self) -> &widgets::WidgetInner {
        self.inner.as_ref()
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        fill_bg(frame.buffer_mut(), area, theme.panel_bg());
        let title = Line::styled(
            pad(&self.title, 2),
            Style::default()
                .fg(theme.error())
                .add_modifier(Modifier::BOLD),
        )
        .centered();
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .title(title)
            .border_style(Style::default().fg(theme.error()))
            .style(Style::default().bg(theme.panel_bg()).fg(theme.text()));

        let inner = area.inner(Margin::new(1, 1));
        let body = Paragraph::new(self.message.as_str())
            .style(Style::default().fg(theme.text()))
            .wrap(Wrap { trim: true })
            .block(block);
        frame.render_widget(body, inner);
    }
}

impl widgets::Popup for ErrorPopup {
    fn rect(&self, area: Rect) -> Rect {
        let width = (area.width as f32 * 0.6) as u16;
        let height = (area.height as f32 * 0.4) as u16;
        let width = width.max(40).min(area.width.saturating_sub(4));
        let height = height.max(8).min(area.height.saturating_sub(4));
        let x = area.x + (area.width - width) / 2;
        let y = area.y + (area.height - height) / 2;
        Rect {
            x,
            y,
            width,
            height,
        }
    }
}
