use std::borrow::Cow;

use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Paragraph, Wrap},
};
use unicode_width::UnicodeWidthStr;

mod widget;

pub use widget::Widget;

use crate::widgets::theme::Theme;

#[derive(Clone)]
pub struct Entry<'a> {
    pub keys: Cow<'a, str>,
    pub short: Cow<'a, str>,
    pub long: Cow<'a, str>,
}

impl<'a> Entry<'a> {
    fn into_owned(&self) -> Entry<'static> {
        Entry {
            keys: Cow::Owned(self.keys.as_ref().to_owned()),
            short: Cow::Owned(self.short.as_ref().to_owned()),
            long: Cow::Owned(self.long.as_ref().to_owned()),
        }
    }
}

fn make_spans<'a>(entries: &'a [&Entry<'a>], theme: &Theme) -> Vec<Span<'a>> {
    let mut spans: Vec<_> = entries
        .iter()
        .flat_map(|entry| {
            let keys = &entry.keys;
            [
                Span::styled(format!("[{keys}]"), Style::default().bold()),
                Span::raw(" "),
                Span::raw(entry.short.to_string()),
                Span::styled(" • ", Style::default().fg(theme.neutral())),
            ]
        })
        .collect();
    // Remove the last span
    if spans.len() > 0 {
        spans.pop();
    }
    spans
}

pub fn height<'a>(entries: &'a [&Entry<'a>], area: Rect) -> u16 {
    let theme = Theme::default();
    let total_width: usize = make_spans(entries, &theme)
        .iter()
        .map(|s| s.content.width())
        .sum();
    let available_width = area.width as usize;

    // number of rows needed = ceil(total_width / available_width)
    ((total_width + available_width - 1) / available_width) as u16
}

pub fn render<'a>(entries: &'a [&Entry<'a>], frame: &mut Frame, area: Rect, theme: &Theme) {
    let spans = make_spans(entries, theme);
    let footer = Paragraph::new(Line::from(spans))
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true });

    frame.render_widget(footer, area);
}
