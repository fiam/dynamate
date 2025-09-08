use std::borrow::Cow;

use ratatui::{layout::{Alignment, Rect}, style::{Color, Style}, text::{Line, Span}, widgets::{Paragraph, Wrap}, Frame};
use unicode_width::UnicodeWidthStr;

pub struct Entry<'a> {
    pub keys:  Cow<'a, str>,
    pub short: Cow<'a, str>,
    pub long:  Cow<'a, str>,
}

fn make_spans<'a>(entries: &'a [&Entry<'a>]) -> Vec<Span<'a>> {
    let mut spans: Vec<_> = entries.iter().flat_map(|entry| {
        let keys = &entry.keys;
        [
            Span::styled(format!("[{keys}]"), Style::default().bold().fg(Color::White)),
            Span::raw(" "),
            Span::raw(entry.short.to_string()),
            Span::raw("   "),
        ]
    }).collect();
    // Remove the last span
    if spans.len() > 0 {
        spans.pop();
    }
    spans
}

pub fn height<'a>(entries: &'a [&Entry<'a>],
    area: Rect) -> u16 {

    let total_width: usize = make_spans(entries).iter().map(|s| s.content.width()).sum();
    let available_width = area.width as usize;

    // number of rows needed = ceil(total_width / available_width)
    ((total_width + available_width - 1) / available_width) as u16
}

pub fn render<'a>(
    entries: &'a [&Entry<'a>],
    frame: &mut Frame,
    area: Rect,
) {
    let spans = make_spans(entries);
    let footer = Paragraph::new(Line::from(spans))
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true });

   frame.render_widget(footer, area);
}
