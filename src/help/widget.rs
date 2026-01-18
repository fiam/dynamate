use crossterm::event::{Event, KeyCode};
use ratatui::{
    Frame,
    layout::{Constraint, Margin, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{Block, BorderType, Padding, Row, Table},
};

use crate::{
    help::Entry,
    util::{fill_bg, pad},
    widgets::{EnvHandle, Popup, theme::Theme},
};

pub struct Widget {
    entries: Vec<Entry<'static>>,
}

impl Widget {
    pub fn new<'a>(entries: Vec<&Entry<'a>>) -> Self {
        Self {
            entries: entries.into_iter().map(|e| e.to_owned_entry()).collect(),
        }
    }
}

impl crate::widgets::Widget for Widget {
    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let buf = frame.buffer_mut();
        fill_bg(buf, area, theme.neutral());
        let title = Line::styled(pad("Help", 2), Style::default().fg(theme.primary())).centered();
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .title(title)
            .padding(Padding::new(5, 5, 5, 5));

        let inner = area.inner(Margin::new(1, 1));

        let rows: Vec<_> = self
            .entries
            .chunks(2)
            .map(|chunk| {
                let left_key = chunk
                    .first()
                    .map(|e| make_key(e, theme))
                    .unwrap_or_default();
                let left_desc = chunk
                    .first()
                    .map(|e| Span::raw(e.long.as_ref()))
                    .unwrap_or_default();
                let right_key = chunk.get(1).map(|e| make_key(e, theme)).unwrap_or_default();
                let right_desc = chunk
                    .get(1)
                    .map(|e| Span::raw(e.long.as_ref()))
                    .unwrap_or_default();
                Row::new(vec![
                    Line::from(left_key),
                    Line::from(left_desc),
                    Line::from(right_key),
                    Line::from(right_desc),
                ])
            })
            .collect();

        let widths = &[
            Constraint::Percentage(10),
            Constraint::Percentage(40),
            Constraint::Percentage(10),
            Constraint::Percentage(40),
        ];
        let table = Table::new(rows, widths).block(block);

        ratatui::widgets::Widget::render(table, inner, buf);
    }

    fn handle_event(&self, env: EnvHandle, event: &Event) -> bool {
        if let Some(key) = event.as_key_press_event()
            && let KeyCode::Char('h') = key.code
        {
            env.dismiss_popup();
            return true;
        }
        false
    }
}

impl Popup for Widget {
    fn rect(&self, area: Rect) -> Rect {
        let width = area.width / 2;
        let height = area.height / 2;
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

fn make_key(entry: &Entry<'static>, theme: &Theme) -> Span<'static> {
    let keys = &entry.keys;
    Span::styled(
        format!("[{keys}]"),
        Style::default().bold().fg(theme.secondary()),
    )
}
