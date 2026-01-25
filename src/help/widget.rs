use std::sync::{Arc, RwLock};

use crossterm::event::{Event, KeyCode, KeyModifiers};
use ratatui::{
    Frame,
    layout::{Constraint, Margin, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Padding, Row, Table},
};

use crate::{
    help::{Entry, ModDisplay},
    util::{fill_bg, pad},
    widgets::{EnvHandle, Popup, theme::Theme},
};

pub struct Widget {
    entries: Vec<Entry<'static>>,
    modifiers: Arc<RwLock<KeyModifiers>>,
    mode: Arc<RwLock<ModDisplay>>,
}

impl Widget {
    pub fn new<'a>(
        entries: Vec<&Entry<'a>>,
        modifiers: Arc<RwLock<KeyModifiers>>,
        mode: Arc<RwLock<ModDisplay>>,
    ) -> Self {
        Self {
            entries: entries.into_iter().map(|e| e.to_owned_entry()).collect(),
            modifiers,
            mode,
        }
    }
}

impl crate::widgets::Widget for Widget {
    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let buf = frame.buffer_mut();
        fill_bg(buf, area, theme.panel_bg());
        let title = Line::styled(
            pad("Help", 2),
            Style::default()
                .fg(theme.accent())
                .add_modifier(Modifier::BOLD),
        )
        .centered();
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .title(title)
            .border_style(Style::default().fg(theme.border()))
            .style(Style::default().bg(theme.panel_bg()).fg(theme.text()))
            .padding(Padding::new(2, 2, 1, 1));

        let inner = area.inner(Margin::new(1, 1));

        let modifiers = *self.modifiers.read().unwrap();
        let mode = *self.mode.read().unwrap();
        let visible: Vec<_> = self
            .entries
            .iter()
            .filter_map(|entry| {
                let display = entry.display(modifiers, mode);
                if display.keys.is_empty() {
                    return None;
                }
                Some(display)
            })
            .collect();

        let rows: Vec<_> = visible
            .chunks(2)
            .map(|chunk| {
                let left_key = chunk
                    .first()
                    .map(|e| make_display_key(e, theme))
                    .unwrap_or_default();
                let left_desc = chunk
                    .first()
                    .map(|e| Span::styled(e.long.as_ref(), Style::default().fg(theme.text())))
                    .unwrap_or_default();
                let right_key = chunk
                    .get(1)
                    .map(|e| make_display_key(e, theme))
                    .unwrap_or_default();
                let right_desc = chunk
                    .get(1)
                    .map(|e| Span::styled(e.long.as_ref(), Style::default().fg(theme.text())))
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
            Constraint::Length(12),
            Constraint::Fill(1),
            Constraint::Length(12),
            Constraint::Fill(1),
        ];
        let table = Table::new(rows, widths)
            .block(block)
            .style(Style::default().fg(theme.text()));

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

fn make_display_key(entry: &crate::help::DisplayEntry<'_>, theme: &Theme) -> Span<'static> {
    let keys = entry.keys.as_ref();
    Span::styled(
        format!("[{keys}]"),
        Style::default()
            .bold()
            .fg(theme.accent_alt())
            .add_modifier(Modifier::BOLD),
    )
}
