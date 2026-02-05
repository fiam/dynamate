use std::{borrow::Cow, cell::RefCell};

use aws_sdk_dynamodb::types::AttributeValue;
use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    prelude::StatefulWidget,
    style::{Modifier, Style},
    text::Line,
    widgets::{Block, BorderType, HighlightSpacing, Row, Table, TableState},
};

use crate::{
    help,
    widgets::{Popup, WidgetInner, theme::Theme},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKind {
    Primary,
    Global,
    Local,
}

impl IndexKind {
    fn label(&self) -> &'static str {
        match self {
            IndexKind::Primary => "PK",
            IndexKind::Global => "GSI",
            IndexKind::Local => "LSI",
        }
    }
}

#[derive(Clone, Debug)]
pub struct IndexTarget {
    pub name: String,
    pub kind: IndexKind,
    pub hash_key: String,
    pub hash_value: AttributeValue,
    pub hash_display: String,
}

impl IndexTarget {
    fn display_name(&self) -> String {
        match self.kind {
            IndexKind::Primary => "Table (PK)".to_string(),
            _ => format!("{} ({})", self.name, self.kind.label()),
        }
    }

    fn display_hash(&self) -> String {
        format!("{}={}", self.hash_key, self.hash_display)
    }
}

pub struct IndexPicker {
    inner: WidgetInner,
    indices: Vec<IndexTarget>,
    state: RefCell<TableState>,
    on_select: Box<dyn Fn(IndexTarget) + Send + 'static>,
}

impl IndexPicker {
    const HELP: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("↑/↓/j/k"),
            short: Cow::Borrowed("move"),
            long: Cow::Borrowed("Move selection"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("⏎"),
            short: Cow::Borrowed("select"),
            long: Cow::Borrowed("Run index query"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("close"),
            long: Cow::Borrowed("Close picker"),
            ctrl: None,
            shift: None,
            alt: None,
        },
    ];

    pub fn new(
        indices: Vec<IndexTarget>,
        on_select: impl Fn(IndexTarget) + Send + 'static,
        parent: crate::env::WidgetId,
    ) -> Self {
        let mut state = TableState::default();
        if !indices.is_empty() {
            state.select(Some(0));
        }
        Self {
            inner: WidgetInner::new::<Self>(parent),
            indices,
            state: RefCell::new(state),
            on_select: Box::new(on_select),
        }
    }
}

impl crate::widgets::Widget for IndexPicker {
    fn inner(&self) -> &WidgetInner {
        &self.inner
    }

    fn help(&self) -> Option<&[help::Entry<'_>]> {
        Some(Self::HELP)
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .title(Line::styled(
                "Index items",
                Style::default()
                    .fg(theme.accent())
                    .add_modifier(Modifier::BOLD),
            ))
            .border_style(Style::default().fg(theme.border()))
            .style(Style::default().bg(theme.panel_bg()).fg(theme.text()));

        let header = Row::new(vec![
            Line::from("Index"),
            Line::from("Partition key"),
        ])
        .style(
            Style::default()
                .fg(theme.text_muted())
                .add_modifier(Modifier::BOLD),
        );

        let rows = self.indices.iter().map(|index| {
            Row::new(vec![
                Line::from(index.display_name()),
                Line::from(index.display_hash()),
            ])
        });

        let table = Table::new(rows, [Constraint::Length(24), Constraint::Fill(1)])
            .block(block)
            .header(header)
            .highlight_spacing(HighlightSpacing::Always)
            .highlight_symbol(">")
            .row_highlight_style(
                Style::default()
                    .bg(theme.selection_bg())
                    .fg(theme.selection_fg()),
            );

        let mut state = self.state.borrow_mut();
        StatefulWidget::render(table, area, frame.buffer_mut(), &mut state);
    }

    fn handle_event(&self, ctx: crate::env::WidgetCtx, event: &crossterm::event::Event) -> bool {
        let Some(key) = event.as_key_press_event() else {
            return true;
        };

        match key.code {
            KeyCode::Esc => {
                ctx.dismiss_popup();
                ctx.invalidate();
                true
            }
            KeyCode::Up | KeyCode::Char('k') => {
                let mut state = self.state.borrow_mut();
                state.scroll_up_by(1);
                ctx.invalidate();
                true
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let mut state = self.state.borrow_mut();
                state.scroll_down_by(1);
                ctx.invalidate();
                true
            }
            KeyCode::Enter => {
                if let Some(selected) = self.state.borrow().selected()
                    && let Some(target) = self.indices.get(selected).cloned()
                {
                    (self.on_select)(target);
                }
                ctx.dismiss_popup();
                ctx.invalidate();
                true
            }
            _ => true,
        }
    }
}

impl Popup for IndexPicker {
    fn rect(&self, area: Rect) -> Rect {
        let width = (area.width as f32 * 0.6) as u16;
        let height = (area.height as f32 * 0.5) as u16;
        let width = width.max(48).min(area.width.saturating_sub(4));
        let height = height.max(10).min(area.height.saturating_sub(4));
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
