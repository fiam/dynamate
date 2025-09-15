use std::{
    borrow::Cow,
    sync::{Arc, RwLock},
};

use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    buffer::Buffer,
    layout::{Alignment, Constraint, Layout, Margin, Rect},
    style::{
        Color, Modifier, Style, Stylize,
        palette::tailwind::{BLUE, GREEN, SLATE},
    },
    symbols,
    text::{Line, Span, Text},
    widgets::{
        Block, Borders, HighlightSpacing, List, ListItem, ListState, Padding, Paragraph, Row,
        StatefulWidget, Table, TableState, Wrap,
    },
};

use crate::{
    help,
    util::{fill_bg, pad},
    widgets::{EnvHandle, Popup, theme},
};

const TODO_HEADER_STYLE: Style = Style::new().fg(SLATE.c100).bg(BLUE.c800);
const NORMAL_ROW_BG: Color = SLATE.c950;
const ALT_ROW_BG_COLOR: Color = SLATE.c900;
const SELECTED_STYLE: Style = Style::new().bg(SLATE.c800).add_modifier(Modifier::BOLD);
const TEXT_FG_COLOR: Color = SLATE.c200;
const COMPLETED_TEXT_FG_COLOR: Color = GREEN.c500;

#[derive(Clone)]
pub struct KeysWidget {
    on_event: Arc<dyn Fn(Event) + Send + Sync + 'static>,
    state: Arc<RwLock<KeysWidgetState>>,
}

#[derive(Debug, Clone)]
pub struct Key {
    pub name: String,
    pub hidden: bool,
}

#[derive(Debug, Default)]
struct KeysWidgetState {
    keys: Vec<Key>,
    table_state: TableState,
}

#[derive(Debug, Clone)]
pub enum Event {
    KeyHidden(String),
    KeyUnhidden(String),
}

impl KeysWidget {
    const HELP: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("↑/↓/i/j"),
            short: Cow::Borrowed("move"),
            long: Cow::Borrowed("Move the selected field up/down"),
        },
        help::Entry {
            keys: Cow::Borrowed("Space"),
            short: Cow::Borrowed("toggle"),
            long: Cow::Borrowed("Select/deselect the current field"),
        },
        help::Entry {
            keys: Cow::Borrowed("a"),
            short: Cow::Borrowed("all"),
            long: Cow::Borrowed("Select all fields"),
        },
        help::Entry {
            keys: Cow::Borrowed("n"),
            short: Cow::Borrowed("none"),
            long: Cow::Borrowed("Deselect all fields"),
        },
    ];
    pub fn new(keys: &[Key], on_event: impl Fn(Event) + Send + Sync + 'static) -> Self {
        let mut state = KeysWidgetState::default();
        state.keys = keys.to_vec();
        state.table_state.select(Some(0));
        Self {
            state: Arc::new(RwLock::new(state)),
            on_event: Arc::new(on_event),
        }
    }

    fn update_all(&self, hidden: bool) {
        let mut state = self.state.write().unwrap();
        for key in state.keys.iter_mut() {
            if key.hidden != hidden {
                key.hidden = hidden;
                let event = if hidden {
                    Event::KeyHidden(key.name.clone())
                } else {
                    Event::KeyUnhidden(key.name.clone())
                };
                (self.on_event)(event);
            }
        }
    }
}

impl crate::widgets::Widget for KeysWidget {
    fn help(&self) -> Option<&[help::Entry<'_>]> {
        Some(Self::HELP)
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &theme::Theme) {
        fill_bg(frame.buffer_mut(), area, theme.neutral());
        let mut state = self.state.write().unwrap();

        let block = Block::bordered()
            .title(Line::raw(pad("Show fields", 2)).centered())
            .padding(Padding::new(1, 1, 1, 0));

        // Iterate through all elements in the `items` and stylize them.
        let rows: Vec<Row> = state
            .keys
            .iter()
            .map(|key| {
                let left = if key.hidden {
                    Span::raw("")
                } else {
                    Span::styled("✓", Style::default().fg(theme.primary()))
                };
                let name = key.name.clone();
                let right = if key.hidden {
                    Span::styled(name, Style::default().add_modifier(Modifier::DIM))
                } else {
                    Span::raw(name)
                };
                Row::new(vec![left, right])
            })
            .collect();
        // Create a Table from all list items and highlight the currently selected one
        let widths = &[Constraint::Length(3), Constraint::Fill(1)];
        let table = Table::new(rows, widths)
            .block(block)
            //.highlight_style(SELECTED_STYLE)
            .highlight_symbol(Text::styled(">  ", Style::default().fg(theme.secondary())))
            .highlight_spacing(HighlightSpacing::Always);

        let mut table_area = area.inner(Margin::new(1, 0));
        table_area.y += 1;
        table_area.height -= 1;
        StatefulWidget::render(
            table,
            table_area,
            frame.buffer_mut(),
            &mut state.table_state,
        );
    }

    fn handle_event(&self, _env: EnvHandle, event: &crossterm::event::Event) -> bool {
        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Down => {
                    self.state.write().unwrap().table_state.scroll_down_by(1);
                }
                KeyCode::Up => {
                    self.state.write().unwrap().table_state.scroll_up_by(1);
                }
                KeyCode::Char(' ') => {
                    let mut state = self.state.write().unwrap();
                    if let Some(selected) = state.table_state.selected() {
                        if let Some(key) = state.keys.get_mut(selected) {
                            key.hidden = !key.hidden;
                            let event = if key.hidden {
                                Event::KeyHidden(key.name.clone())
                            } else {
                                Event::KeyUnhidden(key.name.clone())
                            };
                            (self.on_event)(event);
                        }
                    }
                }
                KeyCode::Char('a') => {
                    self.update_all(false);
                }
                KeyCode::Char('n') => {
                    self.update_all(true);
                }
                _ => {
                    return false; // not handled
                }
            }
            return true;
        }
        false
    }
}

impl Popup for KeysWidget {
    fn rect(&self, area: Rect) -> Rect {
        Rect {
            x: area.x + area.width / 4,
            y: area.y + area.height / 4,
            width: area.width / 2,
            height: area.height / 2,
        }
    }
}

fn key(s: &str) -> Span<'_> {
    // render like a small “keycap”
    Span::styled(format!("[{s}]"), Style::default().bold().fg(Color::White))
}
