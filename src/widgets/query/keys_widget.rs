use std::{borrow::Cow, sync::{Arc, RwLock}};

use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    buffer::Buffer,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{
        Color, Modifier, Style, Stylize,
        palette::tailwind::{BLUE, GREEN, SLATE},
    },
    symbols,
    text::{Line, Span},
    widgets::{
        Block, Borders, HighlightSpacing, List, ListItem, ListState, Padding, Paragraph,
        StatefulWidget, TableState, Wrap,
    },
};

use crate::{help, widgets::{EnvHandle, Popup}};

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
    list_state: ListState,
}

#[derive(Debug, Clone)]
pub enum Event {
    KeyHidden(String),
    KeyUnhidden(String),
}

impl KeysWidget {
    const HELP: &'static [help::Entry<'static>] = &[
        help::Entry { keys: Cow::Borrowed("↑/↓/i/j"), short: Cow::Borrowed("move"), long: Cow::Borrowed("Move the selected field up/down") },
        help::Entry { keys: Cow::Borrowed("Space"), short: Cow::Borrowed("toggle"), long: Cow::Borrowed("Select/deselect the current field") },
        help::Entry { keys: Cow::Borrowed("a"), short: Cow::Borrowed("all"), long: Cow::Borrowed("Select all fields") },
        help::Entry { keys: Cow::Borrowed("n"), short: Cow::Borrowed("none"), long: Cow::Borrowed("Deselect all fields") },
    ];
    pub fn new(keys: &[Key], on_event: impl Fn(Event) + Send + Sync + 'static) -> Self {
        let mut state = KeysWidgetState::default();
        state.keys = keys.to_vec();
        state.list_state.select(Some(0));
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

    fn render(&self, frame: &mut Frame, area: Rect) {
        frame.render_widget(self, area);
    }

    fn handle_event(
        &self,
        _env: EnvHandle,
        event: &crossterm::event::Event,
    ) -> bool {
        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Down => {
                    self.state.write().unwrap().list_state.scroll_down_by(1);
                }
                KeyCode::Up => {
                    self.state.write().unwrap().list_state.scroll_up_by(1);
                }
                KeyCode::Char(' ') => {
                    let mut state = self.state.write().unwrap();
                    if let Some(selected) = state.list_state.selected() {
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
                KeyCode::Char('a')  => {
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

impl ratatui::widgets::Widget for &KeysWidget {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let mut state = self.state.write().unwrap();

        let block = Block::bordered()
            .title(Line::raw("Show fields").centered())
            .padding(Padding::new(1, 1, 1, 0));

        // Iterate through all elements in the `items` and stylize them.
        let items: Vec<ListItem> = state
            .keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                //let color = alternate_colors(i);
                ListItem::from(key)
            })
            .collect();

        // Create a List from all list items and highlight the currently selected one
        let list = List::new(items)
            .block(block)
            //.highlight_style(SELECTED_STYLE)
            .highlight_symbol("> ")
            .highlight_spacing(HighlightSpacing::Always);

        let list_area = area;
        StatefulWidget::render(list, list_area, buf, &mut state.list_state);
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

const fn alternate_colors(i: usize) -> Color {
    if i % 2 == 0 {
        NORMAL_ROW_BG
    } else {
        ALT_ROW_BG_COLOR
    }
}

impl From<&Key> for ListItem<'_> {
    fn from(value: &Key) -> Self {
        let text = if !value.hidden {
            format!(" ✓ {}", value.name)
        } else {
            format!(" ☐ {}", value.name)
        };
        ListItem::new(Line::from(text))
    }
}

fn key(s: &str) -> Span<'_> {
    // render like a small “keycap”
    Span::styled(format!("[{s}]"), Style::default().bold().fg(Color::White))
}
