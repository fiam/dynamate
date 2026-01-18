use std::borrow::Cow;
use std::sync::Arc;

use aws_sdk_dynamodb::Client;
use crossterm::event::{Event, KeyCode, KeyModifiers};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    prelude::Widget,
    style::Style,
    text::Line,
    widgets::{Block, HighlightSpacing, List, ListItem, ListState, Paragraph, StatefulWidget},
};

use crate::{
    help,
    util::pad,
    widgets::{EnvHandle, QueryWidget, theme::Theme},
};

#[derive(Clone)]
pub struct TablePickerWidget {
    client: Arc<Client>,
    state: Arc<std::sync::RwLock<TablePickerState>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum LoadingState {
    #[default]
    Idle,
    Loading,
    Loaded,
    Error(String),
}

#[derive(Debug, Default)]
struct FilterInput {
    value: String,
    cursor: usize,
    is_active: bool,
}

impl FilterInput {
    fn is_active(&self) -> bool {
        self.is_active
    }

    fn set_active(&mut self, active: bool) {
        self.is_active = active;
        if active {
            self.cursor = self.value.len();
        }
    }

    fn clear(&mut self) {
        self.value.clear();
        self.cursor = 0;
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let style = if self.is_active {
            Style::default().fg(theme.secondary())
        } else {
            Style::default().fg(theme.neutral_variant())
        };
        let title = if self.is_active {
            "Filter (type to search)"
        } else {
            "Filter (/ to edit, esc to clear)"
        };
        let block = Block::bordered().title(title).style(style);
        let input = Paragraph::new(self.value.as_str()).block(block);
        input.render(area, frame.buffer_mut());

        if self.is_active {
            frame.set_cursor_position((area.x + self.cursor as u16 + 1, area.y + 1));
        }
    }

    fn handle_event(&mut self, event: &Event) -> bool {
        if !self.is_active {
            return false;
        }

        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Esc => {
                    self.set_active(false);
                }
                KeyCode::Enter => {
                    self.set_active(false);
                }
                KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.cursor = 0;
                }
                KeyCode::Char('e') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.cursor = self.value.len();
                }
                KeyCode::Backspace => {
                    if self.cursor > 0 && !self.value.is_empty() {
                        self.value.remove(self.cursor - 1);
                        self.cursor -= 1;
                    }
                }
                KeyCode::Delete => {
                    if self.cursor < self.value.len() {
                        self.value.remove(self.cursor);
                    }
                }
                KeyCode::Left => {
                    if self.cursor > 0 {
                        self.cursor -= 1;
                    }
                }
                KeyCode::Right => {
                    if self.cursor < self.value.len() {
                        self.cursor += 1;
                    }
                }
                KeyCode::Char(c) => {
                    self.value.insert(self.cursor, c);
                    self.cursor += 1;
                }
                _ => return false,
            }
            return true;
        }
        false
    }
}

#[derive(Debug, Default)]
struct TablePickerState {
    loading_state: LoadingState,
    tables: Vec<String>,
    filtered_tables: Vec<String>,
    list_state: ListState,
    filter: FilterInput,
}

impl TablePickerState {
    fn apply_filter(&mut self) {
        let filter = self.filter.value.trim().to_lowercase();
        let current = self.selected_table_name().map(|name| name.to_string());
        if filter.is_empty() {
            self.filtered_tables.clone_from(&self.tables);
        } else {
            self.filtered_tables = self
                .tables
                .iter()
                .filter(|name| name.to_lowercase().contains(&filter))
                .cloned()
                .collect();
        }

        if let Some(current) = current
            && let Some(index) = self
                .filtered_tables
                .iter()
                .position(|name| name == &current)
        {
            self.list_state.select(Some(index));
            return;
        }

        if self.filtered_tables.is_empty() {
            self.list_state.select(None);
        } else {
            self.list_state.select(Some(0));
        }
    }

    fn selected_table_name(&self) -> Option<&str> {
        self.list_state
            .selected()
            .and_then(|idx| self.filtered_tables.get(idx).map(String::as_str))
    }
}

impl TablePickerWidget {
    const HELP: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("/"),
            short: Cow::Borrowed("filter"),
            long: Cow::Borrowed("Filter tables"),
        },
        help::Entry {
            keys: Cow::Borrowed("enter"),
            short: Cow::Borrowed("select"),
            long: Cow::Borrowed("Open table"),
        },
        help::Entry {
            keys: Cow::Borrowed("j/k/up/down"),
            short: Cow::Borrowed("move"),
            long: Cow::Borrowed("Move selection"),
        },
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("clear"),
            long: Cow::Borrowed("Exit filter"),
        },
    ];

    pub fn new(client: Arc<Client>) -> Self {
        Self {
            client,
            state: Arc::new(std::sync::RwLock::new(TablePickerState::default())),
        }
    }

    async fn load(self, env: EnvHandle) {
        self.set_loading_state(LoadingState::Loading);
        env.invalidate();

        let result = self.fetch_tables().await;
        let mut state = self.state.write().unwrap();
        match result {
            Ok(tables) => {
                state.tables = tables;
                state.apply_filter();
                state.loading_state = LoadingState::Loaded;
            }
            Err(err) => {
                state.loading_state = LoadingState::Error(err);
            }
        }
        env.invalidate();
    }

    async fn fetch_tables(&self) -> Result<Vec<String>, String> {
        let mut table_names = Vec::new();
        let mut last_evaluated_table_name = None;

        loop {
            let output = self
                .client
                .list_tables()
                .set_exclusive_start_table_name(last_evaluated_table_name)
                .send()
                .await
                .map_err(|err| err.to_string())?;
            table_names.extend(output.table_names().iter().cloned());

            if output.last_evaluated_table_name().is_none() {
                break;
            }
            last_evaluated_table_name = output.last_evaluated_table_name().map(|s| s.to_string());
        }

        table_names.sort();
        Ok(table_names)
    }

    fn set_loading_state(&self, state: LoadingState) {
        self.state.write().unwrap().loading_state = state;
    }

    fn select_next(&self) {
        let mut state = self.state.write().unwrap();
        let len = state.filtered_tables.len();
        if len == 0 {
            return;
        }
        let next = match state.list_state.selected() {
            Some(index) => (index + 1).min(len - 1),
            None => 0,
        };
        state.list_state.select(Some(next));
    }

    fn select_previous(&self) {
        let mut state = self.state.write().unwrap();
        let len = state.filtered_tables.len();
        if len == 0 {
            return;
        }
        let next = match state.list_state.selected() {
            Some(index) => index.saturating_sub(1),
            None => 0,
        };
        state.list_state.select(Some(next));
    }

    fn handle_selection(&self, env: EnvHandle) -> bool {
        let selected = {
            self.state
                .read()
                .unwrap()
                .selected_table_name()
                .map(str::to_string)
        };
        if let Some(table_name) = selected {
            let widget = Arc::new(QueryWidget::new(self.client.clone(), &table_name));
            env.pop_widget();
            env.push_widget(widget);
            return true;
        }
        false
    }
}

impl crate::widgets::Widget for TablePickerWidget {
    fn start(&self, env: EnvHandle) {
        let this = self.clone();
        tokio::spawn(this.load(env));
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let mut state = self.state.write().unwrap();
        let layout = Layout::vertical([Constraint::Length(3), Constraint::Fill(1)]);
        let [filter_area, list_area] = area.layout(&layout);

        state.filter.render(frame, filter_area, theme);

        let block = Block::bordered()
            .title_top("Tables")
            .title_bottom(Line::styled(
                pad(format!("{} tables", state.filtered_tables.len()), 2),
                Style::default().fg(theme.neutral_variant()),
            ));

        match &state.loading_state {
            LoadingState::Loading => {
                let text = Paragraph::new("Loading tables...").block(block);
                frame.render_widget(text, list_area);
            }
            LoadingState::Error(message) => {
                let text = Paragraph::new(format!("Error: {message}")).block(block);
                frame.render_widget(text, list_area);
            }
            LoadingState::Idle | LoadingState::Loaded => {
                if state.filtered_tables.is_empty() {
                    let text = if state.tables.is_empty() {
                        "No tables found"
                    } else {
                        "No tables match filter"
                    };
                    let empty = Paragraph::new(text).block(block);
                    frame.render_widget(empty, list_area);
                    return;
                }

                let rows: Vec<ListItem> = state
                    .filtered_tables
                    .iter()
                    .map(|name| ListItem::new(Line::from(name.clone())))
                    .collect();
                let list = List::new(rows)
                    .block(block)
                    .highlight_symbol(">> ")
                    .highlight_spacing(HighlightSpacing::Always)
                    .highlight_style(Style::default().bg(theme.secondary()));

                StatefulWidget::render(list, list_area, frame.buffer_mut(), &mut state.list_state);
            }
        }
    }

    fn handle_event(&self, env: EnvHandle, event: &Event) -> bool {
        let filter_active = self.state.read().unwrap().filter.is_active();
        if filter_active {
            let mut state = self.state.write().unwrap();
            if state.filter.handle_event(event) {
                state.apply_filter();
                return true;
            }
        }

        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Char('/') => {
                    let mut state = self.state.write().unwrap();
                    state.filter.set_active(true);
                    return true;
                }
                KeyCode::Esc if filter_active => {
                    let mut state = self.state.write().unwrap();
                    state.filter.set_active(false);
                    state.filter.clear();
                    state.apply_filter();
                    return true;
                }
                KeyCode::Enter if !filter_active => {
                    return self.handle_selection(env);
                }
                KeyCode::Char('j') | KeyCode::Down => {
                    self.select_next();
                    return true;
                }
                KeyCode::Char('k') | KeyCode::Up => {
                    self.select_previous();
                    return true;
                }
                _ => {}
            }
        }
        false
    }

    fn help(&self) -> Option<&[help::Entry<'_>]> {
        Some(Self::HELP)
    }
}
