use std::sync::Arc;
use std::{borrow::Cow, time::Duration};

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
use unicode_width::UnicodeWidthStr;

use crate::{
    env::{Toast, ToastKind},
    help,
    util::pad,
    widgets::{QueryWidget, WidgetInner, error::ErrorPopup, theme::Theme},
};

#[derive(Clone)]
pub struct TablePickerWidget {
    inner: Arc<WidgetInner>,
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
        let block = Block::bordered()
            .title("Filter")
            .style(Style::default().bg(theme.panel_bg_alt()).fg(theme.text()))
            .border_style(Style::default().fg(theme.accent()));
        let input = Paragraph::new(self.value.as_str()).block(block);
        input.render(area, frame.buffer_mut());

        frame.set_cursor_position((area.x + self.cursor as u16 + 1, area.y + 1));
    }

    fn handle_event(&mut self, event: &Event) -> bool {
        if !self.is_active {
            return false;
        }

        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Esc => {
                    self.clear();
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
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("⏎"),
            short: Cow::Borrowed("select"),
            long: Cow::Borrowed("Open table"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("j/k/↑/↓"),
            short: Cow::Borrowed("move"),
            long: Cow::Borrowed("Move selection"),
            ctrl: None,
            shift: None,
            alt: None,
        },
    ];
    const HELP_FILTER_EDIT: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("clear"),
            long: Cow::Borrowed("Clear filter"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("⏎"),
            short: Cow::Borrowed("apply"),
            long: Cow::Borrowed("Apply filter"),
            ctrl: None,
            shift: None,
            alt: None,
        },
    ];
    const HELP_FILTER_APPLIED: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("/"),
            short: Cow::Borrowed("filter"),
            long: Cow::Borrowed("Edit filter"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("clear filter"),
            long: Cow::Borrowed("Clear filter"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("⏎"),
            short: Cow::Borrowed("select"),
            long: Cow::Borrowed("Open table"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("j/k/↑/↓"),
            short: Cow::Borrowed("move"),
            long: Cow::Borrowed("Move selection"),
            ctrl: None,
            shift: None,
            alt: None,
        },
    ];

    pub fn new(client: Arc<Client>, parent: crate::env::WidgetId) -> Self {
        Self {
            inner: Arc::new(WidgetInner::new::<Self>(parent)),
            client,
            state: Arc::new(std::sync::RwLock::new(TablePickerState::default())),
        }
    }

    async fn load(self, ctx: crate::env::WidgetCtx) {
        self.set_loading_state(LoadingState::Loading);
        ctx.invalidate();

        let result = self.fetch_tables().await;
        let mut state = self.state.write().unwrap();
        match result {
            Ok(tables) => {
                state.tables = tables;
                state.apply_filter();
                state.loading_state = LoadingState::Loaded;
            }
            Err(err) => {
                state.loading_state = LoadingState::Error(err.clone());
                let is_empty = state.tables.is_empty();
                drop(state);
                if is_empty {
                    ctx.set_popup(Box::new(ErrorPopup::new("Error", err, self.inner.id())));
                } else {
                    ctx.show_toast(Toast {
                        message: err,
                        kind: ToastKind::Error,
                        duration: Duration::from_secs(4),
                    });
                }
                ctx.invalidate();
                return;
            }
        }
        ctx.invalidate();
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

    fn handle_selection(&self, ctx: crate::env::WidgetCtx) -> bool {
        let selected = {
            self.state
                .read()
                .unwrap()
                .selected_table_name()
                .map(str::to_string)
        };
        if let Some(table_name) = selected {
            let widget = Box::new(QueryWidget::new(
                self.client.as_ref().clone(),
                &table_name,
                self.inner.id(),
            ));
            ctx.push_widget(widget);
            return true;
        }
        false
    }
}

impl crate::widgets::Widget for TablePickerWidget {
    fn inner(&self) -> &WidgetInner {
        self.inner.as_ref()
    }

    fn start(&self, ctx: crate::env::WidgetCtx) {
        let this = self.clone();
        tokio::task::spawn_local(this.load(ctx));
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let mut state = self.state.write().unwrap();
        let filter_active = state.filter.is_active();
        let list_area = if filter_active {
            let layout = Layout::vertical([Constraint::Length(3), Constraint::Fill(1)]);
            let [filter_area, list_area] = area.layout(&layout);
            state.filter.render(frame, filter_area, theme);
            list_area
        } else {
            area
        };

        let title = Line::styled("Tables", Style::default().fg(theme.text()));
        let block = Block::bordered()
            .title_top(title)
            .title_bottom(Line::styled(
                pad(format!("{} tables", state.filtered_tables.len()), 2),
                Style::default().fg(theme.text_muted()),
            ))
            .border_style(Style::default().fg(theme.border()))
            .style(Style::default().bg(theme.panel_bg_alt()).fg(theme.text()));

        match &state.loading_state {
            LoadingState::Loading => {
                let text = Paragraph::new("Loading tables...")
                    .style(Style::default().fg(theme.warning()))
                    .block(block);
                frame.render_widget(text, list_area);
            }
            LoadingState::Error(_) => {
                let text = Paragraph::new("Error")
                    .style(Style::default().fg(theme.error()))
                    .block(block);
                frame.render_widget(text, list_area);
            }
            LoadingState::Idle | LoadingState::Loaded => {
                if state.filtered_tables.is_empty() {
                    let empty = Paragraph::new("").block(block);
                    frame.render_widget(empty, list_area);
                } else {
                    let rows: Vec<ListItem> = state
                        .filtered_tables
                        .iter()
                        .map(|name| {
                            ListItem::new(Line::styled(
                                name.clone(),
                                Style::default().fg(theme.text()),
                            ))
                        })
                        .collect();
                    let list = List::new(rows)
                        .block(block)
                        .highlight_symbol(">> ")
                        .highlight_spacing(HighlightSpacing::Always)
                        .highlight_style(
                            Style::default()
                                .bg(theme.selection_bg())
                                .fg(theme.selection_fg()),
                        );

                    StatefulWidget::render(
                        list,
                        list_area,
                        frame.buffer_mut(),
                        &mut state.list_state,
                    );
                }
            }
        }

        let value = state.filter.value.as_str();
        if !value.is_empty() {
            let title = format!("</{value}>");
            let width = title.width() as u16;
            if list_area.width > 2 && width < list_area.width - 2 {
                let start = list_area.x + (list_area.width - width) / 2;
                let y = list_area.y;
                let buf = frame.buffer_mut();
                buf.set_stringn(
                    start,
                    y,
                    title,
                    width as usize,
                    Style::default().fg(theme.accent()),
                );
            }
        }
    }

    fn handle_event(&self, ctx: crate::env::WidgetCtx, event: &Event) -> bool {
        let filter_active = self.state.read().unwrap().filter.is_active();
        let filter_applied = !self.state.read().unwrap().filter.value.is_empty();
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
                KeyCode::Enter if !filter_active => {
                    return self.handle_selection(ctx);
                }
                KeyCode::Esc if !filter_active && filter_applied => {
                    let mut state = self.state.write().unwrap();
                    state.filter.clear();
                    state.apply_filter();
                    return true;
                }
                KeyCode::Esc if !filter_active => {
                    ctx.pop_widget();
                    return true;
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
        let state = self.state.read().unwrap();
        let filter_active = state.filter.is_active();
        let filter_applied = !state.filter.value.is_empty();
        if filter_active {
            Some(Self::HELP_FILTER_EDIT)
        } else if filter_applied {
            Some(Self::HELP_FILTER_APPLIED)
        } else {
            Some(Self::HELP)
        }
    }

    fn suppress_global_help(&self) -> bool {
        self.state.read().unwrap().filter.is_active()
    }
}
