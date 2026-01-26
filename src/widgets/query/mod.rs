use std::{
    borrow::Cow,
    cmp::{max, min},
    collections::{HashMap, HashSet},
    env, fs,
    process::Command,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_sdk_dynamodb::types::{
    AttributeValue, KeySchemaElement, KeyType, TableDescription, TimeToLiveStatus,
};
use crossterm::cursor::MoveTo;
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers,
    KeyboardEnhancementFlags, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use crossterm::terminal::{
    Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    prelude::Widget,
    style::Style,
    text::Line,
    widgets::{Block, HighlightSpacing, Paragraph, Row, StatefulWidget, Table, TableState},
};
use tokio::sync::OnceCell;

use item_keys::ItemKeys;
use keys_widget::KeysWidget;

use crate::{
    env::{Toast, ToastKind},
    help,
    util::pad,
    widgets::{EnvHandle, error::ErrorPopup, theme::Theme},
};
use dynamate::dynamodb::json;
use dynamate::dynamodb::size::estimate_item_size_bytes;
use dynamate::dynamodb::{SecondaryIndex, TableInfo};
use chrono::{DateTime, Utc};
use humansize::{BINARY, format_size};
use unicode_width::UnicodeWidthStr;
use dynamate::{
    dynamodb::{DynamoDbRequest, Kind, Output, ScanBuilder, execute_page},
    expr::parse_dynamo_expression,
};

mod input;
mod item_keys;
mod keys_widget;
mod tree;

#[derive(Clone)]
pub struct QueryWidget {
    client: Arc<aws_sdk_dynamodb::Client>,
    table_name: String,
    sync_state: Arc<std::sync::RwLock<QuerySyncState>>,
    table_desc: Arc<OnceCell<Arc<TableDescription>>>,
    ttl_attr: Arc<OnceCell<Option<String>>>,
}

#[derive(Default)]
struct QuerySyncState {
    input: input::Input,
    filter: FilterInput,
    loading_state: LoadingState,
    query_output: Option<Output>,
    items: Vec<Item>,
    filtered_indices: Vec<usize>,
    item_keys: Arc<item_keys::ItemKeys>,
    table_state: TableState,
    last_evaluated_key: Option<HashMap<String, AttributeValue>>,
    last_query: String,
    is_loading_more: bool,
    show_tree: bool,
    reopen_tree: Option<usize>,
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

#[derive(Debug, Clone)]
struct Item(HashMap<String, AttributeValue>, Arc<ItemKeys>);

impl Item {
    fn value(&self, key: &str) -> String {
        self.0
            .get(key)
            .map(|val| {
                if let Ok(v) = val.as_s() {
                    v.clone()
                } else if let Ok(v) = val.as_n() {
                    v.clone()
                } else if let Ok(v) = val.as_bool() {
                    v.to_string()
                } else {
                    format!("{val:?}")
                }
            })
            .unwrap_or_default()
    }

    fn value_size(&self, key: &str) -> usize {
        self.value(key).len()
    }
}

impl QuerySyncState {
    fn filter_applied(&self) -> bool {
        !self.filter.value.trim().is_empty()
    }

    fn apply_filter(&mut self) {
        let needle = self.filter.value.trim().to_lowercase();
        let current_item = self
            .table_state
            .selected()
            .and_then(|idx| self.filtered_indices.get(idx).copied());

        if needle.is_empty() {
            self.filtered_indices = (0..self.items.len()).collect();
        } else {
            self.filtered_indices = self
                .items
                .iter()
                .enumerate()
                .filter(|(_, item)| item_matches_filter(item, &needle))
                .map(|(idx, _)| idx)
                .collect();
        }

        if self.filtered_indices.is_empty() {
            self.table_state.select(None);
            return;
        }

        if let Some(current_item) = current_item {
            if let Some(new_idx) = self
                .filtered_indices
                .iter()
                .position(|idx| *idx == current_item)
            {
                self.table_state.select(Some(new_idx));
                return;
            }
        }

        self.table_state.select(Some(0));
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum LoadingState {
    #[default]
    Idle,
    Loading,
    Loaded,
    Error(String),
}

impl crate::widgets::Widget for QueryWidget {
    fn start(&self, env: EnvHandle) {
        let this: QueryWidget = self.clone();
        tokio::spawn(this.load(env));
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let mut state = self.sync_state.write().unwrap();

        if state.show_tree {
            self.render_tree(frame, area, theme, &mut state);
        } else {
            let filter_active = state.filter.is_active();
            let layout = if filter_active {
                Layout::vertical([Constraint::Length(3), Constraint::Length(3), Constraint::Fill(1)])
            } else {
                Layout::vertical([Constraint::Length(3), Constraint::Fill(1)])
            };
            let (query_area, filter_area, results_area) = if filter_active {
                let [query_area, filter_area, results_area] = area.layout(&layout);
                (query_area, Some(filter_area), results_area)
            } else {
                let [query_area, results_area] = area.layout(&layout);
                (query_area, None, results_area)
            };
            state.input.render(frame, query_area, theme);
            if let Some(filter_area) = filter_area {
                state.filter.render(frame, filter_area, theme);
            }
            self.render_table(frame, results_area, theme, &mut state);
        }
    }

    fn handle_event(&self, env: EnvHandle, event: &Event) -> bool {
        let input_is_active = self.sync_state.read().unwrap().input.is_active();
        let filter_active = self.sync_state.read().unwrap().filter.is_active();
        if input_is_active && self.sync_state.write().unwrap().input.handle_event(event) {
            return true;
        }
        if filter_active {
            let mut state = self.sync_state.write().unwrap();
            if state.filter.handle_event(event) {
                state.apply_filter();
                return true;
            }
        }
        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Tab | KeyCode::BackTab => {
                    self.sync_state.write().unwrap().input.toggle_active()
                }
                KeyCode::Esc if input_is_active => {
                    self.sync_state.write().unwrap().input.toggle_active()
                }
                KeyCode::Esc if filter_active => {
                    let mut state = self.sync_state.write().unwrap();
                    state.filter.clear();
                    state.filter.set_active(false);
                    state.apply_filter();
                }
                KeyCode::Esc => {
                    let mut state = self.sync_state.write().unwrap();
                    if state.show_tree {
                        state.show_tree = false;
                    } else if state.filter_applied() {
                        state.filter.clear();
                        state.apply_filter();
                    } else {
                        drop(state);
                        env.pop_widget();
                    }
                }
                KeyCode::Enter if input_is_active => {
                    let query = {
                        let mut state = self.sync_state.write().unwrap();
                        let value = state.input.value().to_string();
                        state.input.toggle_active();
                        value
                    };
                    self.start_query(Some(&query), env.clone());
                }
                KeyCode::Enter => {
                    let mut state = self.sync_state.write().unwrap();
                    if !state.show_tree {
                        state.show_tree = true;
                    }
                }
                KeyCode::Char('/') if !input_is_active && !filter_active => {
                    let mut state = self.sync_state.write().unwrap();
                    if !state.show_tree {
                        state.filter.set_active(true);
                    }
                }
                KeyCode::Char('j') | KeyCode::Down => self.scroll_down(env.clone()),
                KeyCode::Char('k') | KeyCode::Up => self.scroll_up(),
                KeyCode::Char('f') => {
                    let state = self.sync_state.read().unwrap();
                    let keys = state
                        .item_keys
                        .sorted()
                        .iter()
                        .map(|k| keys_widget::Key {
                            name: k.clone(),
                            hidden: state.item_keys.is_hidden(k),
                        })
                        .collect::<Vec<_>>();
                    let item_keys = state.item_keys.clone();
                    let popup = Arc::new(KeysWidget::new(&keys, move |ev| match ev {
                        keys_widget::Event::KeyHidden(name) => {
                            item_keys.hide(&name);
                        }
                        keys_widget::Event::KeyUnhidden(name) => {
                            item_keys.unhide(&name);
                        }
                    }));
                    env.set_popup(popup);
                }
                KeyCode::Char('t') => {
                    let mut state = self.sync_state.write().unwrap();
                    state.show_tree = !state.show_tree;
                }
                KeyCode::Char('e')
                    if !input_is_active
                        && key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) =>
                {
                    self.edit_selected(EditorFormat::DynamoDb, env.clone());
                }
                KeyCode::Char('e') => {
                    self.edit_selected(EditorFormat::Plain, env.clone());
                }
                KeyCode::Char('E') => {
                    self.edit_selected(EditorFormat::DynamoDb, env.clone());
                }
                KeyCode::Char('n')
                    if !input_is_active
                        && key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) =>
                {
                    self.create_item(EditorFormat::DynamoDb, env.clone());
                }
                KeyCode::Char('n') => {
                    self.create_item(EditorFormat::Plain, env.clone());
                }
                KeyCode::Char('N') => {
                    self.create_item(EditorFormat::DynamoDb, env.clone());
                }
                _ => {
                    return false; // not handled
                }
            }
            return true;
        }

        if let Some(mouse) = event.as_mouse_event() {
            match mouse.kind {
                crossterm::event::MouseEventKind::ScrollUp => self.scroll_up(),
                crossterm::event::MouseEventKind::ScrollDown => self.scroll_down(env.clone()),
                _ => return false, // not handled
            }
        }

        false
    }

    fn help(&self) -> Option<&[help::Entry<'_>]> {
        let show_tree = self.sync_state.read().unwrap().show_tree;
        if show_tree {
            return Some(Self::HELP_TREE);
        }
        let state = self.sync_state.read().unwrap();
        if state.filter.is_active() {
            Some(Self::HELP_FILTER_EDIT)
        } else if state.filter_applied() {
            Some(Self::HELP_FILTER_APPLIED)
        } else {
            Some(Self::HELP_TABLE)
        }
    }

    fn suppress_global_help(&self) -> bool {
        self.sync_state.read().unwrap().filter.is_active()
    }

}

impl QueryWidget {
    const HELP_TABLE: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("/"),
            short: Cow::Borrowed("filter"),
            long: Cow::Borrowed("Filter items"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("f"),
            short: Cow::Borrowed("fields"),
            long: Cow::Borrowed("Enable/disable fields"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("⏎"),
            short: Cow::Borrowed("view"),
            long: Cow::Borrowed("View selected item"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("e"),
            short: Cow::Borrowed("edit"),
            long: Cow::Borrowed("Edit item (JSON)"),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^e")),
                short: Some(Cow::Borrowed("edit (Dynamo JSON)")),
                long: Some(Cow::Borrowed("Edit item (Dynamo JSON)")),
            }),
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("n"),
            short: Cow::Borrowed("new"),
            long: Cow::Borrowed("New item"),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^n")),
                short: Some(Cow::Borrowed("new (Dynamo JSON)")),
                long: Some(Cow::Borrowed("New item (Dynamo JSON)")),
            }),
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
            keys: Cow::Borrowed("f"),
            short: Cow::Borrowed("fields"),
            long: Cow::Borrowed("Enable/disable fields"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("⏎"),
            short: Cow::Borrowed("view"),
            long: Cow::Borrowed("View selected item"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("e"),
            short: Cow::Borrowed("edit"),
            long: Cow::Borrowed("Edit item (JSON)"),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^e")),
                short: Some(Cow::Borrowed("edit (Dynamo JSON)")),
                long: Some(Cow::Borrowed("Edit item (Dynamo JSON)")),
            }),
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("n"),
            short: Cow::Borrowed("new"),
            long: Cow::Borrowed("New item"),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^n")),
                short: Some(Cow::Borrowed("new (Dynamo JSON)")),
                long: Some(Cow::Borrowed("New item (Dynamo JSON)")),
            }),
            shift: None,
            alt: None,
        },
    ];
    const HELP_TREE: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("j/k/↑/↓"),
            short: Cow::Borrowed("next/prev"),
            long: Cow::Borrowed("Next/previous item"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("e"),
            short: Cow::Borrowed("edit"),
            long: Cow::Borrowed("Edit item (JSON)"),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^e")),
                short: Some(Cow::Borrowed("edit (Dynamo JSON)")),
                long: Some(Cow::Borrowed("Edit item (Dynamo JSON)")),
            }),
            shift: None,
            alt: None,
        },
    ];
    const PAGE_SIZE: i32 = 100;
    pub fn new(client: Arc<aws_sdk_dynamodb::Client>, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
            sync_state: Arc::new(std::sync::RwLock::new(QuerySyncState::default())),
            table_desc: Arc::new(OnceCell::new()),
            ttl_attr: Arc::new(OnceCell::new()),
        }
    }

    async fn load(self, env: EnvHandle) {
        self.start_query(None, env);
    }

    fn set_loading_state(&self, state: LoadingState) {
        self.sync_state.write().unwrap().loading_state = state;
    }

    fn show_error(&self, env: EnvHandle, message: &str) {
        let is_empty = self.sync_state.read().unwrap().items.is_empty();
        if is_empty {
            env.set_popup(Arc::new(ErrorPopup::new("Error", message)));
        } else {
            env.show_toast(Toast {
                message: message.to_string(),
                kind: ToastKind::Error,
                duration: Duration::from_secs(4),
            });
        }
    }

    async fn table_description(&self) -> Result<Arc<TableDescription>, String> {
        let arc_ref = self
            .table_desc
            .get_or_try_init(|| async {
                let out = self
                    .client
                    .describe_table()
                    .table_name(self.table_name.clone())
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;

                let table = out
                    .table()
                    .ok_or_else(|| "DescribeTable: missing table".to_string())?;
                Ok::<Arc<TableDescription>, String>(Arc::new(table.clone()))
            })
            .await?;

        Ok(arc_ref.clone())
    }

    async fn ttl_attribute_name(&self) -> Option<String> {
        let result = self
            .ttl_attr
            .get_or_try_init(|| async {
                let output = self
                    .client
                    .describe_time_to_live()
                    .table_name(self.table_name.clone())
                    .send()
                    .await;
                let attr = match output {
                    Ok(out) => out.time_to_live_description().and_then(|desc| {
                        let enabled = matches!(
                            desc.time_to_live_status(),
                            Some(TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling)
                        );
                        if enabled {
                            desc.attribute_name().map(|name| name.to_string())
                        } else {
                            None
                        }
                    }),
                    Err(_) => None,
                };
                Ok::<Option<String>, String>(attr)
            })
            .await;

        result.ok().and_then(|name| name.clone())
    }

    fn scroll_down(&self, env: EnvHandle) {
        let should_load_more = {
            let mut state = self.sync_state.write().unwrap();
            state.table_state.scroll_down_by(1);
            if state.show_tree {
                false
            } else {
                self.should_load_more(&state)
            }
        };

        if should_load_more {
            self.load_more(env);
        }
    }

    fn scroll_up(&self) {
        let mut state = self.sync_state.write().unwrap();
        state.table_state.scroll_up_by(1);
    }

    fn should_load_more(&self, state: &QuerySyncState) -> bool {
        if state.is_loading_more || state.last_evaluated_key.is_none() {
            return false;
        }
        let visible_len = state.filtered_indices.len();
        if visible_len == 0 {
            return state.filter_applied();
        }
        let selected = state.table_state.selected().unwrap_or(0);
        selected + 1 >= visible_len
    }

    fn load_more(&self, env: EnvHandle) {
        let (query, start_key) = {
            let mut state = self.sync_state.write().unwrap();
            if state.is_loading_more {
                return;
            }
            let Some(start_key) = state.last_evaluated_key.clone() else {
                return;
            };
            state.is_loading_more = true;
            (state.last_query.clone(), start_key)
        };

        self.start_query_page(query, Some(start_key), true, env);
    }

    async fn create_request(&self, query: &str) -> Result<DynamoDbRequest, String> {
        let query = query.trim();
        if query.is_empty() {
            return Ok(DynamoDbRequest::Scan(ScanBuilder::new()));
        }
        let expr = parse_dynamo_expression(query).map_err(|e| e.to_string())?;
        let table_desc = self.table_description().await.map_err(|e| e.to_string())?;
        Ok(DynamoDbRequest::from_expression_and_table(
            &expr,
            &table_desc,
        ))
    }

    fn start_query(&self, query: Option<&str>, env: EnvHandle) {
        self.start_query_with_reopen(query, env, None);
    }

    fn start_query_with_reopen(
        &self,
        query: Option<&str>,
        env: EnvHandle,
        reopen_tree: Option<usize>,
    ) {
        let query = query.unwrap_or("").to_string();
        {
            let mut state = self.sync_state.write().unwrap();
            state.items.clear();
            state.filtered_indices.clear();
            state.item_keys.clear();
            state.table_state = TableState::default();
            state.query_output = None;
            state.last_evaluated_key = None;
            state.is_loading_more = false;
            state.last_query = query.clone();
            state.loading_state = LoadingState::Loading;
            state.show_tree = false;
            state.reopen_tree = reopen_tree;
        }
        env.invalidate();
        let this: QueryWidget = self.clone();
        tokio::spawn(async move {
            let _ = this.ttl_attribute_name().await;
        });
        self.start_query_page(query, None, false, env);
    }

    fn start_query_page(
        &self,
        query: String,
        start_key: Option<HashMap<String, AttributeValue>>,
        append: bool,
        env: EnvHandle,
    ) {
        let this: QueryWidget = self.clone();
        tokio::spawn(async move {
            match this.create_request(&query).await {
                Ok(request) => {
                    match execute_page(
                        this.client.clone(),
                        &this.table_name,
                        &request,
                        start_key,
                        Some(Self::PAGE_SIZE),
                    )
                    .await
                    {
                        Ok(query_output) => {
                            this.process_query_output(query_output, append).await;
                            if !append {
                                this.set_loading_state(LoadingState::Loaded);
                            }
                        }
                        Err(e) => {
                            let message = e.to_string();
                            this.set_loading_state(LoadingState::Error(message.clone()));
                            this.show_error(env.clone(), &message);
                        }
                    };
                }
                Err(e) => {
                    this.set_loading_state(LoadingState::Error(e.clone()));
                    this.show_error(env.clone(), &e);
                }
            }
            env.invalidate();
        });
    }

    async fn process_query_output(&self, output: Output, append: bool) {
        let mut item_keys = HashSet::new();
        let shared_item_keys = self.sync_state.read().unwrap().item_keys.clone();

        let items = output.items();
        let new_items: Vec<Item> = items
            .iter()
            .map(|item| {
                item_keys.extend(item.keys().cloned());
                Item(item.clone(), shared_item_keys.clone())
            })
            .collect();

        let keys_for_update: Vec<String> = item_keys.into_iter().collect();
        let table_desc = self.table_desc.get().cloned();

        let mut state = self.sync_state.write().unwrap();
        if !append {
            state.items.clear();
        }
        state.items.extend(new_items);
        state.last_evaluated_key = output.last_evaluated_key().cloned();
        state.is_loading_more = false;

        state.query_output = Some(output);
        state.apply_filter();
        if !append {
            if let Some(index) = state.reopen_tree.take() {
                if state.filtered_indices.is_empty() {
                    state.show_tree = false;
                    state.table_state.select(None);
                } else {
                    if let Some(pos) = state
                        .filtered_indices
                        .iter()
                        .position(|idx| *idx == index)
                    {
                        state.table_state.select(Some(pos));
                        state.show_tree = true;
                    } else {
                        state.show_tree = false;
                        state.table_state.select(None);
                    }
                }
            }
        }

        drop(state);

        if let Some(table_desc) = table_desc {
            shared_item_keys.extend(keys_for_update, &table_desc);
        } else {
            shared_item_keys.extend_unordered(keys_for_update.clone());
            let this: QueryWidget = self.clone();
            let shared_item_keys = shared_item_keys.clone();
            tokio::spawn(async move {
                if let Ok(table_desc) = this.table_description().await {
                    shared_item_keys.extend(keys_for_update, &table_desc);
                }
            });
        }
    }

    fn render_table(
        &self,
        frame: &mut Frame,
        area: Rect,
        theme: &Theme,
        state: &mut QuerySyncState,
    ) {
        let keys_view = state.item_keys.sorted();
        let header = Row::new(
            keys_view
                .as_slice()
                .iter()
                .map(|key| Line::from(key.clone())),
        )
        .style(Style::new().bold());

        let items = &state.items;
        let widths: Vec<Constraint> = keys_view
            .as_slice()
            .iter()
            .map(|key| {
                let max_value = items
                    .iter()
                    .map(|item| item.value_size(key))
                    .max()
                    .unwrap_or(0);
                let key_size = key.len() + 2;
                Constraint::Min(max(max_value, key_size) as u16)
            })
            .collect();

        drop(keys_view);

        // maximum rows is the area height, minus 2 for the the top and bottom borders,
        // minus 1 for the header
        let max_rows = (area.height - 2 - 1) as usize;
        let total = state.filtered_indices.len();
        let (first_item, last_item) = if total == 0 {
            (0, 0)
        } else {
            let first_item = state.table_state.offset() + 1;
            let last_item = min(first_item + max_rows, total);
            (first_item, last_item)
        };

        // a block with a right aligned title with the loading state on the right
        let (title, title_bottom, title_style) = match &state.loading_state {
            LoadingState::Idle | LoadingState::Loaded => (
                format!("Results{}", output_info(state.query_output.as_ref())),
                pad(
                    format!("{} results, showing {}-{}", total, first_item, last_item),
                    2,
                ),
                Style::default().fg(theme.text()),
            ),
            LoadingState::Loading => (
                "Loading".to_string(),
                "".to_string(),
                Style::default().fg(theme.warning()),
            ),
            LoadingState::Error(_) => (
                "Error".to_string(),
                "".to_string(),
                Style::default().fg(theme.error()),
            ),
        };

        let border = match &state.loading_state {
            LoadingState::Error(_) => Style::default().fg(theme.error()),
            _ => Style::default().fg(theme.border()),
        };
        let block = Block::bordered()
            .title_top(Line::styled(title, title_style))
            .title_bottom(Line::styled(
                title_bottom,
                Style::default().fg(theme.text_muted()),
            ))
            .border_style(border)
            .style(Style::default().bg(theme.panel_bg_alt()).fg(theme.text()));

        if state.table_state.selected().is_none() && !state.filtered_indices.is_empty() {
            state.table_state.select(Some(0));
        }

        let filtered_items: Vec<&Item> = state
            .filtered_indices
            .iter()
            .filter_map(|idx| state.items.get(*idx))
            .collect();
        let table = Table::new(filtered_items, widths)
            .block(block)
            .header(header)
            .highlight_spacing(HighlightSpacing::Always)
            .highlight_symbol(">>")
            .row_highlight_style(
                Style::default()
                    .bg(theme.selection_bg())
                    .fg(theme.selection_fg()),
            );

        StatefulWidget::render(table, area, frame.buffer_mut(), &mut state.table_state);

        let filter_value = state.filter.value.trim();
        if !filter_value.is_empty() {
            let title = format!("</{filter_value}>");
            let width = title.width() as u16;
            if area.width > 2 && width < area.width - 2 {
                let start = area.x + (area.width - width) / 2;
                let y = area.y;
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

    fn render_tree(
        &self,
        frame: &mut Frame,
        area: Rect,
        theme: &Theme,
        state: &mut QuerySyncState,
    ) {
        let (title, title_bottom, title_style) = match &state.loading_state {
            LoadingState::Idle | LoadingState::Loaded => (
                self.item_view_title(state),
                self.item_view_subtitle(state),
                Style::default().fg(theme.text()),
            ),
            LoadingState::Loading => (
                "Loading".to_string(),
                "".to_string(),
                Style::default().fg(theme.warning()),
            ),
            LoadingState::Error(_) => (
                "Error".to_string(),
                "".to_string(),
                Style::default().fg(theme.error()),
            ),
        };

        let border = match &state.loading_state {
            LoadingState::Error(_) => Style::default().fg(theme.error()),
            _ => Style::default().fg(theme.border()),
        };
        let block = Block::bordered()
            .title_top(Line::styled(title, title_style))
            .title_bottom(Line::styled(
                title_bottom,
                Style::default().fg(theme.text_muted()),
            ))
            .border_style(border)
            .style(Style::default().bg(theme.panel_bg_alt()).fg(theme.text()));

        let selected = state.table_state.selected().unwrap_or(0);
        let content = state
            .filtered_indices
            .get(selected)
            .and_then(|idx| state.items.get(*idx))
            .map(|item| tree::item_to_lines(&item.0))
            .unwrap_or_else(|| vec!["No item selected".to_string()]);

        let text = content.join("\n");
        let paragraph = Paragraph::new(text).block(block);
        frame.render_widget(paragraph, area);
    }

    fn item_view_title(&self, state: &QuerySyncState) -> String {
        let Some(table_desc) = self.table_desc.get() else {
            return " Item ".to_string();
        };
        let (hash_key, range_key) = extract_hash_range(table_desc);

        let selected = state.table_state.selected().unwrap_or(0);
        let Some(item) = state
            .filtered_indices
            .get(selected)
            .and_then(|idx| state.items.get(*idx))
        else {
            return " Item ".to_string();
        };

        let mut parts = Vec::new();
        if let Some(hash_key) = hash_key {
            let value = if item.0.contains_key(&hash_key) {
                item.value(&hash_key)
            } else {
                "<missing>".to_string()
            };
            parts.push(format!("{hash_key}={value}"));
        }
        if let Some(range_key) = range_key {
            let value = if item.0.contains_key(&range_key) {
                item.value(&range_key)
            } else {
                "<missing>".to_string()
            };
            parts.push(format!("{range_key}={value}"));
        }

        if parts.is_empty() {
            " Item ".to_string()
        } else {
            format!(" Item: {} ", parts.join(", "))
        }
    }

    fn item_view_subtitle(&self, state: &QuerySyncState) -> String {
        let selected = state.table_state.selected().unwrap_or(0);
        let Some(item) = state
            .filtered_indices
            .get(selected)
            .and_then(|idx| state.items.get(*idx))
        else {
            return pad("No item selected ", 2);
        };
        let bytes = estimate_item_size_bytes(&item.0);
        let size = format_size(bytes as u64, BINARY);
        let mut parts = vec![format!("~{}", size)];

        if let Some(ttl_attr) = self.ttl_attr.get().and_then(|name| name.as_ref()) {
            if let Some(ttl_value) = item.0.get(ttl_attr) {
                if let Some(formatted) = format_ttl_value(ttl_value) {
                    parts.push(format!("ttl: {formatted}"));
                }
            }
        }

        if let Some(table_desc) = self.table_desc.get() {
            let table_info = TableInfo::from_table_description(table_desc);
            let gsi_count = table_info
                .global_secondary_indexes
                .iter()
                .filter(|index| item_matches_index(item, index))
                .count();
            let lsi_count = table_info
                .local_secondary_indexes
                .iter()
                .filter(|index| item_matches_index(item, index))
                .count();
            if gsi_count > 0 {
                parts.push(format!("GSI: {}", gsi_count));
            }
            if lsi_count > 0 {
                parts.push(format!("LSI: {}", lsi_count));
            }
        }

        pad(format!("{} ", parts.join(" · ")), 2)
    }

    fn edit_selected(&self, format: EditorFormat, env: EnvHandle) {
        let (item, query, reopen_tree) = {
            let state = self.sync_state.read().unwrap();
            let selected = state.table_state.selected();
            let item_index = selected
                .and_then(|index| state.filtered_indices.get(index).copied());
            let item = item_index
                .and_then(|index| state.items.get(index))
                .map(|item| item.0.clone());
            let reopen_tree = if state.show_tree { item_index } else { None };
            (item, state.last_query.clone(), reopen_tree)
        };

        let Some(item) = item else {
            let message = "No item selected".to_string();
            self.set_loading_state(LoadingState::Error(message.clone()));
            self.show_error(env.clone(), &message);
            env.invalidate();
            return;
        };

        let initial = match format {
            EditorFormat::Plain => json::to_json_string(&item),
            EditorFormat::DynamoDb => json::to_dynamodb_json_string(&item),
        };
        let initial = match initial {
            Ok(value) => value,
            Err(err) => {
                let message = err.to_string();
                self.set_loading_state(LoadingState::Error(message.clone()));
                self.show_error(env.clone(), &message);
                env.invalidate();
                return;
            }
        };

        let edited = match self.open_editor(&initial, env.clone()) {
            Ok(value) => value,
            Err(err) => {
                self.set_loading_state(LoadingState::Error(err.clone()));
                self.show_error(env.clone(), &err);
                env.invalidate();
                return;
            }
        };
        env.invalidate();

        let updated = match format {
            EditorFormat::Plain => json::from_json_string(&edited),
            EditorFormat::DynamoDb => json::from_dynamodb_json_string(&edited),
        };
        let updated = match updated {
            Ok(value) => value,
            Err(err) => {
                let message = err.to_string();
                self.set_loading_state(LoadingState::Error(message.clone()));
                self.show_error(env.clone(), &message);
                env.invalidate();
                return;
            }
        };

        self.put_item(updated, query, env, reopen_tree);
    }

    fn create_item(&self, format: EditorFormat, env: EnvHandle) {
        let query = self.sync_state.read().unwrap().last_query.clone();
        let initial = match format {
            EditorFormat::Plain => "{}\n".to_string(),
            EditorFormat::DynamoDb => "{}\n".to_string(),
        };

        let edited = match self.open_editor(&initial, env.clone()) {
            Ok(value) => value,
            Err(err) => {
                self.set_loading_state(LoadingState::Error(err.clone()));
                self.show_error(env.clone(), &err);
                env.invalidate();
                return;
            }
        };
        env.invalidate();

        let updated = match format {
            EditorFormat::Plain => json::from_json_string(&edited),
            EditorFormat::DynamoDb => json::from_dynamodb_json_string(&edited),
        };
        let updated = match updated {
            Ok(value) => value,
            Err(err) => {
                let message = err.to_string();
                self.set_loading_state(LoadingState::Error(message.clone()));
                self.show_error(env.clone(), &message);
                env.invalidate();
                return;
            }
        };

        self.put_item(updated, query, env, None);
    }

    fn open_editor(&self, initial: &str, env: EnvHandle) -> Result<String, String> {
        let editor = env::var("EDITOR").map_err(|_| "EDITOR is not set".to_string())?;
        let temp_path = self.temp_path();
        fs::write(&temp_path, initial).map_err(|err| err.to_string())?;

        let keyboard_support = crossterm::terminal::supports_keyboard_enhancement().unwrap_or(false);
        disable_raw_mode().map_err(|err| err.to_string())?;
        if keyboard_support {
            let _ = crossterm::execute!(std::io::stdout(), PopKeyboardEnhancementFlags);
        }
        crossterm::execute!(std::io::stdout(), LeaveAlternateScreen, DisableMouseCapture)
            .map_err(|err| err.to_string())?;

        let command = format!("{editor} \"{}\"", temp_path.display());
        let status = Command::new("sh")
            .arg("-c")
            .arg(command)
            .status()
            .map_err(|err| err.to_string())?;

        crossterm::execute!(
            std::io::stdout(),
            EnterAlternateScreen,
            EnableMouseCapture,
            Clear(ClearType::All),
            MoveTo(0, 0)
        )
        .map_err(|err| err.to_string())?;
        if keyboard_support {
            let flags = KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
                | KeyboardEnhancementFlags::REPORT_ALL_KEYS_AS_ESCAPE_CODES
                | KeyboardEnhancementFlags::REPORT_EVENT_TYPES;
            let _ = crossterm::execute!(std::io::stdout(), PushKeyboardEnhancementFlags(flags));
        }
        enable_raw_mode().map_err(|err| err.to_string())?;
        env.force_redraw();

        if !status.success() {
            return Err("Editor exited with a non-zero status".to_string());
        }

        let contents = fs::read_to_string(&temp_path).map_err(|err| err.to_string())?;
        let _ = fs::remove_file(&temp_path);
        Ok(contents)
    }

    fn temp_path(&self) -> std::path::PathBuf {
        let mut path = env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        path.push(format!("dynamate-edit-{nanos}.json"));
        path
    }

    fn put_item(
        &self,
        item: HashMap<String, AttributeValue>,
        query: String,
        env: EnvHandle,
        reopen_tree: Option<usize>,
    ) {
        let this: QueryWidget = self.clone();
        tokio::spawn(async move {
            this.set_loading_state(LoadingState::Loading);
            env.invalidate();
            let result = this
                .client
                .put_item()
                .table_name(&this.table_name)
                .set_item(Some(item))
                .send()
                .await;
            match result {
                Ok(_) => {
                    this.start_query_with_reopen(Some(&query), env.clone(), reopen_tree);
                }
                Err(err) => {
                    let message = err.to_string();
                    this.set_loading_state(LoadingState::Error(message.clone()));
                    this.show_error(env.clone(), &message);
                    env.invalidate();
                }
            }
        });
    }
}

#[derive(Debug, Clone, Copy)]
enum EditorFormat {
    Plain,
    DynamoDb,
}

impl<'a> From<&Item> for Row<'a> {
    fn from(item: &Item) -> Self {
        let mut parts = Vec::new();
        let view = item.1.sorted();
        for key in view.as_slice() {
            parts.push(item.value(key));
        }
        Row::new(parts)
    }
}

fn extract_hash_range(table: &TableDescription) -> (Option<String>, Option<String>) {
    let mut hash = None;
    let mut range = None;
    for KeySchemaElement {
        attribute_name,
        key_type,
        ..
    } in table.key_schema()
    {
        match key_type {
            KeyType::Hash => hash = Some(attribute_name.clone()),
            KeyType::Range => range = Some(attribute_name.clone()),
            _ => {}
        }
    }
    (hash, range)
}

fn item_matches_index(item: &Item, index: &SecondaryIndex) -> bool {
    if !item.0.contains_key(&index.hash_key) {
        return false;
    }
    match &index.range_key {
        Some(range_key) => item.0.contains_key(range_key),
        None => true,
    }
}

fn item_matches_filter(item: &Item, needle: &str) -> bool {
    for (key, value) in &item.0 {
        if key.to_lowercase().contains(needle) {
            return true;
        }
        let value = match value {
            AttributeValue::S(v) => v.clone(),
            AttributeValue::N(v) => v.clone(),
            AttributeValue::Bool(v) => v.to_string(),
            _ => format!("{value:?}"),
        };
        if value.to_lowercase().contains(needle) {
            return true;
        }
    }
    false
}

fn format_ttl_value(value: &AttributeValue) -> Option<String> {
    let text = match value {
        AttributeValue::N(num) => num,
        AttributeValue::S(text) => text,
        _ => return None,
    };
    let ts: i64 = text.parse().ok()?;
    if ts <= 0 {
        return None;
    }
    let time = UNIX_EPOCH + Duration::from_secs(ts as u64);
    let dt: DateTime<Utc> = time.into();
    Some(dt.format("%Y-%m-%d %H:%M:%SZ").to_string())
}

fn output_info(output: Option<&Output>) -> String {
    match output.map(|o| o.kind()) {
        Some(Kind::Scan) => " (Scan)".to_string(),
        Some(Kind::Query) => " (Query)".to_string(),
        Some(Kind::QueryGSI(index_name)) => {
            format!(" (Query GSI: {})", index_name)
        }
        Some(Kind::QueryLSI(index_name)) => {
            format!(" (Query LSI: {})", index_name)
        }
        None => "".to_string(),
    }
}
