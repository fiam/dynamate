use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    cmp::{max, min},
    collections::{HashMap, HashSet},
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_sdk_dynamodb::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::RequestId;
use aws_sdk_dynamodb::types::{
    AttributeValue, KeySchemaElement, KeyType, TableDescription, TimeToLiveStatus,
};
use crossterm::cursor::MoveTo;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers};
use crossterm::terminal::{
    Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    prelude::Widget,
    style::Style,
    text::{Line, Span},
    widgets::{Block, HighlightSpacing, Paragraph, Row, StatefulWidget, Table, TableState},
};

use super::{
    export_popup::{ExportMode, ExportPopup},
    index_picker, input, item_keys, keys_widget, tree,
};
use keys_widget::KeysWidget;

use crate::{
    env::{Toast, ToastAction, ToastKind},
    help,
    util::{abbreviate_home, env_flag, pad},
    widgets::{
        WidgetInner,
        confirm::{ConfirmAction, ConfirmPopup},
        error::ErrorPopup,
        theme::Theme,
    },
};
use chrono::{DateTime, Utc};
use dynamate::dynamodb::json;
use dynamate::dynamodb::size::estimate_item_size_bytes;
use dynamate::dynamodb::{SecondaryIndex, TableInfo, send_dynamo_request};
use dynamate::{
    dynamodb::{
        DynamoDbRequest, KeyCondition, KeyConditionType, Kind, Output, QueryBuilder, QueryType,
        ScanBuilder, execute_page,
    },
    expr::{
        Comparator, DynamoExpression, Operand, parse_dynamo_expression, parse_single_value_token,
    },
};
use humansize::{BINARY, format_size};
use unicode_width::UnicodeWidthStr;

pub struct QueryWidget {
    inner: WidgetInner,
    client: aws_sdk_dynamodb::Client,
    table_name: String,
    initial_query: Option<ActiveQuery>,
    state: RefCell<QueryState>,
    table_meta: RefCell<Option<TableMeta>>,
    meta_started: Cell<bool>,
    request_seq: Cell<u64>,
    export_seq: Cell<u64>,
    page_size: i32,
}

#[derive(Default)]
struct QueryState {
    input: input::Input,
    filter: FilterInput,
    loading_state: LoadingState,
    query_output: Option<Output>,
    items: Vec<Item>,
    filtered_indices: Vec<usize>,
    item_keys: item_keys::ItemKeys,
    table_state: TableState,
    last_evaluated_key: Option<HashMap<String, AttributeValue>>,
    last_query: String,
    active_query: ActiveQuery,
    is_loading_more: bool,
    show_tree: bool,
    reopen_tree: Option<usize>,
    scanned_total: i64,
    matched_total: i64,
    last_render_capacity: usize,
    is_prefetching: bool,
    export_id: Option<u64>,
    export_cancel: Option<Arc<AtomicBool>>,
}

struct QueryPageEvent {
    request_id: u64,
    append: bool,
    start_key_present: bool,
    result: Result<Output, String>,
}

#[derive(Clone)]
struct TableMeta {
    table_desc: TableDescription,
    ttl_attr: Option<String>,
}

struct TableMetaEvent {
    meta: TableMeta,
}

struct PutItemEvent {
    active_query: ActiveQuery,
    reopen_tree: Option<usize>,
    action: PutAction,
    result: Result<(), String>,
}

struct DeleteItemRequest {
    key: HashMap<String, AttributeValue>,
}

struct DeleteItemEvent {
    key: HashMap<String, AttributeValue>,
    result: Result<(), String>,
}

struct IndexQueryEvent {
    target: index_picker::IndexTarget,
}

struct KeyVisibilityEvent {
    name: String,
    hidden: bool,
}

struct ExportRequest {
    mode: ExportMode,
    path: PathBuf,
    fetch_all: bool,
    overwrite_confirmed: bool,
}

struct ExportEvent {
    result: Result<ExportOutcome, String>,
}

struct ExportProgressEvent {
    export_id: u64,
    count: usize,
}

struct ExportOutcome {
    mode: ExportMode,
    path: PathBuf,
    count: usize,
}

#[derive(Debug, Clone, Copy)]
enum PutAction {
    Create,
    Update,
}

impl PutAction {
    fn success_message(&self) -> &'static str {
        match self {
            PutAction::Create => "Item created",
            PutAction::Update => "Item updated",
        }
    }

    fn error_prefix(&self) -> &'static str {
        match self {
            PutAction::Create => "Failed to create item",
            PutAction::Update => "Failed to update item",
        }
    }
}

#[derive(Clone, Debug)]
enum ActiveQuery {
    Text(String),
    Index(index_picker::IndexTarget),
}

impl Default for ActiveQuery {
    fn default() -> Self {
        ActiveQuery::Text(String::new())
    }
}

impl ActiveQuery {
    fn input_value(&self) -> Option<String> {
        match self {
            ActiveQuery::Text(query) => Some(query.clone()),
            ActiveQuery::Index(target) => QueryWidget::format_index_query(target),
        }
    }
}

struct DeleteTarget {
    key: HashMap<String, AttributeValue>,
    summary: String,
}

fn format_sdk_error<E>(err: &SdkError<E>) -> String
where
    E: ProvideErrorMetadata + RequestId + std::error::Error + 'static,
{
    if let Some(service_err) = err.as_service_error() {
        let code = service_err.code().unwrap_or("ServiceError");
        let message = service_err.message().unwrap_or("").trim();
        let mut summary = if message.is_empty() {
            code.to_string()
        } else {
            format!("{code}: {message}")
        };
        if let Some(request_id) = service_err.request_id() {
            summary.push_str(&format!(" (request id: {request_id})"));
        }
        return summary;
    }
    DisplayErrorContext(err).to_string()
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
struct Item(HashMap<String, AttributeValue>);

impl Item {
    const MAX_CELL_LEN: usize = 120;

    fn value(&self, key: &str) -> String {
        let value = self
            .0
            .get(key)
            .map(|val| {
                if let Ok(v) = val.as_s() {
                    v.clone()
                } else if let Ok(v) = val.as_n() {
                    v.clone()
                } else if let Ok(v) = val.as_bool() {
                    v.to_string()
                } else if val.as_null().is_ok() {
                    "null".to_string()
                } else if let Ok(v) = val.as_b() {
                    format!("<binary:{}>", v.as_ref().len())
                } else if let Ok(v) = val.as_ss() {
                    format!("<ss:{}>", v.len())
                } else if let Ok(v) = val.as_ns() {
                    format!("<ns:{}>", v.len())
                } else if let Ok(v) = val.as_bs() {
                    format!("<bs:{}>", v.len())
                } else if let Ok(v) = val.as_l() {
                    format!("<list:{}>", v.len())
                } else if let Ok(v) = val.as_m() {
                    format!("<map:{}>", v.len())
                } else {
                    "<unknown>".to_string()
                }
            })
            .unwrap_or_default();
        truncate_cell(value)
    }

    fn value_size(&self, key: &str) -> usize {
        let Some(val) = self.0.get(key) else {
            return 0;
        };
        let len = if let Ok(v) = val.as_s() {
            v.len()
        } else if let Ok(v) = val.as_n() {
            v.len()
        } else if let Ok(v) = val.as_bool() {
            if *v { 4 } else { 5 }
        } else if val.as_null().is_ok() {
            4
        } else if let Ok(v) = val.as_b() {
            tag_len("binary", v.as_ref().len())
        } else if let Ok(v) = val.as_ss() {
            tag_len("ss", v.len())
        } else if let Ok(v) = val.as_ns() {
            tag_len("ns", v.len())
        } else if let Ok(v) = val.as_bs() {
            tag_len("bs", v.len())
        } else if let Ok(v) = val.as_l() {
            tag_len("list", v.len())
        } else if let Ok(v) = val.as_m() {
            tag_len("map", v.len())
        } else {
            "<unknown>".len()
        };
        len.min(Self::MAX_CELL_LEN)
    }
}

fn truncate_cell(mut value: String) -> String {
    if value.len() > Item::MAX_CELL_LEN {
        let keep = Item::MAX_CELL_LEN.saturating_sub(3);
        value.truncate(keep);
        value.push_str("...");
    }
    value
}

fn tag_len(tag: &str, size: usize) -> usize {
    // "<" + tag + ":" + digits + ">"
    2 + tag.len() + digits(size)
}

fn digits(mut value: usize) -> usize {
    let mut count = 1;
    while value >= 10 {
        value /= 10;
        count += 1;
    }
    count
}

impl QueryState {
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
                .filter(|(_, item)| item_matches_filter(&item.0, &needle))
                .map(|(idx, _)| idx)
                .collect();
        }

        if self.filtered_indices.is_empty() {
            self.table_state.select(None);
            return;
        }

        if let Some(current_item) = current_item
            && let Some(new_idx) = self
                .filtered_indices
                .iter()
                .position(|idx| *idx == current_item)
        {
            self.table_state.select(Some(new_idx));
            self.clamp_table_offset();
            return;
        }

        self.table_state.select(Some(0));
        self.clamp_table_offset();
    }

    fn clamp_table_offset(&mut self) {
        let total = self.filtered_indices.len();
        let max_rows = self.last_render_capacity.max(1);
        if total == 0 {
            self.table_state.select(None);
            *self.table_state.offset_mut() = 0;
            return;
        }
        let selected = match self.table_state.selected() {
            Some(selected) if selected < total => selected,
            Some(_) | None => {
                let last = total.saturating_sub(1);
                self.table_state.select(Some(last));
                last
            }
        };
        if total <= max_rows {
            *self.table_state.offset_mut() = 0;
            return;
        }
        let offset = self.table_state.offset();
        if selected < offset {
            *self.table_state.offset_mut() = selected;
            return;
        }
        let end = offset.saturating_add(max_rows);
        if selected >= end {
            let new_offset = selected + 1 - max_rows;
            *self.table_state.offset_mut() = new_offset.min(total.saturating_sub(1));
        }
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
    fn inner(&self) -> &WidgetInner {
        &self.inner
    }

    fn navigation_title(&self) -> Option<String> {
        let state = self.state.borrow();
        if state.show_tree {
            return Some("item".to_string());
        }
        Some(self.table_view_title(&state))
    }

    fn is_loading(&self) -> bool {
        let state = self.state.borrow();
        matches!(state.loading_state, LoadingState::Loading) || state.is_prefetching
    }

    fn esc_cancels_export(&self) -> bool {
        let state = self.state.borrow();
        state.is_prefetching
            && !state.show_tree
            && !state.input.is_active()
            && !state.filter.is_active()
    }

    fn start(&self, ctx: crate::env::WidgetCtx) {
        if let Some(initial_query) = self.initial_query.clone() {
            self.restart_query(initial_query, ctx, None);
        } else {
            self.start_query(None, ctx);
        }
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        self.render_with_nav(frame, area, theme, &crate::widgets::NavContext::default());
    }

    fn render_with_nav(
        &self,
        frame: &mut Frame,
        area: Rect,
        theme: &Theme,
        nav: &crate::widgets::NavContext,
    ) {
        let mut state = self.state.borrow_mut();
        let back_title = if state.show_tree {
            Some(self.table_view_title(&state))
        } else {
            nav.back_title.clone()
        };

        if state.show_tree {
            self.render_tree(frame, area, theme, &mut state, back_title.as_deref());
        } else {
            let query_active = state.input.is_active();
            let filter_active = state.filter.is_active();
            let mut constraints = Vec::new();
            if query_active {
                constraints.push(Constraint::Length(3));
            }
            if filter_active {
                constraints.push(Constraint::Length(3));
            }
            constraints.push(Constraint::Fill(1));
            let areas = Layout::vertical(constraints).split(area);

            let mut idx = 0;
            if query_active {
                let query_area = areas[idx];
                state.input.render(frame, query_area, theme);
                idx += 1;
            }
            if filter_active {
                let filter_area = areas[idx];
                state.filter.render(frame, filter_area, theme);
                idx += 1;
            }
            let results_area = areas[idx];
            self.render_table(
                frame,
                results_area,
                theme,
                &mut state,
                back_title.as_deref(),
            );
        }
    }

    fn handle_event(&self, ctx: crate::env::WidgetCtx, event: &Event) -> bool {
        if event.as_key_press_event().is_some() {
            let mut state = self.state.borrow_mut();
            if matches!(state.loading_state, LoadingState::Error(_)) {
                state.loading_state = if state.items.is_empty() {
                    LoadingState::Idle
                } else {
                    LoadingState::Loaded
                };
            }
        }
        let input_is_active = self.state.borrow().input.is_active();
        let filter_active = self.state.borrow().filter.is_active();
        if input_is_active && self.state.borrow_mut().input.handle_event(event) {
            return true;
        }
        if filter_active {
            let mut state = self.state.borrow_mut();
            if state.filter.handle_event(event) {
                state.apply_filter();
                return true;
            }
        }
        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Tab | KeyCode::BackTab => self.state.borrow_mut().input.toggle_active(),
                KeyCode::Esc if input_is_active => {
                    let mut state = self.state.borrow_mut();
                    state.input.toggle_active();
                }
                KeyCode::Esc if filter_active => {
                    let mut state = self.state.borrow_mut();
                    state.filter.clear();
                    state.filter.set_active(false);
                    state.apply_filter();
                }
                KeyCode::Esc => {
                    let mut state = self.state.borrow_mut();
                    if state.show_tree {
                        state.show_tree = false;
                    } else if state.is_prefetching {
                        drop(state);
                        self.request_export_cancel(ctx.clone(), true);
                    } else if matches!(state.loading_state, LoadingState::Loading) {
                        drop(state);
                        self.cancel_active_request();
                    } else if state.filter_applied() {
                        state.filter.clear();
                        state.apply_filter();
                    } else {
                        drop(state);
                        ctx.pop_widget();
                    }
                }
                KeyCode::Enter if input_is_active => {
                    let query = {
                        let mut state = self.state.borrow_mut();
                        let value = state.input.value().to_string();
                        state.input.toggle_active();
                        value
                    };
                    self.start_query(Some(&query), ctx.clone());
                }
                KeyCode::Enter => {
                    let mut state = self.state.borrow_mut();
                    if !state.show_tree {
                        state.show_tree = true;
                    }
                }
                KeyCode::Char('/') if !input_is_active && !filter_active => {
                    let mut state = self.state.borrow_mut();
                    if !state.show_tree {
                        state.filter.set_active(true);
                    }
                }
                KeyCode::Char('q') if !input_is_active && !filter_active => {
                    let mut state = self.state.borrow_mut();
                    if !state.show_tree {
                        state.input.set_active(true);
                    }
                }
                KeyCode::Char('j') | KeyCode::Down => self.scroll_down(ctx.clone()),
                KeyCode::Char('k') | KeyCode::Up => self.scroll_up(),
                KeyCode::PageDown => self.page_down(ctx.clone()),
                KeyCode::PageUp => self.page_up(),
                KeyCode::Char('f') => {
                    let state = self.state.borrow();
                    let keys = state
                        .item_keys
                        .sorted()
                        .iter()
                        .map(|k| keys_widget::Key {
                            name: k.clone(),
                            hidden: state.item_keys.is_hidden(k),
                        })
                        .collect::<Vec<_>>();
                    let ctx_for_keys = ctx.clone();
                    let popup = Box::new(KeysWidget::new(
                        &keys,
                        move |ev| match ev {
                            keys_widget::Event::KeyHidden(name) => {
                                ctx_for_keys.emit_self(KeyVisibilityEvent { name, hidden: true });
                            }
                            keys_widget::Event::KeyUnhidden(name) => {
                                ctx_for_keys.emit_self(KeyVisibilityEvent {
                                    name,
                                    hidden: false,
                                });
                            }
                        },
                        self.inner.id(),
                    ));
                    ctx.set_popup(popup);
                }
                KeyCode::Char('x') if !input_is_active && !filter_active => {
                    if self.state.borrow().show_tree {
                        self.show_export_popup(ExportMode::Item, ctx.clone());
                    } else {
                        self.show_export_popup(ExportMode::Results, ctx.clone());
                    }
                }
                KeyCode::Char('t') => {
                    let mut state = self.state.borrow_mut();
                    state.show_tree = !state.show_tree;
                }
                KeyCode::Char('i') if !input_is_active && !filter_active => {
                    self.show_index_picker(ctx.clone());
                }
                KeyCode::Char('e')
                    if !input_is_active
                        && key
                            .modifiers
                            .contains(crossterm::event::KeyModifiers::CONTROL) =>
                {
                    self.edit_selected(EditorFormat::DynamoDb, ctx.clone());
                }
                KeyCode::Char('e') => {
                    self.edit_selected(EditorFormat::Plain, ctx.clone());
                }
                KeyCode::Char('E') => {
                    self.edit_selected(EditorFormat::DynamoDb, ctx.clone());
                }
                KeyCode::Char('n')
                    if !input_is_active
                        && key
                            .modifiers
                            .contains(crossterm::event::KeyModifiers::CONTROL) =>
                {
                    self.create_item(EditorFormat::DynamoDb, ctx.clone());
                }
                KeyCode::Char('d')
                    if !input_is_active
                        && !filter_active
                        && key
                            .modifiers
                            .contains(crossterm::event::KeyModifiers::CONTROL) =>
                {
                    self.confirm_delete(ctx.clone());
                }
                KeyCode::Char('n') => {
                    self.create_item(EditorFormat::Plain, ctx.clone());
                }
                KeyCode::Char('N') => {
                    self.create_item(EditorFormat::DynamoDb, ctx.clone());
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
                crossterm::event::MouseEventKind::ScrollDown => self.scroll_down(ctx.clone()),
                _ => return false, // not handled
            }
        }

        false
    }

    fn help(&self) -> Option<&[help::Entry<'_>]> {
        let show_tree = self.state.borrow().show_tree;
        if show_tree {
            return Some(Self::HELP_TREE);
        }
        let state = self.state.borrow();
        if state.input.is_active() {
            return Some(Self::HELP_QUERY_EDIT);
        }
        if matches!(state.loading_state, LoadingState::Loading) && !state.filter.is_active() {
            return Some(Self::HELP_LOADING);
        }
        if state.filter.is_active() {
            Some(Self::HELP_FILTER_EDIT)
        } else if state.filter_applied() {
            Some(Self::HELP_FILTER_APPLIED)
        } else {
            Some(Self::HELP_TABLE)
        }
    }

    fn suppress_global_help(&self) -> bool {
        let state = self.state.borrow();
        state.filter.is_active() || state.input.is_active()
    }

    fn on_self_event(&self, ctx: crate::env::WidgetCtx, event: &crate::env::AppEvent) {
        if let Some(page_event) = event.payload::<QueryPageEvent>() {
            if !self.is_request_active(page_event.request_id) {
                return;
            }
            match page_event.result.as_ref() {
                Ok(output) => {
                    let output = output.clone();
                    tracing::trace!(
                        table = %self.table_name,
                        request_id = page_event.request_id,
                        "execute_page_ok"
                    );
                    let (scanned_total, matched_total) = self.record_query_progress(&output);
                    let next_key = output.last_evaluated_key().cloned();
                    tracing::debug!(
                        table = %self.table_name,
                        request_id = page_event.request_id,
                        start_key_present = page_event.start_key_present,
                        next_key_present = next_key.is_some(),
                        items = output.items().len(),
                        scanned = output.scanned_count(),
                        matched = output.count(),
                        "query_page"
                    );
                    self.process_query_output(output, page_event.append);
                    if !page_event.append {
                        self.set_loading_state(LoadingState::Loaded);
                    }
                    {
                        let mut state = self.state.borrow_mut();
                        state.is_prefetching = false;
                    }
                    ctx.invalidate();
                    let _ = (scanned_total, matched_total);
                }
                Err(err) => {
                    tracing::error!(
                        table = %self.table_name,
                        request_id = page_event.request_id,
                        error = %err,
                        "execute_page_error"
                    );
                    self.set_loading_state(LoadingState::Error(err.clone()));
                    self.show_error(ctx.clone(), err);
                    let mut state = self.state.borrow_mut();
                    state.is_loading_more = false;
                    state.is_prefetching = false;
                    ctx.invalidate();
                }
            }
            return;
        }

        if let Some(meta_event) = event.payload::<TableMetaEvent>() {
            let meta = meta_event.meta.clone();
            self.table_meta.borrow_mut().replace(meta.clone());
            let mut state = self.state.borrow_mut();
            state.item_keys.rebuild_with_schema(&meta.table_desc);
            ctx.invalidate();
            return;
        }

        if let Some(key_event) = event.payload::<KeyVisibilityEvent>() {
            let mut state = self.state.borrow_mut();
            if key_event.hidden {
                state.item_keys.hide(&key_event.name);
            } else {
                state.item_keys.unhide(&key_event.name);
            }
            ctx.invalidate();
            return;
        }

        if let Some(export_request) = event.payload::<ExportRequest>() {
            if !export_request.overwrite_confirmed && export_request.path.exists() {
                let filename = export_request
                    .path
                    .file_name()
                    .map(|name| name.to_string_lossy().to_string())
                    .unwrap_or_else(|| export_request.path.display().to_string());
                let message = format!("{filename} already exists");
                let ctx_for_confirm = ctx.clone();
                let confirm_action = ConfirmAction::new(
                    KeyCode::Char('o'),
                    KeyModifiers::CONTROL,
                    "^o",
                    "overwrite",
                    "Overwrite file",
                );
                let mode = export_request.mode;
                let fetch_all = export_request.fetch_all;
                let path = export_request.path.clone();
                let popup = Box::new(ConfirmPopup::new_with_action(
                    "Overwrite?",
                    message,
                    "Overwrite",
                    "cancel",
                    confirm_action,
                    move || {
                        ctx_for_confirm.emit_self(ExportRequest {
                            mode,
                            path: path.clone(),
                            fetch_all,
                            overwrite_confirmed: true,
                        });
                    },
                    self.inner.id(),
                ));
                ctx.set_popup(popup);
                return;
            }
            self.start_export(
                export_request.mode,
                export_request.path.clone(),
                export_request.fetch_all,
                ctx,
            );
            return;
        }

        if let Some(export_event) = event.payload::<ExportEvent>() {
            {
                let mut state = self.state.borrow_mut();
                state.is_prefetching = false;
                state.export_id = None;
                state.export_cancel = None;
            }
            match export_event.result.as_ref() {
                Ok(outcome) => {
                    let display_path = abbreviate_home(&outcome.path);
                    let message = match outcome.mode {
                        ExportMode::Item => format!("Exported to {display_path}"),
                        ExportMode::Results => {
                            format!("Exported {} items to {}", outcome.count, display_path)
                        }
                    };
                    ctx.show_toast(Toast {
                        message,
                        kind: ToastKind::Info,
                        duration: Duration::from_secs(4),
                        action: Some(ToastAction::copy_path(
                            'c',
                            outcome.path.display().to_string(),
                        )),
                    });
                }
                Err(err) => {
                    if err == "Export canceled" {
                        ctx.show_toast(Toast {
                            message: "Export canceled".to_string(),
                            kind: ToastKind::Info,
                            duration: Duration::from_secs(2),
                            action: None,
                        });
                    } else {
                        self.show_error(ctx.clone(), err);
                        ctx.invalidate();
                    }
                }
            }
            return;
        }

        if let Some(progress) = event.payload::<ExportProgressEvent>() {
            let should_update = {
                let state = self.state.borrow();
                state.export_id == Some(progress.export_id)
                    && !state
                        .export_cancel
                        .as_ref()
                        .is_some_and(|flag| flag.load(Ordering::Relaxed))
            };
            if should_update {
                self.show_export_progress_toast(ctx, progress.count);
            }
            return;
        }

        if let Some(put_event) = event.payload::<PutItemEvent>() {
            match put_event.result.as_ref() {
                Ok(()) => {
                    ctx.show_toast(Toast {
                        message: put_event.action.success_message().to_string(),
                        kind: ToastKind::Info,
                        duration: Duration::from_secs(3),
                        action: None,
                    });
                    self.restart_query(
                        put_event.active_query.clone(),
                        ctx.clone(),
                        put_event.reopen_tree,
                    );
                }
                Err(err) => {
                    let message = format!("{}: {err}", put_event.action.error_prefix());
                    self.set_loading_state(LoadingState::Error(message.clone()));
                    self.show_error(ctx.clone(), &message);
                    ctx.invalidate();
                }
            }
        }

        if let Some(delete_event) = event.payload::<DeleteItemRequest>() {
            self.delete_item(delete_event.key.clone(), ctx);
            return;
        }

        if let Some(delete_event) = event.payload::<DeleteItemEvent>() {
            match delete_event.result.as_ref() {
                Ok(()) => {
                    self.set_loading_state(LoadingState::Loaded);
                    self.remove_item_by_key(&delete_event.key);
                    ctx.show_toast(Toast {
                        message: "Item deleted".to_string(),
                        kind: ToastKind::Info,
                        duration: Duration::from_secs(3),
                        action: None,
                    });
                    ctx.invalidate();
                }
                Err(err) => {
                    let message = format!("Failed to delete item: {err}");
                    self.set_loading_state(LoadingState::Error(message.clone()));
                    self.show_error(ctx.clone(), &message);
                    ctx.invalidate();
                }
            }
        }

        if let Some(index_event) = event.payload::<IndexQueryEvent>() {
            let widget = Box::new(QueryWidget::new_with_query(
                self.client.clone(),
                &self.table_name,
                self.inner.id(),
                Some(ActiveQuery::Index(index_event.target.clone())),
            ));
            ctx.push_widget(widget);
        }
    }
}

impl QueryWidget {
    const HELP_TABLE: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("q"),
            short: Cow::Borrowed("query"),
            long: Cow::Borrowed("Query table"),
            ctrl: None,
            shift: None,
            alt: None,
        },
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
            keys: Cow::Borrowed("x"),
            short: Cow::Borrowed("export"),
            long: Cow::Borrowed("Export"),
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
            keys: Cow::Borrowed("i"),
            short: Cow::Borrowed("indexes"),
            long: Cow::Borrowed("Query by index PK"),
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
        help::Entry {
            keys: Cow::Borrowed(""),
            short: Cow::Borrowed(""),
            long: Cow::Borrowed(""),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^d")),
                short: Some(Cow::Borrowed("delete")),
                long: Some(Cow::Borrowed("Delete item")),
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
    const HELP_QUERY_EDIT: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("cancel"),
            long: Cow::Borrowed("Close query input"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("⏎"),
            short: Cow::Borrowed("apply"),
            long: Cow::Borrowed("Run query"),
            ctrl: None,
            shift: None,
            alt: None,
        },
    ];
    const HELP_FILTER_APPLIED: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("q"),
            short: Cow::Borrowed("query"),
            long: Cow::Borrowed("Edit query"),
            ctrl: None,
            shift: None,
            alt: None,
        },
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
            keys: Cow::Borrowed("x"),
            short: Cow::Borrowed("export"),
            long: Cow::Borrowed("Export"),
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
            keys: Cow::Borrowed("i"),
            short: Cow::Borrowed("indexes"),
            long: Cow::Borrowed("Query by index PK"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("x"),
            short: Cow::Borrowed("export"),
            long: Cow::Borrowed("Export"),
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
        help::Entry {
            keys: Cow::Borrowed("^d"),
            short: Cow::Borrowed("delete"),
            long: Cow::Borrowed("Delete item"),
            ctrl: None,
            shift: None,
            alt: None,
        },
    ];
    const HELP_LOADING: &'static [help::Entry<'static>] = &[help::Entry {
        keys: Cow::Borrowed("esc"),
        short: Cow::Borrowed("cancel"),
        long: Cow::Borrowed("Cancel request"),
        ctrl: None,
        shift: None,
        alt: None,
    }];
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
            keys: Cow::Borrowed("x"),
            short: Cow::Borrowed("export"),
            long: Cow::Borrowed("Export"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("i"),
            short: Cow::Borrowed("indexes"),
            long: Cow::Borrowed("Query by index PK"),
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
            keys: Cow::Borrowed(""),
            short: Cow::Borrowed(""),
            long: Cow::Borrowed(""),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^d")),
                short: Some(Cow::Borrowed("delete")),
                long: Some(Cow::Borrowed("Delete item")),
            }),
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("back"),
            long: Cow::Borrowed("Back to results"),
            ctrl: None,
            shift: None,
            alt: None,
        },
    ];
    pub fn new(
        client: aws_sdk_dynamodb::Client,
        table_name: &str,
        parent: crate::env::WidgetId,
    ) -> Self {
        Self::new_with_query(client, table_name, parent, None)
    }

    pub fn new_with_text_query(
        client: aws_sdk_dynamodb::Client,
        table_name: &str,
        query: &str,
        parent: crate::env::WidgetId,
    ) -> Self {
        Self::new_with_query(
            client,
            table_name,
            parent,
            Some(ActiveQuery::Text(query.to_string())),
        )
    }

    fn new_with_query(
        client: aws_sdk_dynamodb::Client,
        table_name: &str,
        parent: crate::env::WidgetId,
        initial_query: Option<ActiveQuery>,
    ) -> Self {
        let page_size = env_u64("DYNAMATE_PAGE_SIZE")
            .and_then(|value| i32::try_from(value).ok())
            .filter(|value| *value > 0)
            .unwrap_or(100);
        Self {
            inner: WidgetInner::new::<Self>(parent),
            client,
            table_name: table_name.to_string(),
            initial_query,
            state: RefCell::new(QueryState::default()),
            table_meta: RefCell::new(None),
            meta_started: Cell::new(false),
            request_seq: Cell::new(0),
            export_seq: Cell::new(0),
            page_size,
        }
    }

    fn set_loading_state(&self, state: LoadingState) {
        self.state.borrow_mut().loading_state = state;
    }

    fn show_error(&self, ctx: crate::env::WidgetCtx, message: &str) {
        let is_empty = self.state.borrow().items.is_empty();
        if is_empty {
            ctx.set_popup(Box::new(ErrorPopup::new("Error", message, self.inner.id())));
        } else {
            ctx.show_toast(Toast {
                message: message.to_string(),
                kind: ToastKind::Error,
                duration: Duration::from_secs(4),
                action: None,
            });
        }
    }

    fn confirm_delete(&self, ctx: crate::env::WidgetCtx) {
        let target = match self.delete_target() {
            Ok(target) => target,
            Err(err) => {
                self.show_error(ctx.clone(), &err);
                return;
            }
        };
        let message = target.summary;
        let key = target.key;
        let ctx_for_delete = ctx.clone();
        let popup = Box::new(ConfirmPopup::new(
            "Delete item",
            message,
            "Delete",
            "cancel",
            move || {
                ctx_for_delete.emit_self(DeleteItemRequest { key: key.clone() });
            },
            self.inner.id(),
        ));
        ctx.set_popup(popup);
    }

    fn show_index_picker(&self, ctx: crate::env::WidgetCtx) {
        let targets = match self.index_targets() {
            Ok(targets) if targets.is_empty() => {
                ctx.show_toast(Toast {
                    message: "No indexes available for this item".to_string(),
                    kind: ToastKind::Info,
                    duration: Duration::from_secs(3),
                    action: None,
                });
                return;
            }
            Ok(targets) => targets,
            Err(err) => {
                self.show_error(ctx.clone(), &err);
                return;
            }
        };
        let ctx_for_select = ctx.clone();
        let popup = Box::new(index_picker::IndexPicker::new(
            targets,
            move |target| {
                ctx_for_select.emit_self(IndexQueryEvent { target });
            },
            self.inner.id(),
        ));
        ctx.set_popup(popup);
    }

    fn show_export_popup(&self, mode: ExportMode, ctx: crate::env::WidgetCtx) {
        if matches!(mode, ExportMode::Item) && self.selected_item().is_err() {
            self.show_error(ctx.clone(), "No item selected");
            return;
        }
        let path = self.export_path(mode);
        let fetch_all = false;
        let ctx_for_confirm = ctx.clone();
        let popup = Box::new(ExportPopup::new(
            mode,
            path,
            fetch_all,
            move |path, fetch_all| {
                ctx_for_confirm.emit_self(ExportRequest {
                    mode,
                    path,
                    fetch_all,
                    overwrite_confirmed: false,
                });
            },
            self.inner.id(),
        ));
        ctx.set_popup(popup);
    }

    fn show_export_progress_toast(&self, ctx: crate::env::WidgetCtx, count: usize) {
        let message = format!(
            "Exporting... {} item{}",
            count,
            if count == 1 { "" } else { "s" }
        );
        ctx.show_toast(Toast {
            message,
            kind: ToastKind::Info,
            duration: Duration::from_secs(3600),
            action: None,
        });
    }

    fn start_export(
        &self,
        mode: ExportMode,
        path: PathBuf,
        fetch_all: bool,
        ctx: crate::env::WidgetCtx,
    ) {
        let busy = {
            let state = self.state.borrow();
            matches!(state.loading_state, LoadingState::Loading) || state.is_loading_more
        };
        if busy {
            self.show_error(
                ctx.clone(),
                "Query is still loading; wait for it to finish before exporting.",
            );
            return;
        }

        match mode {
            ExportMode::Item => {
                let item = match self.selected_item() {
                    Ok(item) => item,
                    Err(err) => {
                        self.show_error(ctx.clone(), &err);
                        return;
                    }
                };
                self.spawn_export_task(mode, path, ctx, move |path| {
                    export_item_to_path(&item, &path)
                });
            }
            ExportMode::Results => {
                let items = {
                    let state = self.state.borrow();
                    state
                        .filtered_indices
                        .iter()
                        .filter_map(|idx| state.items.get(*idx))
                        .map(|item| item.0.clone())
                        .collect::<Vec<_>>()
                };
                if !fetch_all {
                    self.spawn_export_task(mode, path, ctx, move |path| {
                        export_results_to_path(&items, &path)
                    });
                    return;
                }
                let (active_query, start_key, filter, items) = {
                    let state = self.state.borrow();
                    let filter_value = state.filter.value.trim().to_lowercase();
                    let filter = if filter_value.is_empty() {
                        None
                    } else {
                        Some(filter_value)
                    };
                    let items = if let Some(needle) = filter.as_deref() {
                        state
                            .items
                            .iter()
                            .filter(|item| item_matches_filter(&item.0, needle))
                            .map(|item| item.0.clone())
                            .collect::<Vec<_>>()
                    } else {
                        state.items.iter().map(|item| item.0.clone()).collect()
                    };
                    (
                        state.active_query.clone(),
                        state.last_evaluated_key.clone(),
                        filter,
                        items,
                    )
                };
                let Some(start_key) = start_key else {
                    self.spawn_export_task(mode, path, ctx, move |path| {
                        export_results_to_path(&items, &path)
                    });
                    return;
                };
                let initial_count = items.len();
                let export_id = self.next_export_id();
                let cancel = Arc::new(AtomicBool::new(false));
                {
                    let mut state = self.state.borrow_mut();
                    state.is_prefetching = true;
                    state.export_id = Some(export_id);
                    state.export_cancel = Some(cancel.clone());
                }
                self.show_export_progress_toast(ctx.clone(), initial_count);
                let client = self.client.clone();
                let table_name = self.table_name.clone();
                let cached_meta = self.table_meta.borrow().clone();
                let ctx_for_export = ctx.clone();
                let request = ExportResultsRequest {
                    items,
                    filter,
                    start_key,
                    active_query,
                    cached_meta,
                    client,
                    table_name,
                    ctx: ctx_for_export.clone(),
                    path: path.clone(),
                    cancel,
                    export_id,
                };
                tokio::spawn(async move {
                    let result = export_results_full(request)
                        .await
                        .map(|count| ExportOutcome { mode, path, count });
                    ctx_for_export.emit_self(ExportEvent { result });
                });
            }
        }
    }

    fn spawn_export_task<F>(
        &self,
        mode: ExportMode,
        path: PathBuf,
        ctx: crate::env::WidgetCtx,
        task: F,
    ) where
        F: FnOnce(PathBuf) -> Result<usize, String> + Send + 'static,
    {
        let ctx_for_export = ctx.clone();
        tokio::spawn(async move {
            let result = task(path.clone()).map(|count| ExportOutcome { mode, path, count });
            ctx_for_export.emit_self(ExportEvent { result });
        });
    }

    fn selected_item(&self) -> Result<HashMap<String, AttributeValue>, String> {
        let state = self.state.borrow();
        let selected = state.table_state.selected();
        let item_index = selected.and_then(|index| state.filtered_indices.get(index).copied());
        let item = item_index
            .and_then(|index| state.items.get(index))
            .map(|item| item.0.clone());
        item.ok_or_else(|| "No item selected".to_string())
    }

    fn export_path(&self, mode: ExportMode) -> PathBuf {
        let base = export_base_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        match mode {
            ExportMode::Item => {
                let item = self.selected_item().ok();
                if let Some(item) = item
                    && let Some(name) = self.item_export_file_name(&item)
                {
                    return base.join(name);
                }
                base.join(export_file_name(&self.table_name, mode, timestamp))
            }
            ExportMode::Results => {
                let table_desc = self
                    .table_meta
                    .borrow()
                    .as_ref()
                    .map(|meta| meta.table_desc.clone());
                let query = {
                    let state = self.state.borrow();
                    normalized_query(&state.active_query, table_desc.as_ref())
                };
                base.join(export_results_file_name(
                    &self.table_name,
                    query.as_deref(),
                    timestamp,
                ))
            }
        }
    }

    fn item_export_file_name(&self, item: &HashMap<String, AttributeValue>) -> Option<String> {
        let meta = self.table_meta.borrow();
        let meta = meta.as_ref()?;
        let table_info = TableInfo::from_table_description(&meta.table_desc);
        let pk_name = table_info.primary_key.hash_key.as_str();
        let pk_value = item.get(pk_name)?;
        let pk_component = sanitize_filename_component(pk_name, "pk");
        let pk_value_component =
            sanitize_filename_component(&attribute_value_for_filename(pk_value), "value");
        let mut name = format!("{pk_component}_{pk_value_component}");
        if let Some(sk_name) = table_info.primary_key.range_key.as_deref()
            && let Some(sk_value) = item.get(sk_name)
        {
            let sk_component = sanitize_filename_component(sk_name, "sk");
            let sk_value_component =
                sanitize_filename_component(&attribute_value_for_filename(sk_value), "value");
            name.push_str(&format!("-{sk_component}_{sk_value_component}"));
        }
        Some(format!("{name}.json"))
    }

    fn index_targets(&self) -> Result<Vec<index_picker::IndexTarget>, String> {
        let meta = self.table_meta.borrow();
        let Some(meta) = meta.as_ref() else {
            return Err("Table metadata is not available yet".to_string());
        };
        let state = self.state.borrow();
        let selected = state
            .table_state
            .selected()
            .and_then(|idx| state.filtered_indices.get(idx).copied())
            .ok_or_else(|| "No item selected".to_string())?;
        let item = state
            .items
            .get(selected)
            .ok_or_else(|| "No item selected".to_string())?;
        let table_info = TableInfo::from_table_description(&meta.table_desc);
        let mut targets = Vec::new();
        if let Some(value) = item.0.get(&table_info.primary_key.hash_key) {
            targets.push(index_picker::IndexTarget {
                name: "Table".to_string(),
                kind: index_picker::IndexKind::Primary,
                hash_key: table_info.primary_key.hash_key.clone(),
                hash_value: value.clone(),
                hash_display: item.value(&table_info.primary_key.hash_key),
            });
        }
        for gsi in table_info.global_secondary_indexes.iter() {
            if item_matches_index(item, gsi)
                && let Some(value) = item.0.get(&gsi.hash_key)
            {
                targets.push(index_picker::IndexTarget {
                    name: gsi.name.clone(),
                    kind: index_picker::IndexKind::Global,
                    hash_key: gsi.hash_key.clone(),
                    hash_value: value.clone(),
                    hash_display: item.value(&gsi.hash_key),
                });
            }
        }
        for lsi in table_info.local_secondary_indexes.iter() {
            if item_matches_index(item, lsi)
                && let Some(value) = item.0.get(&lsi.hash_key)
            {
                targets.push(index_picker::IndexTarget {
                    name: lsi.name.clone(),
                    kind: index_picker::IndexKind::Local,
                    hash_key: lsi.hash_key.clone(),
                    hash_value: value.clone(),
                    hash_display: item.value(&lsi.hash_key),
                });
            }
        }
        Ok(targets)
    }

    fn delete_target(&self) -> Result<DeleteTarget, String> {
        let meta = self.table_meta.borrow();
        let Some(meta) = meta.as_ref() else {
            return Err("Table metadata is not available yet".to_string());
        };
        let (hash_key, range_key) = extract_hash_range(&meta.table_desc);
        let Some(hash_key) = hash_key else {
            return Err("Table is missing a partition key".to_string());
        };
        let state = self.state.borrow();
        let selected = state
            .table_state
            .selected()
            .and_then(|idx| state.filtered_indices.get(idx).copied())
            .ok_or_else(|| "No item selected".to_string())?;
        let item = state
            .items
            .get(selected)
            .ok_or_else(|| "No item selected".to_string())?;
        let hash_value = item
            .0
            .get(&hash_key)
            .ok_or_else(|| format!("Selected item is missing {hash_key}"))?;
        let mut key = HashMap::new();
        key.insert(hash_key.clone(), hash_value.clone());
        let mut lines = vec![format!("{hash_key}={}", item.value(&hash_key))];
        if let Some(range_key) = range_key {
            let range_value = item
                .0
                .get(&range_key)
                .ok_or_else(|| format!("Selected item is missing {range_key}"))?;
            key.insert(range_key.clone(), range_value.clone());
            lines.push(format!("{range_key}={}", item.value(&range_key)));
        }
        Ok(DeleteTarget {
            key,
            summary: lines.join("\n"),
        })
    }

    fn delete_item(&self, key: HashMap<String, AttributeValue>, ctx: crate::env::WidgetCtx) {
        self.set_loading_state(LoadingState::Loading);
        ctx.invalidate();
        let client = self.client.clone();
        let table_name = self.table_name.clone();
        tokio::spawn(async move {
            let key_len = key.len();
            let span = tracing::trace_span!(
                "DeleteItem",
                table = %table_name,
                key_len = key_len
            );
            let result = send_dynamo_request(
                span,
                || {
                    client
                        .delete_item()
                        .table_name(&table_name)
                        .set_key(Some(key.clone()))
                        .send()
                },
                format_sdk_error,
            )
            .await;
            let event_result = result.map(|_| ()).map_err(|err| format_sdk_error(&err));
            ctx.emit_self(DeleteItemEvent {
                key,
                result: event_result,
            });
        });
    }

    fn remove_item_by_key(&self, key: &HashMap<String, AttributeValue>) {
        let (hash_key, range_key) = {
            let meta = self.table_meta.borrow();
            let Some(meta) = meta.as_ref() else {
                return;
            };
            extract_hash_range(&meta.table_desc)
        };
        let Some(hash_key) = hash_key else {
            return;
        };
        let mut state = self.state.borrow_mut();
        if let Some(index) = state.items.iter().position(|item| {
            let hash_matches = item.0.get(&hash_key) == key.get(&hash_key);
            if let Some(range_key) = range_key.as_ref() {
                hash_matches && item.0.get(range_key) == key.get(range_key)
            } else {
                hash_matches
            }
        }) {
            state.items.remove(index);
            state.apply_filter();
        }
    }

    fn scroll_down(&self, ctx: crate::env::WidgetCtx) {
        let should_load_more = {
            let mut state = self.state.borrow_mut();
            state.table_state.scroll_down_by(1);
            if state.show_tree {
                false
            } else {
                state.clamp_table_offset();
                self.should_load_more(&state)
            }
        };

        if should_load_more {
            self.load_more(ctx);
        }
    }

    fn page_down(&self, ctx: crate::env::WidgetCtx) {
        let should_load_more = {
            let mut state = self.state.borrow_mut();
            let total = state.filtered_indices.len();
            if total == 0 {
                if state.show_tree || state.is_loading_more {
                    false
                } else {
                    self.should_load_more(&state)
                }
            } else {
                let page = state.last_render_capacity.max(1);
                let offset = state.table_state.offset();
                let selected = state
                    .table_state
                    .selected()
                    .unwrap_or(0)
                    .min(total.saturating_sub(1));
                let rel = selected.saturating_sub(offset).min(page.saturating_sub(1));
                let max_offset = total.saturating_sub(page);
                let new_offset = offset.saturating_add(page).min(max_offset);
                let mut new_selected = new_offset.saturating_add(rel);
                if new_selected >= total {
                    new_selected = total.saturating_sub(1);
                }
                *state.table_state.offset_mut() = new_offset;
                state.table_state.select(Some(new_selected));

                if state.show_tree || state.is_loading_more {
                    false
                } else {
                    self.should_load_more(&state)
                        || (state.last_evaluated_key.is_some() && new_offset == max_offset)
                }
            }
        };

        if should_load_more {
            self.load_more(ctx);
        }
    }

    fn scroll_up(&self) {
        let mut state = self.state.borrow_mut();
        state.table_state.scroll_up_by(1);
        state.clamp_table_offset();
    }

    fn page_up(&self) {
        let mut state = self.state.borrow_mut();
        let total = state.filtered_indices.len();
        if total == 0 {
            return;
        }
        let page = state.last_render_capacity.max(1);
        let offset = state.table_state.offset();
        let selected = state
            .table_state
            .selected()
            .unwrap_or(0)
            .min(total.saturating_sub(1));
        let rel = selected.saturating_sub(offset).min(page.saturating_sub(1));
        let new_offset = offset.saturating_sub(page);
        let mut new_selected = new_offset.saturating_add(rel);
        if new_selected >= total {
            new_selected = total.saturating_sub(1);
        }
        *state.table_state.offset_mut() = new_offset;
        state.table_state.select(Some(new_selected));
    }

    fn should_load_more(&self, state: &QueryState) -> bool {
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

    fn load_more(&self, ctx: crate::env::WidgetCtx) {
        let (active_query, start_key) = {
            let mut state = self.state.borrow_mut();
            if state.is_loading_more {
                return;
            }
            let Some(start_key) = state.last_evaluated_key.clone() else {
                return;
            };
            state.is_loading_more = true;
            (state.active_query.clone(), start_key)
        };

        let request_id = self.active_request_id();
        match active_query {
            ActiveQuery::Text(query) => {
                self.start_query_page(query, Some(start_key), true, ctx, request_id);
            }
            ActiveQuery::Index(target) => {
                self.start_index_query_page(target, Some(start_key), true, ctx, request_id);
            }
        }
    }

    fn start_query(&self, query: Option<&str>, ctx: crate::env::WidgetCtx) {
        self.start_query_with_reopen(query, ctx, None);
    }

    fn restart_query(
        &self,
        active_query: ActiveQuery,
        ctx: crate::env::WidgetCtx,
        reopen_tree: Option<usize>,
    ) {
        match active_query {
            ActiveQuery::Text(query) => {
                self.start_query_with_reopen(Some(&query), ctx, reopen_tree);
            }
            ActiveQuery::Index(target) => {
                self.start_index_query(target, ctx, reopen_tree);
            }
        }
    }

    fn start_query_with_reopen(
        &self,
        query: Option<&str>,
        ctx: crate::env::WidgetCtx,
        reopen_tree: Option<usize>,
    ) {
        self.maybe_start_meta_fetch(ctx.clone());
        let query = query.unwrap_or("").to_string();
        let active_query = ActiveQuery::Text(query.clone());
        let request_id = self.bump_request_id();
        tracing::debug!(
            table = %self.table_name,
            request_id,
            query = %query,
            "start_query"
        );
        {
            let mut state = self.state.borrow_mut();
            state.items.clear();
            state.filtered_indices.clear();
            state.item_keys.clear();
            state.table_state = TableState::default();
            state.query_output = None;
            state.last_evaluated_key = None;
            state.is_loading_more = false;
            state.last_query = active_query.input_value().unwrap_or_default();
            state.active_query = active_query.clone();
            if let Some(value) = active_query.input_value() {
                state.input.set_value(value);
            }
            state.loading_state = LoadingState::Loading;
            state.show_tree = false;
            state.reopen_tree = reopen_tree;
            state.scanned_total = 0;
            state.matched_total = 0;
            state.is_prefetching = false;
        }
        ctx.invalidate();
        self.start_query_page(query, None, false, ctx, request_id);
    }

    fn start_query_page(
        &self,
        query: String,
        start_key: Option<HashMap<String, AttributeValue>>,
        append: bool,
        ctx: crate::env::WidgetCtx,
        request_id: u64,
    ) {
        let client = self.client.clone();
        let table_name = self.table_name.clone();
        let page_size = self.page_size;
        let cached_meta = self.table_meta.borrow().clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let request_result = create_request_from_query(
                &query,
                cached_meta,
                client.clone(),
                table_name.clone(),
                ctx.clone(),
            )
            .await;
            let result = match request_result {
                Ok(request) => {
                    let request_start_key = start_key.clone();
                    tracing::trace!(
                        table = %table_name,
                        request_id,
                        append,
                        start_key_present = request_start_key.is_some(),
                        "execute_page_start"
                    );
                    execute_page(
                        &client,
                        &table_name,
                        &request,
                        request_start_key,
                        Some(page_size),
                    )
                    .await
                    .map_err(|e| e.to_string())
                }
                Err(e) => Err(e),
            };
            ctx.emit_self(QueryPageEvent {
                request_id,
                append,
                start_key_present: start_key.is_some(),
                result,
            });
        });
    }

    fn start_index_query(
        &self,
        target: index_picker::IndexTarget,
        ctx: crate::env::WidgetCtx,
        reopen_tree: Option<usize>,
    ) {
        self.maybe_start_meta_fetch(ctx.clone());
        let active_query = ActiveQuery::Index(target.clone());
        let request_id = self.bump_request_id();
        tracing::debug!(
            table = %self.table_name,
            request_id,
            index = %target.name,
            "start_index_query"
        );
        {
            let mut state = self.state.borrow_mut();
            state.items.clear();
            state.filtered_indices.clear();
            state.item_keys.clear();
            state.table_state = TableState::default();
            state.query_output = None;
            state.last_evaluated_key = None;
            state.is_loading_more = false;
            state.last_query = active_query.input_value().unwrap_or_default();
            state.active_query = active_query.clone();
            if let Some(value) = active_query.input_value() {
                state.input.set_value(value);
            }
            state.loading_state = LoadingState::Loading;
            state.show_tree = false;
            state.reopen_tree = reopen_tree;
            state.scanned_total = 0;
            state.matched_total = 0;
            state.is_prefetching = false;
        }
        ctx.invalidate();
        self.start_index_query_page(target, None, false, ctx, request_id);
    }

    fn start_index_query_page(
        &self,
        target: index_picker::IndexTarget,
        start_key: Option<HashMap<String, AttributeValue>>,
        append: bool,
        ctx: crate::env::WidgetCtx,
        request_id: u64,
    ) {
        let client = self.client.clone();
        let table_name = self.table_name.clone();
        let page_size = self.page_size;
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let request = request_for_index_target(&target);
            let request_start_key = start_key.clone();
            tracing::trace!(
                table = %table_name,
                request_id,
                append,
                start_key_present = request_start_key.is_some(),
                "execute_page_start"
            );
            let result = execute_page(
                &client,
                &table_name,
                &request,
                request_start_key,
                Some(page_size),
            )
            .await
            .map_err(|e| e.to_string());
            ctx.emit_self(QueryPageEvent {
                request_id,
                append,
                start_key_present: start_key.is_some(),
                result,
            });
        });
    }

    fn format_query_value(value: &AttributeValue) -> Option<String> {
        match value {
            AttributeValue::S(text) => serde_json::to_string(text).ok(),
            AttributeValue::N(num) => Some(num.clone()),
            AttributeValue::Bool(value) => Some(value.to_string()),
            AttributeValue::Null(_) => Some("null".to_string()),
            _ => None,
        }
    }

    fn format_index_query(target: &index_picker::IndexTarget) -> Option<String> {
        let value = Self::format_query_value(&target.hash_value)?;
        Some(format!("{} = {}", target.hash_key, value))
    }

    fn bump_request_id(&self) -> u64 {
        let next = self.request_seq.get() + 1;
        self.request_seq.set(next);
        next
    }

    fn next_export_id(&self) -> u64 {
        let next = self.export_seq.get().saturating_add(1);
        self.export_seq.set(next);
        next
    }

    fn active_request_id(&self) -> u64 {
        self.request_seq.get()
    }

    fn is_request_active(&self, request_id: u64) -> bool {
        self.active_request_id() == request_id
    }

    fn cancel_active_request(&self) {
        self.bump_request_id();
        let mut state = self.state.borrow_mut();
        state.is_loading_more = false;
        state.is_prefetching = false;
        if matches!(state.loading_state, LoadingState::Loading) {
            state.loading_state = LoadingState::Loaded;
        }
    }

    fn request_export_cancel(&self, ctx: crate::env::WidgetCtx, show_toast: bool) {
        let cancel = {
            let state = self.state.borrow();
            state.export_cancel.clone()
        };
        let Some(cancel) = cancel else {
            return;
        };
        if !cancel.swap(true, Ordering::Relaxed) && show_toast {
            ctx.show_toast(Toast {
                message: "Canceling export...".to_string(),
                kind: ToastKind::Info,
                duration: Duration::from_secs(2),
                action: None,
            });
        }
    }

    fn maybe_start_meta_fetch(&self, ctx: crate::env::WidgetCtx) {
        if self.meta_started.get() {
            return;
        }
        self.meta_started.set(true);
        let client = self.client.clone();
        let table_name = self.table_name.clone();
        tokio::spawn(async move {
            if let Ok(meta) = fetch_table_meta(client, table_name).await {
                ctx.emit_self(TableMetaEvent { meta });
            }
        });
    }

    fn record_query_progress(&self, output: &Output) -> (i64, i64) {
        let mut state = self.state.borrow_mut();
        state.scanned_total += output.scanned_count() as i64;
        state.matched_total += output.count() as i64;
        (state.scanned_total, state.matched_total)
    }

    fn process_query_output(&self, output: Output, append: bool) {
        let mut item_keys = HashSet::new();

        let items = output.items();
        let new_items: Vec<Item> = items
            .iter()
            .map(|item| {
                item_keys.extend(item.keys().cloned());
                Item(item.clone())
            })
            .collect();

        let keys_for_update: Vec<String> = item_keys.into_iter().collect();
        let table_desc = self
            .table_meta
            .borrow()
            .as_ref()
            .map(|meta| meta.table_desc.clone());

        let mut state = self.state.borrow_mut();
        if !append {
            state.items.clear();
        }
        state.items.extend(new_items);
        state.last_evaluated_key = output.last_evaluated_key().cloned();
        state.is_loading_more = false;

        if let Some(table_desc) = table_desc.as_ref() {
            state.item_keys.extend(keys_for_update, table_desc);
        } else {
            state.item_keys.extend_unordered(keys_for_update);
        }
        state.query_output = Some(output);
        state.apply_filter();
        if !append && let Some(index) = state.reopen_tree.take() {
            if state.filtered_indices.is_empty() {
                state.show_tree = false;
                state.table_state.select(None);
            } else if let Some(pos) = state.filtered_indices.iter().position(|idx| *idx == index) {
                state.table_state.select(Some(pos));
                state.show_tree = true;
            } else {
                state.show_tree = false;
                state.table_state.select(None);
            }
        }

        drop(state);
    }

    fn render_table(
        &self,
        frame: &mut Frame,
        area: Rect,
        theme: &Theme,
        state: &mut QueryState,
        back_title: Option<&str>,
    ) {
        // maximum rows is the area height, minus 2 for the the top and bottom borders,
        // minus 1 for the header
        let max_rows = (area.height - 2 - 1) as usize;
        state.last_render_capacity = max_rows;
        state.clamp_table_offset();
        let total = state.filtered_indices.len();
        let (first_item, last_item) = if total == 0 {
            (0, 0)
        } else {
            let first_item = state.table_state.offset() + 1;
            let last_item = min(first_item + max_rows, total);
            (first_item, last_item)
        };

        let keys: Vec<String> = state.item_keys.visible().to_vec();
        let header =
            Row::new(keys.iter().map(|key| Line::from(key.clone()))).style(Style::new().bold());

        let visible_indices = if total == 0 {
            &[][..]
        } else {
            let start = state.table_state.offset();
            let end = start.saturating_add(max_rows).min(total);
            &state.filtered_indices[start..end]
        };

        let widths: Vec<Constraint> = keys
            .iter()
            .map(|key| {
                let max_value = visible_indices
                    .iter()
                    .filter_map(|idx| state.items.get(*idx))
                    .map(|item| item.value_size(key))
                    .max()
                    .unwrap_or(0);
                let key_size = key.len() + 2;
                Constraint::Min(max(max_value, key_size) as u16)
            })
            .collect();

        // a block with a right aligned title with the loading state on the right
        let more_marker = if state.last_evaluated_key.is_some() {
            "more"
        } else {
            "end"
        };
        let approx_total = self
            .table_meta
            .borrow()
            .as_ref()
            .and_then(|meta| meta.table_desc.item_count())
            .map(|count| format!("~{count} items"));
        let mut footer_suffix = String::new();
        if let Some(value) = approx_total.as_ref() {
            footer_suffix.push_str(&format!(" · {value}"));
        }
        let table_desc = self
            .table_meta
            .borrow()
            .as_ref()
            .map(|meta| meta.table_desc.clone());
        if let Some(value) = query_footer_label(
            state.query_output.as_ref(),
            &state.active_query,
            table_desc.as_ref(),
        ) {
            footer_suffix.push_str(&format!(" · {value}"));
        }
        let (title, title_bottom, title_style) = match &state.loading_state {
            LoadingState::Idle | LoadingState::Loaded => (
                format!("Results{}", output_info(state.query_output.as_ref())),
                pad(
                    format!(
                        "{} results, showing {}-{} · {}{}",
                        total,
                        first_item,
                        last_item,
                        more_marker,
                        footer_suffix.clone()
                    ),
                    2,
                ),
                Style::default().fg(theme.text()),
            ),
            LoadingState::Loading => (
                "Loading".to_string(),
                pad(
                    format!(
                        "scanned {} · matched {} · {}{}",
                        state.scanned_total, state.matched_total, more_marker, footer_suffix
                    ),
                    2,
                ),
                Style::default().fg(theme.warning()),
            ),
            LoadingState::Error(_) => (
                "Error".to_string(),
                "".to_string(),
                Style::default().fg(theme.error()),
            ),
        };

        let title_line = self.title_line(title, title_style, theme, back_title);
        let border = match &state.loading_state {
            LoadingState::Error(_) => Style::default().fg(theme.error()),
            _ => Style::default().fg(theme.border()),
        };
        let block = Block::bordered()
            .title_top(title_line)
            .title_bottom(Line::styled(
                title_bottom,
                Style::default().fg(theme.text_muted()),
            ))
            .border_style(border)
            .style(Style::default().bg(theme.panel_bg_alt()).fg(theme.text()));

        if state.table_state.selected().is_none() && !state.filtered_indices.is_empty() {
            state.table_state.select(Some(0));
        }

        let rows: Vec<Row> = visible_indices
            .iter()
            .filter_map(|idx| state.items.get(*idx))
            .map(|item| {
                let values = keys.iter().map(|key| item.value(key));
                Row::new(values)
            })
            .collect();
        let visible_len = rows.len();
        let table = Table::new(rows, widths)
            .block(block)
            .header(header)
            .highlight_spacing(HighlightSpacing::Always)
            .highlight_symbol(">>")
            .row_highlight_style(
                Style::default()
                    .bg(theme.selection_bg())
                    .fg(theme.selection_fg()),
            );

        let selected_global = state.table_state.selected();
        let selected_visible = selected_global
            .and_then(|selected| selected.checked_sub(state.table_state.offset()))
            .filter(|selected| *selected < visible_len);
        let mut render_state = TableState::default();
        render_state.select(selected_visible);
        StatefulWidget::render(table, area, frame.buffer_mut(), &mut render_state);

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
        state: &mut QueryState,
        back_title: Option<&str>,
    ) {
        let more_marker = if state.last_evaluated_key.is_some() {
            "more"
        } else {
            "end"
        };
        let (title, title_bottom, title_style) = match &state.loading_state {
            LoadingState::Idle | LoadingState::Loaded => (
                self.item_view_title(state),
                self.item_view_subtitle(state),
                Style::default().fg(theme.text()),
            ),
            LoadingState::Loading => (
                "Loading".to_string(),
                pad(
                    format!(
                        "scanned {} · matched {} · {}",
                        state.scanned_total, state.matched_total, more_marker
                    ),
                    2,
                ),
                Style::default().fg(theme.warning()),
            ),
            LoadingState::Error(_) => (
                "Error".to_string(),
                "".to_string(),
                Style::default().fg(theme.error()),
            ),
        };

        let title_line = self.title_line(title, title_style, theme, back_title);
        let border = match &state.loading_state {
            LoadingState::Error(_) => Style::default().fg(theme.error()),
            _ => Style::default().fg(theme.border()),
        };
        let block = Block::bordered()
            .title_top(title_line)
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
            .map(|item| tree::item_to_lines(&item.0, theme, Some(state.item_keys.sorted())))
            .unwrap_or_else(|| vec![Line::from("No item selected")]);

        let paragraph = Paragraph::new(content).block(block);
        frame.render_widget(paragraph, area);
    }

    fn item_view_title(&self, state: &QueryState) -> String {
        let meta_ref = self.table_meta.borrow();
        let Some(meta) = meta_ref.as_ref() else {
            return " Item ".to_string();
        };
        let (hash_key, range_key) = extract_hash_range(&meta.table_desc);

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
            format!(" {} ", parts.join(" · "))
        }
    }

    fn title_line(
        &self,
        title: String,
        title_style: Style,
        theme: &Theme,
        back_title: Option<&str>,
    ) -> Line<'static> {
        let Some(back_title) = back_title else {
            return Line::styled(title, title_style);
        };
        Line::from(vec![
            Span::styled(
                format!("← {back_title} "),
                Style::default().fg(theme.text_muted()),
            ),
            Span::styled(title, title_style),
        ])
    }

    fn item_view_subtitle(&self, state: &QueryState) -> String {
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

        if let Some(ttl_attr) = self
            .table_meta
            .borrow()
            .as_ref()
            .and_then(|meta| meta.ttl_attr.as_ref())
            && let Some(ttl_value) = item.0.get(ttl_attr)
            && let Some(formatted) = format_ttl_value(ttl_value)
        {
            parts.push(format!("ttl: {formatted}"));
        }

        let meta_ref = self.table_meta.borrow();
        if let Some(meta) = meta_ref.as_ref() {
            let table_info = TableInfo::from_table_description(&meta.table_desc);
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

    fn table_view_title(&self, state: &QueryState) -> String {
        let query = state
            .active_query
            .input_value()
            .unwrap_or_default()
            .trim()
            .to_string();
        if query.is_empty() {
            self.table_name.clone()
        } else {
            query
        }
    }

    fn edit_selected(&self, format: EditorFormat, ctx: crate::env::WidgetCtx) {
        let (item, active_query, reopen_tree) = {
            let state = self.state.borrow();
            let selected = state.table_state.selected();
            let item_index = selected.and_then(|index| state.filtered_indices.get(index).copied());
            let item = item_index
                .and_then(|index| state.items.get(index))
                .map(|item| item.0.clone());
            let reopen_tree = if state.show_tree { item_index } else { None };
            (item, state.active_query.clone(), reopen_tree)
        };

        let Some(item) = item else {
            let message = "No item selected".to_string();
            self.set_loading_state(LoadingState::Error(message.clone()));
            self.show_error(ctx.clone(), &message);
            ctx.invalidate();
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
                self.show_error(ctx.clone(), &message);
                ctx.invalidate();
                return;
            }
        };

        let edited = match self.open_editor(&initial, ctx.clone()) {
            Ok(value) => value,
            Err(err) => {
                self.set_loading_state(LoadingState::Error(err.clone()));
                self.show_error(ctx.clone(), &err);
                ctx.invalidate();
                return;
            }
        };
        ctx.invalidate();

        let updated = match format {
            EditorFormat::Plain => json::from_json_string(&edited),
            EditorFormat::DynamoDb => json::from_dynamodb_json_string(&edited),
        };
        let updated = match updated {
            Ok(value) => value,
            Err(err) => {
                let message = err.to_string();
                self.set_loading_state(LoadingState::Error(message.clone()));
                self.show_error(ctx.clone(), &message);
                ctx.invalidate();
                return;
            }
        };

        if updated == item {
            ctx.show_toast(Toast {
                message: "Item unchanged".to_string(),
                kind: ToastKind::Info,
                duration: Duration::from_secs(3),
                action: None,
            });
            return;
        }

        self.put_item(updated, active_query, PutAction::Update, ctx, reopen_tree);
    }

    fn create_item(&self, format: EditorFormat, ctx: crate::env::WidgetCtx) {
        let active_query = self.state.borrow().active_query.clone();
        let initial = match format {
            EditorFormat::Plain => "{}\n".to_string(),
            EditorFormat::DynamoDb => "{}\n".to_string(),
        };

        let edited = match self.open_editor(&initial, ctx.clone()) {
            Ok(value) => value,
            Err(err) => {
                self.set_loading_state(LoadingState::Error(err.clone()));
                self.show_error(ctx.clone(), &err);
                ctx.invalidate();
                return;
            }
        };
        ctx.invalidate();

        let updated = match format {
            EditorFormat::Plain => json::from_json_string(&edited),
            EditorFormat::DynamoDb => json::from_dynamodb_json_string(&edited),
        };
        let updated = match updated {
            Ok(value) => value,
            Err(err) => {
                let message = err.to_string();
                self.set_loading_state(LoadingState::Error(message.clone()));
                self.show_error(ctx.clone(), &message);
                ctx.invalidate();
                return;
            }
        };

        self.put_item(updated, active_query, PutAction::Create, ctx, None);
    }

    fn open_editor(&self, initial: &str, ctx: crate::env::WidgetCtx) -> Result<String, String> {
        let editor = env::var("EDITOR").map_err(|_| "EDITOR is not set".to_string())?;
        let temp_path = self.temp_path();
        fs::write(&temp_path, initial).map_err(|err| err.to_string())?;
        let restore_mouse_capture = env_flag("DYNAMATE_MOUSE_CAPTURE");

        disable_raw_mode().map_err(|err| err.to_string())?;
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
            Clear(ClearType::All),
            MoveTo(0, 0)
        )
        .map_err(|err| err.to_string())?;
        if restore_mouse_capture {
            crossterm::execute!(std::io::stdout(), EnableMouseCapture)
                .map_err(|err| err.to_string())?;
        }
        enable_raw_mode().map_err(|err| err.to_string())?;
        ctx.force_redraw();

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
        active_query: ActiveQuery,
        action: PutAction,
        ctx: crate::env::WidgetCtx,
        reopen_tree: Option<usize>,
    ) {
        self.set_loading_state(LoadingState::Loading);
        ctx.invalidate();
        let client = self.client.clone();
        let table_name = self.table_name.clone();
        tokio::spawn(async move {
            let item_len = item.len();
            let span = tracing::trace_span!(
                "PutItem",
                table = %table_name,
                item_len = item_len
            );
            let result = send_dynamo_request(
                span,
                || {
                    client
                        .put_item()
                        .table_name(&table_name)
                        .set_item(Some(item))
                        .send()
                },
                format_sdk_error,
            )
            .await;
            let event_result = result.map(|_| ()).map_err(|err| format_sdk_error(&err));
            ctx.emit_self(PutItemEvent {
                active_query,
                reopen_tree,
                action,
                result: event_result,
            });
        });
    }
}

fn request_for_index_target(target: &index_picker::IndexTarget) -> DynamoDbRequest {
    let hash_condition = KeyCondition {
        attribute_name: target.hash_key.clone(),
        condition: KeyConditionType::Equal(target.hash_value.clone()),
    };
    let query_type = match target.kind {
        index_picker::IndexKind::Primary => QueryType::TableQuery {
            hash_key_condition: hash_condition,
            range_key_condition: None,
        },
        index_picker::IndexKind::Global => QueryType::GlobalSecondaryIndexQuery {
            index_name: target.name.clone(),
            hash_key_condition: hash_condition,
            range_key_condition: None,
        },
        index_picker::IndexKind::Local => QueryType::LocalSecondaryIndexQuery {
            index_name: target.name.clone(),
            hash_key_condition: hash_condition,
            range_key_condition: None,
        },
    };
    DynamoDbRequest::Query(Box::new(QueryBuilder::from_query_type(query_type)))
}

async fn create_request_from_query(
    query: &str,
    cached_meta: Option<TableMeta>,
    client: aws_sdk_dynamodb::Client,
    table_name: String,
    ctx: crate::env::WidgetCtx,
) -> Result<DynamoDbRequest, String> {
    let query = query.trim();
    if query.is_empty() {
        return Ok(DynamoDbRequest::Scan(ScanBuilder::new()));
    }
    let table_desc = match cached_meta {
        Some(meta) => meta.table_desc,
        None => {
            let meta = fetch_table_meta(client, table_name).await?;
            let desc = meta.table_desc.clone();
            ctx.emit_self(TableMetaEvent { meta });
            desc
        }
    };
    let expr = parse_query_expression(query, &table_desc)?;
    Ok(DynamoDbRequest::from_expression_and_table(
        &expr,
        &table_desc,
    ))
}

fn parse_query_expression(
    query: &str,
    table_desc: &TableDescription,
) -> Result<DynamoExpression, String> {
    match parse_dynamo_expression(query) {
        Ok(expr) => Ok(expr),
        Err(parse_error) => {
            let parse_error_text = parse_error.to_string();
            let value = match parse_single_value_token(query) {
                Ok(value) => value,
                Err(_) => return Err(parse_error_text),
            };
            let (hash_key, _) = extract_hash_range(table_desc);
            let Some(hash_key) = hash_key else {
                return Err(parse_error_text);
            };
            Ok(DynamoExpression::Comparison {
                left: Operand::Path(hash_key),
                operator: Comparator::Equal,
                right: value,
            })
        }
    }
}

async fn fetch_table_description(
    client: aws_sdk_dynamodb::Client,
    table_name: String,
) -> Result<TableDescription, String> {
    let span = tracing::trace_span!("DescribeTable", table = %table_name);
    let result = send_dynamo_request(
        span,
        || client.describe_table().table_name(&table_name).send(),
        |err| err.to_string(),
    )
    .await;
    let out = result.map_err(|e| e.to_string())?;
    let table = out
        .table()
        .ok_or_else(|| "DescribeTable: missing table".to_string())?;
    Ok(table.clone())
}

async fn fetch_ttl_attribute(
    client: aws_sdk_dynamodb::Client,
    table_name: String,
) -> Option<String> {
    let span = tracing::trace_span!("DescribeTimeToLive", table = %table_name);
    let output = send_dynamo_request(
        span,
        || {
            client
                .describe_time_to_live()
                .table_name(&table_name)
                .send()
        },
        |err| err.to_string(),
    )
    .await;
    match output {
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
    }
}

async fn fetch_table_meta(
    client: aws_sdk_dynamodb::Client,
    table_name: String,
) -> Result<TableMeta, String> {
    let table_desc = fetch_table_description(client.clone(), table_name.clone()).await?;
    let ttl_attr = fetch_ttl_attribute(client, table_name).await;
    Ok(TableMeta {
        table_desc,
        ttl_attr,
    })
}

#[derive(Debug, Clone, Copy)]
enum EditorFormat {
    Plain,
    DynamoDb,
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

fn env_u64(name: &str) -> Option<u64> {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
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

fn item_matches_filter(item: &HashMap<String, AttributeValue>, needle: &str) -> bool {
    for (key, value) in item {
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

struct ExportResultsRequest {
    items: Vec<HashMap<String, AttributeValue>>,
    filter: Option<String>,
    start_key: HashMap<String, AttributeValue>,
    active_query: ActiveQuery,
    cached_meta: Option<TableMeta>,
    client: aws_sdk_dynamodb::Client,
    table_name: String,
    ctx: crate::env::WidgetCtx,
    path: PathBuf,
    cancel: Arc<AtomicBool>,
    export_id: u64,
}

async fn export_results_full(request: ExportResultsRequest) -> Result<usize, String> {
    let ExportResultsRequest {
        mut items,
        filter,
        start_key,
        active_query,
        cached_meta,
        client,
        table_name,
        ctx,
        path,
        cancel,
        export_id,
    } = request;

    if cancel.load(Ordering::Relaxed) {
        return Err("Export canceled".to_string());
    }

    let request = match active_query {
        ActiveQuery::Text(query) => {
            create_request_from_query(
                &query,
                cached_meta,
                client.clone(),
                table_name.clone(),
                ctx.clone(),
            )
            .await?
        }
        ActiveQuery::Index(target) => request_for_index_target(&target),
    };

    let mut next_key = Some(start_key);
    while let Some(start_key) = next_key {
        if cancel.load(Ordering::Relaxed) {
            return Err("Export canceled".to_string());
        }
        let output = execute_page(&client, &table_name, &request, Some(start_key), None)
            .await
            .map_err(|err| err.to_string())?;
        for item in output.items() {
            let keep = filter
                .as_deref()
                .map(|needle| item_matches_filter(item, needle))
                .unwrap_or(true);
            if keep {
                items.push(item.clone());
            }
        }
        ctx.emit_self(ExportProgressEvent {
            export_id,
            count: items.len(),
        });
        if cancel.load(Ordering::Relaxed) {
            return Err("Export canceled".to_string());
        }
        next_key = output.last_evaluated_key().cloned();
    }

    if cancel.load(Ordering::Relaxed) {
        return Err("Export canceled".to_string());
    }
    export_results_to_path(&items, &path)
}

fn export_item_to_path(
    item: &HashMap<String, AttributeValue>,
    path: &Path,
) -> Result<usize, String> {
    let value = json::to_json(item).map_err(|err| err.to_string())?;
    write_json_to_path(path, &value)?;
    Ok(1)
}

fn export_results_to_path(
    items: &[HashMap<String, AttributeValue>],
    path: &Path,
) -> Result<usize, String> {
    let values = items_to_json_values(items)?;
    write_json_to_path(path, &serde_json::Value::Array(values))?;
    Ok(items.len())
}

fn items_to_json_values(
    items: &[HashMap<String, AttributeValue>],
) -> Result<Vec<serde_json::Value>, String> {
    let mut values = Vec::with_capacity(items.len());
    for (idx, item) in items.iter().enumerate() {
        let value = json::to_json(item)
            .map_err(|err| format!("Failed to convert item {}: {err}", idx + 1))?;
        values.push(value);
    }
    Ok(values)
}

fn write_json_to_path(path: &Path, value: &serde_json::Value) -> Result<(), String> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Failed to create export directory: {err}"))?;
    }
    let payload = serde_json::to_string_pretty(value).map_err(|err| err.to_string())?;
    fs::write(path, payload).map_err(|err| err.to_string())?;
    Ok(())
}

fn export_base_dir() -> PathBuf {
    match env::current_dir() {
        Ok(dir) => dir,
        Err(_) => env::temp_dir(),
    }
}

fn export_file_name(table_name: &str, mode: ExportMode, timestamp_ms: u128) -> String {
    let table = sanitize_export_component(table_name);
    let label = match mode {
        ExportMode::Item => "item",
        ExportMode::Results => "results",
    };
    format!("dynamate-export-{}-{}-{}.json", table, label, timestamp_ms)
}

fn export_results_file_name(table_name: &str, query: Option<&str>, timestamp_ms: u128) -> String {
    let table = sanitize_export_component(table_name);
    let query = query
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(sanitize_query_component);
    match query {
        Some(query) => format!("{table}-{query}_{timestamp_ms}.json"),
        None => format!("{table}_{timestamp_ms}.json"),
    }
}

fn attribute_value_for_filename(value: &AttributeValue) -> String {
    if let Ok(v) = value.as_s() {
        v.clone()
    } else if let Ok(v) = value.as_n() {
        v.clone()
    } else if let Ok(v) = value.as_bool() {
        v.to_string()
    } else if value.as_null().is_ok() {
        "null".to_string()
    } else if let Ok(v) = value.as_b() {
        format!("binary{}", v.as_ref().len())
    } else if let Ok(v) = value.as_ss() {
        format!("ss{}", v.len())
    } else if let Ok(v) = value.as_ns() {
        format!("ns{}", v.len())
    } else if let Ok(v) = value.as_bs() {
        format!("bs{}", v.len())
    } else if let Ok(v) = value.as_l() {
        format!("list{}", v.len())
    } else if let Ok(v) = value.as_m() {
        format!("map{}", v.len())
    } else {
        "value".to_string()
    }
}

fn sanitize_filename_component(raw: &str, fallback: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

fn sanitize_export_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "table".to_string()
    } else {
        trimmed.to_string()
    }
}

fn sanitize_query_component(raw: &str) -> Option<String> {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
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

fn query_footer_label(
    output: Option<&Output>,
    active_query: &ActiveQuery,
    table_desc: Option<&TableDescription>,
) -> Option<String> {
    let (prefix, allow_query) = match output.map(|o| o.kind()) {
        Some(Kind::Scan) => ("scan".to_string(), true),
        Some(Kind::Query) => ("query".to_string(), true),
        Some(Kind::QueryGSI(index_name)) => (format!("query@{index_name}"), true),
        Some(Kind::QueryLSI(index_name)) => (format!("query@{index_name}"), true),
        None => return None,
    };
    let query = if allow_query {
        normalized_query(active_query, table_desc)
    } else {
        None
    };
    match query {
        Some(text) if !text.is_empty() => Some(format!("{prefix} {text}")),
        _ => Some(prefix),
    }
}

fn normalized_query(
    active_query: &ActiveQuery,
    table_desc: Option<&TableDescription>,
) -> Option<String> {
    let raw = active_query.input_value()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let parsed = match table_desc {
        Some(table_desc) => parse_query_expression(trimmed, table_desc).ok(),
        None => parse_dynamo_expression(trimmed).ok(),
    };
    match parsed {
        Some(expr) => Some(format_query_summary(&expr)),
        None => Some(trimmed.to_string()),
    }
}

fn format_query_summary(expr: &dynamate::expr::DynamoExpression) -> String {
    if !contains_or_or_not(expr) {
        let mut parts = Vec::new();
        collect_and_parts(expr, &mut parts);
        return parts
            .into_iter()
            .map(format_expr_compact)
            .collect::<Vec<_>>()
            .join(" ");
    }
    format_expr(expr, 0)
}

fn contains_or_or_not(expr: &dynamate::expr::DynamoExpression) -> bool {
    use dynamate::expr::DynamoExpression::*;
    match expr {
        Or(_, _) | Not(_) => true,
        And(left, right) => contains_or_or_not(left) || contains_or_or_not(right),
        Parentheses(inner) => contains_or_or_not(inner),
        Comparison { .. } | Between { .. } | In { .. } | Function { .. } => false,
    }
}

fn collect_and_parts<'a>(
    expr: &'a dynamate::expr::DynamoExpression,
    parts: &mut Vec<&'a dynamate::expr::DynamoExpression>,
) {
    use dynamate::expr::DynamoExpression::*;
    match expr {
        And(left, right) => {
            collect_and_parts(left, parts);
            collect_and_parts(right, parts);
        }
        Parentheses(inner) => collect_and_parts(inner, parts),
        _ => parts.push(expr),
    }
}

fn format_expr(expr: &dynamate::expr::DynamoExpression, parent_prec: u8) -> String {
    use dynamate::expr::DynamoExpression::*;
    let my_prec = match expr {
        Or(_, _) => 1,
        And(_, _) => 2,
        Not(_) => 3,
        _ => 4,
    };
    let rendered = match expr {
        Comparison {
            left,
            operator,
            right,
        } => {
            format!(
                "{}{}{}",
                format_operand(left),
                format_comparator(operator),
                format_operand(right)
            )
        }
        Between {
            operand,
            lower,
            upper,
        } => {
            format!(
                "{} BETWEEN {} AND {}",
                format_operand(operand),
                format_operand(lower),
                format_operand(upper)
            )
        }
        In { operand, values } => {
            let values = values
                .iter()
                .map(format_operand)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{} IN ({values})", format_operand(operand))
        }
        Function { name, args } => {
            let args = args
                .iter()
                .map(format_operand)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({})", format_function_name(name), args)
        }
        And(left, right) => {
            format!(
                "{} AND {}",
                format_expr(left, my_prec),
                format_expr(right, my_prec)
            )
        }
        Or(left, right) => {
            format!(
                "{} OR {}",
                format_expr(left, my_prec),
                format_expr(right, my_prec)
            )
        }
        Not(inner) => format!("NOT {}", format_expr(inner, my_prec)),
        Parentheses(inner) => format!("({})", format_expr(inner, 0)),
    };
    if my_prec < parent_prec {
        format!("({rendered})")
    } else {
        rendered
    }
}

fn format_expr_compact(expr: &dynamate::expr::DynamoExpression) -> String {
    use dynamate::expr::DynamoExpression::*;
    match expr {
        Comparison {
            left,
            operator,
            right,
        } => {
            format!(
                "{}{}{}",
                format_operand(left),
                format_comparator(operator),
                format_operand(right)
            )
        }
        Between {
            operand,
            lower,
            upper,
        } => {
            format!(
                "{} BETWEEN {} AND {}",
                format_operand(operand),
                format_operand(lower),
                format_operand(upper)
            )
        }
        In { operand, values } => {
            let values = values
                .iter()
                .map(format_operand)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{} IN ({values})", format_operand(operand))
        }
        Function { name, args } => {
            let args = args
                .iter()
                .map(format_operand)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({})", format_function_name(name), args)
        }
        Parentheses(inner) => format!("({})", format_expr(inner, 0)),
        And(_, _) | Or(_, _) | Not(_) => format_expr(expr, 0),
    }
}

fn format_operand(operand: &dynamate::expr::Operand) -> String {
    use dynamate::expr::Operand;
    match operand {
        Operand::Path(path) => format_path(path),
        Operand::Value(value) => format_string(value),
        Operand::Number(num) => format_number(*num),
        Operand::Boolean(value) => value.to_string(),
        Operand::Null => "null".to_string(),
    }
}

fn format_comparator(comp: &dynamate::expr::Comparator) -> &'static str {
    use dynamate::expr::Comparator::*;
    match comp {
        Equal => "=",
        NotEqual => "!=",
        Less => "<",
        LessOrEqual => "<=",
        Greater => ">",
        GreaterOrEqual => ">=",
    }
}

fn format_function_name(name: &dynamate::expr::FunctionName) -> &'static str {
    use dynamate::expr::FunctionName::*;
    match name {
        AttributeExists => "attribute_exists",
        AttributeNotExists => "attribute_not_exists",
        AttributeType => "attribute_type",
        BeginsWith => "begins_with",
        Contains => "contains",
        Size => "size",
    }
}

fn format_path(path: &str) -> String {
    if path.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        path.to_string()
    } else {
        format!("`{}`", path)
    }
}

fn format_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("\"{}\"", value))
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{:.0}", value)
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_description_with_hash_key(hash_key: &str) -> TableDescription {
        let key = KeySchemaElement::builder()
            .attribute_name(hash_key)
            .key_type(KeyType::Hash)
            .build()
            .expect("hash key schema should be valid");
        TableDescription::builder()
            .table_name("demo")
            .key_schema(key)
            .build()
    }

    #[test]
    fn sanitize_export_component_rewrites_invalid_chars() {
        assert_eq!(sanitize_export_component("My Table"), "my_table");
        assert_eq!(sanitize_export_component("Table/Name"), "table_name");
        assert_eq!(sanitize_export_component("___"), "table");
    }

    #[test]
    fn sanitize_filename_component_preserves_safe_chars() {
        assert_eq!(
            sanitize_filename_component("PK-Name_01", "fallback"),
            "PK-Name_01"
        );
        assert_eq!(
            sanitize_filename_component("Value/With Spaces", "fallback"),
            "Value_With_Spaces"
        );
        assert_eq!(sanitize_filename_component("___", "fallback"), "fallback");
    }

    #[test]
    fn export_file_name_is_stable() {
        let name = export_file_name("My Table", ExportMode::Results, 12345);
        assert_eq!(name, "dynamate-export-my_table-results-12345.json");
    }

    #[test]
    fn export_results_file_name_includes_query() {
        let name = export_results_file_name("My Table", Some("status = Active"), 12345);
        assert_eq!(name, "my_table-status___active_12345.json");
    }

    #[test]
    fn export_results_file_name_without_query() {
        let name = export_results_file_name("My Table", None, 12345);
        assert_eq!(name, "my_table_12345.json");
    }

    #[test]
    fn export_results_file_name_ignores_empty_query() {
        let name = export_results_file_name("My Table", Some("!!!"), 12345);
        assert_eq!(name, "my_table_12345.json");
    }

    #[test]
    fn parse_query_expression_uses_primary_hash_key_shortcut() {
        let table_desc = table_description_with_hash_key("PK");
        let parsed = parse_query_expression("customer_123", &table_desc).unwrap();
        assert_eq!(
            parsed,
            DynamoExpression::Comparison {
                left: Operand::Path("PK".to_string()),
                operator: Comparator::Equal,
                right: Operand::Value("customer_123".to_string()),
            }
        );
    }

    #[test]
    fn parse_query_expression_shortcut_supports_quoted_scalars() {
        let table_desc = table_description_with_hash_key("PK");
        let parsed = parse_query_expression(r#""foo bar""#, &table_desc).unwrap();
        assert_eq!(
            parsed,
            DynamoExpression::Comparison {
                left: Operand::Path("PK".to_string()),
                operator: Comparator::Equal,
                right: Operand::Value("foo bar".to_string()),
            }
        );
    }

    #[test]
    fn parse_query_expression_shortcut_rejects_backticks() {
        let table_desc = table_description_with_hash_key("PK");
        let err = parse_query_expression("`other field`", &table_desc).unwrap_err();
        assert!(err.contains("Expected comparison operator"));
    }

    #[test]
    fn normalized_query_applies_pk_shortcut_with_table_metadata() {
        let table_desc = table_description_with_hash_key("PK");
        let query = ActiveQuery::Text("foo".to_string());
        let normalized = normalized_query(&query, Some(&table_desc));
        assert_eq!(normalized.as_deref(), Some("PK=\"foo\""));
    }

    #[test]
    fn normalized_query_keeps_raw_single_token_without_table_metadata() {
        let query = ActiveQuery::Text("foo".to_string());
        let normalized = normalized_query(&query, None);
        assert_eq!(normalized.as_deref(), Some("foo"));
    }
}
