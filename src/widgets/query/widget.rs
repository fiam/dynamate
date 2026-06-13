use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    cmp::{max, min},
    collections::{HashMap, HashSet},
    env,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_sdk_dynamodb::types::{
    AttributeValue, DeleteRequest, KeySchemaElement, KeyType, TableDescription, TimeToLiveStatus,
    WriteRequest,
};
use crossterm::cursor::MoveTo;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers};
use crossterm::terminal::{
    Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Margin, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, HighlightSpacing, Paragraph, Row, Scrollbar, ScrollbarOrientation, ScrollbarState,
        StatefulWidget, Table, TableState,
    },
};

use super::{
    completion::{self, Suggestion, SuggestionKind, TokenSpan},
    export_popup::ExportPopup,
    expr_format, index_picker, input, item_keys, keys_widget,
    reference_popup::ReferencePopup,
    selection::{ItemKey, SelectionMode, SelectionSnapshot},
    tree,
};
use keys_widget::KeysWidget;

use crate::{
    env::{Toast, ToastAction, ToastKind},
    help,
    util::{abbreviate_home, env_flag, fill_bg, pad},
    widgets::{
        WidgetInner,
        confirm::{ConfirmAction, ConfirmPopup},
        error::ErrorPopup,
        filter_input::FilterInput,
        theme::Theme,
    },
};
use chrono::{DateTime, Utc};
use dynamate::dynamodb::json;
use dynamate::dynamodb::size::estimate_item_size_bytes;
use dynamate::dynamodb::{SecondaryIndex, TableInfo, format_sdk_error, send_dynamo_request};
use dynamate::{
    dynamodb::{
        DynamoDbRequest, KeyCondition, KeyConditionType, Kind, Output, QueryBuilder, QueryType,
        ScanBuilder, execute_page,
    },
    expr::{
        Comparator, DynamoExpression, Operand, ParseError, parse_dynamo_expression,
        parse_single_value_token,
    },
};
use humansize::{BINARY, format_size};
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
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
    column_offset: usize,
    compact_columns: bool,
    tree_scroll_offset: usize,
    tree_render_capacity: usize,
    tree_line_count: usize,
    selection: SelectionMode,
    completion: Completion,
}

/// Autocompletion state for the query input. Suggestions are recomputed from the
/// input text + cursor whenever the text changes; `dismissed` suppresses the
/// dropdown (after Esc) until the next edit.
///
/// When `has_sentinel` is set (the user hasn't typed a prefix the suggestions
/// match), the dropdown shows a leading "no selection" row, highlighted by
/// default, so pressing Enter runs the query instead of accepting a suggestion.
/// `selected` indexes a virtual list where index 0 is the sentinel (if present)
/// and the items follow.
#[derive(Default)]
struct Completion {
    visible: bool,
    dismissed: bool,
    has_sentinel: bool,
    selected: usize,
    items: Vec<Suggestion>,
    span: TokenSpan,
}

/// Width of the gutter that shows the selection bar (`▌`) for selected
/// rows. Only rendered while a selection is active.
const SELECTION_GUTTER_WIDTH: u16 = 1;
/// Glyph drawn in the selection gutter for a selected row.
const SELECTION_BAR: &str = "▌";
const TABLE_RENDER_CHROME_WIDTH: usize = 4;
const TABLE_COLUMN_SPACING: usize = 1;
const TABLE_MIN_COLUMN_WIDTH: usize = 1;
const TABLE_MAX_COLUMN_WIDTH: usize = 48;
const TABLE_MAX_COLUMN_WIDTH_COMPACT: usize = 20;
const TABLE_MAX_RENDER_COLUMNS: usize = 24;
const MAX_DROPDOWN_ROWS: usize = 8;

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

struct DeleteSelectionRequest {
    selection: SelectionSnapshot,
}

struct DeleteSelectionEvent {
    result: Result<usize, String>,
}

struct IndexQueryEvent {
    target: index_picker::IndexTarget,
}

struct KeyVisibilityEvent {
    name: String,
    hidden: bool,
}

struct ExportRequest {
    mode: ExportKind,
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
    mode: ExportKind,
    path: PathBuf,
    count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExportKind {
    Item,
    Selection,
    Results,
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

impl Completion {
    /// Number of selectable rows, including the leading sentinel when present.
    fn row_count(&self) -> usize {
        self.items.len() + self.has_sentinel as usize
    }

    /// The item index for the current selection, or `None` when the sentinel
    /// ("run query / no completion") row is selected.
    fn selected_item(&self) -> Option<usize> {
        if self.has_sentinel {
            self.selected.checked_sub(1)
        } else {
            Some(self.selected)
        }
    }

    fn select_next(&mut self) {
        let total = self.row_count();
        if total > 0 {
            self.selected = (self.selected + 1) % total;
        }
    }

    fn select_prev(&mut self) {
        let total = self.row_count();
        if total > 0 {
            self.selected = (self.selected + total - 1) % total;
        }
    }

    fn dismiss(&mut self) {
        self.visible = false;
        self.dismissed = true;
    }
}

impl QueryState {
    /// Recompute completion suggestions from the current input text, cursor and
    /// the attribute names seen in loaded items.
    fn refresh_completion(&mut self) {
        let value = self.input.value().to_string();
        let cursor = self.input.cursor_byte();
        let attrs: Vec<String> = self.item_keys.sorted().to_vec();
        let items_ref = &self.items;
        let dialect = dynamate::expr::builtins::default_dialect();
        let (span, items) = completion::suggestions(&value, cursor, &attrs, dialect, |path| {
            collect_attribute_values(items_ref, path)
        });
        // When the current text is already a runnable query, the user may want to
        // run it as-is, so show the sentinel row and default to it (Enter runs).
        // When it isn't runnable yet, default to the first suggestion so Enter
        // makes progress instead of erroring.
        self.completion.has_sentinel = query_is_runnable(&value);
        self.completion.span = span;
        self.completion.items = items;
        self.completion.selected = 0;
        self.completion.visible = !self.completion.dismissed && !self.completion.items.is_empty();
    }

    /// Forget any dismissal so suggestions can show again (e.g. after the input
    /// is (re)activated or its text changes).
    fn reset_completion_dismissal(&mut self) {
        self.completion.dismissed = false;
    }

    /// Replace the token under the cursor with the highlighted suggestion.
    ///
    /// Returns false without changing anything when the sentinel row is selected
    /// (and `fallback_first` is not set), letting the caller run the query
    /// instead. When `fallback_first` is set (Tab), the sentinel completes the
    /// first item.
    fn accept_completion(&mut self, fallback_first: bool) -> bool {
        let idx = match self.completion.selected_item() {
            Some(idx) => idx,
            None if fallback_first => 0,
            None => return false,
        };
        let Some(suggestion) = self.completion.items.get(idx) else {
            return false;
        };
        let span = self.completion.span;
        let text = suggestion.text.clone();
        self.input.replace_token(span.start, span.end, &text);
        // Recompute against the new text; functions insert a trailing "(" so the
        // next suggestion round naturally targets the first argument.
        self.refresh_completion();
        true
    }

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
            self.reset_tree_scroll();
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
        self.reset_tree_scroll();
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

    fn reset_tree_scroll(&mut self) {
        self.tree_scroll_offset = 0;
    }

    fn clamp_tree_offset(&mut self) {
        let viewport = self.tree_render_capacity.max(1);
        let max_offset = self.tree_line_count.saturating_sub(viewport);
        self.tree_scroll_offset = self.tree_scroll_offset.min(max_offset);
    }

    fn scroll_tree_down(&mut self) {
        self.clamp_tree_offset();
        let viewport = self.tree_render_capacity.max(1);
        let max_offset = self.tree_line_count.saturating_sub(viewport);
        self.tree_scroll_offset = (self.tree_scroll_offset + 1).min(max_offset);
    }

    fn scroll_tree_up(&mut self) {
        self.tree_scroll_offset = self.tree_scroll_offset.saturating_sub(1);
    }

    fn page_tree_down(&mut self) {
        self.clamp_tree_offset();
        let viewport = self.tree_render_capacity.max(1);
        let max_offset = self.tree_line_count.saturating_sub(viewport);
        self.tree_scroll_offset = self
            .tree_scroll_offset
            .saturating_add(viewport)
            .min(max_offset);
    }

    fn page_tree_up(&mut self) {
        let viewport = self.tree_render_capacity.max(1);
        self.tree_scroll_offset = self.tree_scroll_offset.saturating_sub(viewport);
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

    fn status(&self) -> crate::widgets::StatusInfo {
        let state = self.state.borrow();

        // Title-bar context: table name · region · approximate item count.
        let mut context_parts = vec![self.table_name.clone()];
        if let Some(region) = env::var("AWS_REGION")
            .or_else(|_| env::var("AWS_DEFAULT_REGION"))
            .ok()
            .filter(|r| !r.is_empty())
        {
            context_parts.push(region);
        }
        if let Some(count) = self
            .table_meta
            .borrow()
            .as_ref()
            .and_then(|meta| meta.table_desc.item_count())
        {
            context_parts.push(format!("~{count} items"));
        }
        let context = Some(context_parts.join(" · "));

        // Mode chip reflects the dominant interaction state.
        let mode = if matches!(state.loading_state, LoadingState::Error(_)) {
            "ERROR"
        } else if state.show_tree {
            "ITEM"
        } else if state.input.is_active() {
            "QUERY"
        } else if state.filter.is_active() {
            "FILTER"
        } else if state.selection.is_active() {
            "SELECT"
        } else if matches!(state.loading_state, LoadingState::Loading) {
            "LOADING"
        } else {
            "BROWSE"
        };

        // Stats: result count plus any selection summary (skipped in tree view).
        let stats = if state.show_tree {
            None
        } else {
            let mut parts = vec![format!("{} results", state.filtered_indices.len())];
            if let Some(selection) = self.selection_status(&state) {
                parts.push(selection);
            }
            Some(parts.join(" · "))
        };

        crate::widgets::StatusInfo {
            context,
            mode: Some(mode.to_string()),
            stats,
        }
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
            let completion_visible = state.completion.visible;
            let dropdown_h = if completion_visible {
                state.completion.row_count().min(MAX_DROPDOWN_ROWS + 1) as u16
            } else {
                0
            };
            // Query region = bordered box (3) + hint line (1) + dropdown rows,
            // clamped so it never consumes more than half the available height.
            let mut query_region_h = 3 + 1 + dropdown_h;
            let cap = (area.height / 2).max(4);
            if query_region_h > cap {
                query_region_h = cap;
            }

            let mut constraints = Vec::new();
            if query_active {
                constraints.push(Constraint::Length(query_region_h));
            }
            if filter_active {
                constraints.push(Constraint::Length(3));
            }
            constraints.push(Constraint::Fill(1));
            let areas = Layout::vertical(constraints).split(area);

            let mut idx = 0;
            if query_active {
                let region = areas[idx];
                let actual_dropdown = region.height.saturating_sub(4);
                let sub = Layout::vertical([
                    Constraint::Length(3),
                    Constraint::Length(1),
                    Constraint::Length(actual_dropdown),
                ])
                .split(region);
                state.input.render(frame, sub[0], theme);
                let hint = self.query_hint_line(state.input.value(), theme);
                frame.render_widget(Paragraph::new(hint), sub[1]);
                if completion_visible && actual_dropdown > 0 {
                    self.render_completion(frame, sub[2], theme, &state.completion);
                }
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
        self.reset_error_state_on_key(event);
        let input_is_active = self.state.borrow().input.is_active();
        let filter_active = self.state.borrow().filter.is_active();

        if input_is_active && self.handle_query_input_key(&ctx, event) {
            return true;
        }
        if filter_active && self.handle_filter_key(event) {
            return true;
        }
        if let Some(key) = event.as_key_press_event() {
            return self.handle_browse_key(&ctx, key, input_is_active, filter_active);
        }
        if let Some(mouse) = event.as_mouse_event() {
            match mouse.kind {
                crossterm::event::MouseEventKind::ScrollUp => self.scroll_up(),
                crossterm::event::MouseEventKind::ScrollDown => self.scroll_down(ctx.clone()),
                _ => {}
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
        } else if state.selection.is_active() {
            Some(Self::HELP_SELECTION)
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
                let filename = export_request.path.file_name().map_or_else(
                    || export_request.path.display().to_string(),
                    |name| name.to_string_lossy().to_string(),
                );
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
                        ExportKind::Item => format!("Exported to {display_path}"),
                        ExportKind::Selection => {
                            format!(
                                "Exported {} selected items to {}",
                                outcome.count, display_path
                            )
                        }
                        ExportKind::Results => {
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

        if let Some(delete_event) = event.payload::<DeleteSelectionRequest>() {
            self.delete_selection(delete_event.selection.clone(), ctx);
            return;
        }

        if let Some(delete_event) = event.payload::<DeleteItemEvent>() {
            match delete_event.result.as_ref() {
                Ok(()) => {
                    self.set_loading_state(LoadingState::Loaded);
                    self.remove_item_by_key(&delete_event.key);
                    self.remove_selection_key(&delete_event.key);
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

        if let Some(delete_event) = event.payload::<DeleteSelectionEvent>() {
            match delete_event.result {
                Ok(count) => {
                    self.clear_selection();
                    ctx.show_toast(Toast {
                        message: format!("Deleted {count} items"),
                        kind: ToastKind::Info,
                        duration: Duration::from_secs(4),
                        action: None,
                    });
                    let active_query = self.state.borrow().active_query.clone();
                    self.restart_query(active_query, ctx.clone(), None);
                }
                Err(ref err) => {
                    let message = format!("Failed to delete selection: {err}");
                    self.set_loading_state(LoadingState::Error(message.clone()));
                    self.show_error(ctx.clone(), &message);
                    ctx.invalidate();
                }
            }
            return;
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
    /// On any keypress, clear a transient error banner so the next keystroke
    /// starts from a clean state (returning to Idle or Loaded as appropriate).
    fn reset_error_state_on_key(&self, event: &Event) {
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
    }

    /// Handle a key while the query input is active: completion-dropdown
    /// navigation and text editing. Returns `true` when the key is fully
    /// consumed; `false` lets it fall through to the browse-mode handler (e.g.
    /// Enter on the sentinel completion row runs the query).
    fn handle_query_input_key(&self, ctx: &crate::env::WidgetCtx, event: &Event) -> bool {
        let dropdown_visible = self.state.borrow().completion.visible;
        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Char('g') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    // Surface the query reference. Ctrl+G is layout- and
                    // macOS-safe with no legacy terminal collision; intercepted
                    // before the text input so `?` stays typable.
                    self.open_reference_popup(ctx.clone());
                    return true;
                }
                KeyCode::Up if dropdown_visible => {
                    self.state.borrow_mut().completion.select_prev();
                    return true;
                }
                KeyCode::Down if dropdown_visible => {
                    self.state.borrow_mut().completion.select_next();
                    return true;
                }
                KeyCode::Tab if dropdown_visible => {
                    // Tab always completes — on the sentinel, the first item.
                    self.state.borrow_mut().accept_completion(true);
                    return true;
                }
                KeyCode::Enter if dropdown_visible => {
                    // Accept the highlighted suggestion; if the sentinel row is
                    // selected, fall through so Enter runs the query.
                    if self.state.borrow_mut().accept_completion(false) {
                        return true;
                    }
                }
                KeyCode::Esc if dropdown_visible => {
                    self.state.borrow_mut().completion.dismiss();
                    return true;
                }
                _ => {}
            }
        }
        // Delegate to the text input, then refresh suggestions on any edit.
        let handled = self.state.borrow_mut().input.handle_event(event);
        if handled {
            let mut state = self.state.borrow_mut();
            state.reset_completion_dismissal();
            state.refresh_completion();
            return true;
        }
        false
    }

    /// Handle a key while the results filter is active. Returns `true` when the
    /// filter consumed it (and the visible rows were re-filtered).
    fn handle_filter_key(&self, event: &Event) -> bool {
        let mut state = self.state.borrow_mut();
        if state.filter.handle_event(event) {
            state.apply_filter();
            return true;
        }
        false
    }

    /// Handle a key in browse/tree mode (the main keymap). Returns `true` when
    /// the key was handled, `false` for unrecognized keys.
    fn handle_browse_key(
        &self,
        ctx: &crate::env::WidgetCtx,
        key: crossterm::event::KeyEvent,
        input_is_active: bool,
        filter_active: bool,
    ) -> bool {
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
                } else if state.selection.is_active() {
                    state.selection.clear();
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
                    state.completion.visible = false;
                    value
                };
                self.start_query(Some(&query), ctx.clone());
            }
            KeyCode::Enter => {
                let mut state = self.state.borrow_mut();
                if !state.show_tree {
                    state.show_tree = true;
                    state.reset_tree_scroll();
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
                    state.reset_completion_dismissal();
                    state.refresh_completion();
                }
            }
            KeyCode::Char('j') | KeyCode::Down => self.scroll_down(ctx.clone()),
            KeyCode::Char('k') | KeyCode::Up => self.scroll_up(),
            KeyCode::Char('J') if self.state.borrow().show_tree => self.tree_next_item(ctx.clone()),
            KeyCode::Char('K') if self.state.borrow().show_tree => self.tree_prev_item(),
            KeyCode::PageDown => self.page_down(ctx.clone()),
            KeyCode::PageUp => self.page_up(),
            KeyCode::Left
                if !input_is_active && !filter_active && !self.state.borrow().show_tree =>
            {
                self.scroll_columns_left();
            }
            KeyCode::Right
                if !input_is_active && !filter_active && !self.state.borrow().show_tree =>
            {
                self.scroll_columns_right();
            }
            KeyCode::Char('z')
                if !input_is_active && !filter_active && !self.state.borrow().show_tree =>
            {
                self.toggle_compact_columns();
            }
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
                    self.show_export_popup(ExportKind::Item, ctx.clone());
                } else if self.selection_active() {
                    self.show_export_popup(ExportKind::Selection, ctx.clone());
                } else {
                    self.show_export_popup(ExportKind::Results, ctx.clone());
                }
            }
            KeyCode::Char(' ')
                if !input_is_active && !filter_active && !self.state.borrow().show_tree =>
            {
                match self.toggle_selected_row() {
                    // Advance to the next row so a run of consecutive
                    // items can be selected by tapping space.
                    Ok(()) => self.scroll_down(ctx.clone()),
                    Err(err) => self.show_error(ctx.clone(), &err),
                }
            }
            KeyCode::Char('a')
                if !input_is_active && !filter_active && !self.state.borrow().show_tree =>
            {
                self.select_all_query_matches();
            }
            KeyCode::Char('v')
                if !input_is_active && !filter_active && !self.state.borrow().show_tree =>
            {
                self.invert_selection();
            }
            KeyCode::Char('t') => {
                let mut state = self.state.borrow_mut();
                state.show_tree = !state.show_tree;
                if state.show_tree {
                    state.reset_tree_scroll();
                }
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
                    && self.state.borrow().show_tree
                    && key
                        .modifiers
                        .contains(crossterm::event::KeyModifiers::CONTROL) =>
            {
                self.confirm_delete(ctx.clone());
            }
            KeyCode::Char('d')
                if !input_is_active
                    && !filter_active
                    && !self.state.borrow().show_tree
                    && key
                        .modifiers
                        .contains(crossterm::event::KeyModifiers::CONTROL) =>
            {
                if self.selection_active() {
                    self.confirm_delete_selection(ctx.clone());
                } else {
                    self.confirm_delete(ctx.clone());
                }
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
        true
    }

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
            keys: Cow::Borrowed("←/→"),
            short: Cow::Borrowed("columns"),
            long: Cow::Borrowed("Scroll columns"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("z"),
            short: Cow::Borrowed("compact"),
            long: Cow::Borrowed("Toggle compact columns"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("space/a"),
            short: Cow::Borrowed("select"),
            long: Cow::Borrowed("Toggle row/select all query matches"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("x"),
            short: Cow::Borrowed("export"),
            long: Cow::Borrowed("Export results/selection"),
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
                long: Some(Cow::Borrowed("Delete item/selection")),
            }),
            shift: None,
            alt: None,
        },
    ];
    const HELP_SELECTION: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("space"),
            short: Cow::Borrowed("toggle"),
            long: Cow::Borrowed("Toggle row"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("a"),
            short: Cow::Borrowed("all"),
            long: Cow::Borrowed("Select all query matches"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("v"),
            short: Cow::Borrowed("invert"),
            long: Cow::Borrowed("Invert loaded selection"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("clear"),
            long: Cow::Borrowed("Clear selection"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("x"),
            short: Cow::Borrowed("export"),
            long: Cow::Borrowed("Export selection"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("⏎"),
            short: Cow::Borrowed("view"),
            long: Cow::Borrowed("View focused item"),
            ctrl: None,
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
                long: Some(Cow::Borrowed("Delete selection")),
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
            long: Cow::Borrowed("Close query input / dismiss suggestions"),
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
        help::Entry {
            keys: Cow::Borrowed("↑/↓"),
            short: Cow::Borrowed("suggest"),
            long: Cow::Borrowed("Move through suggestions"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("tab"),
            short: Cow::Borrowed("complete"),
            long: Cow::Borrowed("Accept the highlighted suggestion"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("^g"),
            short: Cow::Borrowed("reference"),
            long: Cow::Borrowed("Open the query reference"),
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
            keys: Cow::Borrowed("←/→"),
            short: Cow::Borrowed("columns"),
            long: Cow::Borrowed("Scroll columns"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("z"),
            short: Cow::Borrowed("compact"),
            long: Cow::Borrowed("Toggle compact columns"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("space/a"),
            short: Cow::Borrowed("select"),
            long: Cow::Borrowed("Toggle row/select all query matches"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("x"),
            short: Cow::Borrowed("export"),
            long: Cow::Borrowed("Export results/selection"),
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
            keys: Cow::Borrowed("^d"),
            short: Cow::Borrowed("delete"),
            long: Cow::Borrowed("Delete item/selection"),
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
            short: Cow::Borrowed("scroll"),
            long: Cow::Borrowed("Scroll item"),
            ctrl: None,
            shift: Some(help::Variant {
                keys: Some(Cow::Borrowed("J/K")),
                short: Some(Cow::Borrowed("next/prev")),
                long: Some(Cow::Borrowed("Next/previous item")),
            }),
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("PgUp/PgDn"),
            short: Cow::Borrowed("page"),
            long: Cow::Borrowed("Page through item"),
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

    fn table_desc(&self) -> Result<TableDescription, String> {
        self.table_meta
            .borrow()
            .as_ref()
            .map(|meta| meta.table_desc.clone())
            .ok_or_else(|| "Table metadata is not available yet".to_string())
    }

    fn selected_item_index(&self) -> Result<usize, String> {
        let state = self.state.borrow();
        state
            .table_state
            .selected()
            .and_then(|idx| state.filtered_indices.get(idx).copied())
            .ok_or_else(|| "No item selected".to_string())
    }

    fn item_key_at_index(&self, index: usize) -> Result<ItemKey, String> {
        let table_desc = self.table_desc()?;
        let state = self.state.borrow();
        let item = state
            .items
            .get(index)
            .ok_or_else(|| "No item selected".to_string())?;
        ItemKey::from_item(&item.0, &table_desc)
    }

    fn selected_item_key(&self) -> Result<ItemKey, String> {
        let index = self.selected_item_index()?;
        self.item_key_at_index(index)
    }

    fn selection_snapshot(&self) -> Option<SelectionSnapshot> {
        self.state.borrow().selection.snapshot()
    }

    fn selection_active(&self) -> bool {
        self.state.borrow().selection.is_active()
    }

    fn clear_selection(&self) {
        self.state.borrow_mut().selection.clear();
    }

    fn select_all_query_matches(&self) {
        self.state.borrow_mut().selection = SelectionMode::Query {
            excluded: HashSet::new(),
        };
    }

    /// Toggle the selected state of every currently-loaded row, leaving
    /// not-yet-loaded rows untouched. Works uniformly across modes:
    /// with no selection it selects all loaded rows; in `Explicit` it
    /// flips membership of each loaded key; in `Query` it flips each
    /// loaded key's exclusion.
    fn invert_selection(&self) {
        let Ok(table_desc) = self.table_desc() else {
            return;
        };
        let mut state = self.state.borrow_mut();
        let loaded_keys: Vec<ItemKey> = state
            .items
            .iter()
            .filter_map(|item| ItemKey::from_item(&item.0, &table_desc).ok())
            .collect();
        state.selection.invert_loaded(loaded_keys);
    }

    fn toggle_selected_row(&self) -> Result<(), String> {
        let key = self.selected_item_key()?;
        let mut state = self.state.borrow_mut();
        let loaded_complete = state.last_evaluated_key.is_none();
        let loaded_count = state.items.len();
        let mut clear_selection = false;
        match &mut state.selection {
            SelectionMode::None => {
                let mut keys = HashSet::new();
                keys.insert(key);
                state.selection = SelectionMode::Explicit(keys);
            }
            SelectionMode::Explicit(keys) => {
                if !keys.remove(&key) {
                    keys.insert(key);
                }
                if keys.is_empty() {
                    state.selection = SelectionMode::None;
                }
            }
            SelectionMode::Query { excluded } => {
                if !excluded.remove(&key) {
                    excluded.insert(key);
                }
                clear_selection = loaded_complete && excluded.len() >= loaded_count;
            }
        }
        if clear_selection {
            state.selection = SelectionMode::None;
        }
        Ok(())
    }

    fn remove_selection_key(&self, key: &HashMap<String, AttributeValue>) {
        let Ok(table_desc) = self.table_desc() else {
            return;
        };
        let Ok(item_key) = ItemKey::from_item(key, &table_desc) else {
            return;
        };
        self.state.borrow_mut().selection.remove_key(&item_key);
    }

    fn selection_status(&self, state: &QueryState) -> Option<String> {
        match &state.selection {
            SelectionMode::None => None,
            SelectionMode::Explicit(keys) => Some(format!("selected {}", keys.len())),
            SelectionMode::Query { excluded } => {
                if state.last_evaluated_key.is_none()
                    && matches!(
                        state.loading_state,
                        LoadingState::Idle | LoadingState::Loaded
                    )
                {
                    let total = state.items.len().saturating_sub(excluded.len());
                    return Some(format!("selected {total}"));
                }
                if excluded.is_empty() {
                    Some("all matching selected".to_string())
                } else {
                    Some(format!(
                        "all matching selected · {} excluded",
                        excluded.len()
                    ))
                }
            }
        }
    }

    fn item_is_selected(
        &self,
        item: &Item,
        table_desc: Option<&TableDescription>,
        selection: Option<&SelectionSnapshot>,
    ) -> bool {
        let Some(selection) = selection else {
            return false;
        };
        match selection {
            SelectionSnapshot::Query { excluded } if excluded.is_empty() => true,
            SelectionSnapshot::Explicit(_) | SelectionSnapshot::Query { .. } => {
                let Some(table_desc) = table_desc else {
                    return false;
                };
                let Ok(item_key) = ItemKey::from_item(&item.0, table_desc) else {
                    return false;
                };
                selection.is_selected(&item_key)
            }
        }
    }

    fn selected_loaded_items(
        &self,
        selection: &SelectionSnapshot,
        table_desc: &TableDescription,
    ) -> Vec<HashMap<String, AttributeValue>> {
        let state = self.state.borrow();
        state
            .items
            .iter()
            .filter_map(|item| {
                let item_key = ItemKey::from_item(&item.0, table_desc).ok()?;
                if selection.is_selected(&item_key) {
                    Some(item.0.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn selected_loaded_keys(
        &self,
        selection: &SelectionSnapshot,
        table_desc: &TableDescription,
    ) -> Vec<ItemKey> {
        let state = self.state.borrow();
        state
            .items
            .iter()
            .filter_map(|item| {
                let item_key = ItemKey::from_item(&item.0, table_desc).ok()?;
                if selection.is_selected(&item_key) {
                    Some(item_key)
                } else {
                    None
                }
            })
            .collect()
    }

    fn selection_summary(&self, selection: &SelectionSnapshot) -> String {
        match selection {
            SelectionSnapshot::Explicit(keys) => {
                let mut lines = vec![format!("{} selected item(s)", keys.len())];
                for line in keys.iter().take(5).map(ItemKey::summary_line) {
                    lines.push(line);
                }
                if keys.len() > 5 {
                    lines.push(format!("... and {} more", keys.len() - 5));
                }
                lines.join("\n")
            }
            SelectionSnapshot::Query { excluded } => {
                let mut lines = vec!["All items matching the query will be affected.".to_string()];
                if !excluded.is_empty() {
                    lines.push(format!("Excluded items: {}", excluded.len()));
                }
                lines.join("\n")
            }
        }
    }

    fn open_reference_popup(&self, ctx: crate::env::WidgetCtx) {
        ctx.set_popup(Box::new(ReferencePopup::new(self.inner.id())));
    }

    /// One-line feedback shown under the query box while editing: a placeholder
    /// when empty, otherwise whether the expression is valid and whether it will
    /// run as a Query or a Scan.
    fn query_hint_line(&self, value: &str, theme: &Theme) -> Line<'static> {
        let meta = self.table_meta.borrow();
        let Some(meta) = meta.as_ref() else {
            return Line::from(Span::styled(
                "  loading table metadata…".to_string(),
                Style::default().fg(theme.text_muted()),
            ));
        };

        if value.trim().is_empty() {
            // Use the table's real partition-key name so the example doesn't
            // imply a case-insensitive `pk`.
            let hash_key = extract_hash_range(&meta.table_desc)
                .0
                .unwrap_or_else(|| "key".to_string());
            return Line::from(Span::styled(
                format!(
                    "  {hash_key} = \"USER#123\"   ·   AND / OR / NOT / BETWEEN / IN   ·   ^g for functions & full reference"
                ),
                Style::default().fg(theme.text_muted()),
            ));
        }

        let ok_line = |req: DynamoDbRequest| {
            // A Query targets a key and is cheap; a Scan reads the whole table,
            // so flag it as a warning to make the difference obvious.
            if req.is_scan() {
                Line::from(vec![
                    Span::styled("  ⚠ ".to_string(), Style::default().fg(theme.warning())),
                    Span::styled("Scan".to_string(), Style::default().fg(theme.warning())),
                    Span::styled(
                        " — reads the whole table".to_string(),
                        Style::default().fg(theme.text_muted()),
                    ),
                ])
            } else {
                Line::from(vec![
                    Span::styled("  ✓ ".to_string(), Style::default().fg(theme.success())),
                    Span::styled(req.operation_type(), Style::default().fg(theme.success())),
                ])
            }
        };

        match parse_dynamo_expression(value) {
            Ok(expr) => ok_line(DynamoDbRequest::from_expression_and_table(
                &expr,
                &meta.table_desc,
            )),
            Err(err) => {
                // A single bare token targets the partition key (the PK shortcut).
                if let Ok(operand) = parse_single_value_token(value)
                    && let (Some(hash_key), _) = extract_hash_range(&meta.table_desc)
                {
                    let expr = DynamoExpression::Comparison {
                        left: Operand::Path(hash_key),
                        operator: Comparator::Equal,
                        right: operand,
                    };
                    return ok_line(DynamoDbRequest::from_expression_and_table(
                        &expr,
                        &meta.table_desc,
                    ));
                }
                if parse_error_is_incomplete(&err) {
                    // The expression is unfinished, not wrong — don't cry wolf.
                    Line::from(Span::styled(
                        "  … keep typing".to_string(),
                        Style::default().fg(theme.text_muted()),
                    ))
                } else {
                    Line::from(vec![
                        Span::styled("  ✗ ".to_string(), Style::default().fg(theme.error())),
                        Span::styled(err.to_string(), Style::default().fg(theme.text_muted())),
                    ])
                }
            }
        }
    }

    fn render_completion(
        &self,
        frame: &mut Frame,
        area: Rect,
        theme: &Theme,
        completion: &Completion,
    ) {
        fill_bg(frame.buffer_mut(), area, theme.panel_bg_alt());
        let max_rows = area.height;
        let mut drawn: u16 = 0;
        let row_rect = |drawn: u16| Rect {
            x: area.x,
            y: area.y + drawn,
            width: area.width,
            height: 1,
        };

        // Sentinel row: selecting it (the default when no prefix is typed) and
        // pressing Enter runs the query instead of accepting a suggestion.
        if completion.has_sentinel && drawn < max_rows {
            let row_area = row_rect(drawn);
            let selected = completion.selected == 0;
            let style = if selected {
                fill_bg(frame.buffer_mut(), row_area, theme.accent());
                Style::default()
                    .fg(theme.panel_bg())
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(theme.text_muted())
            };
            frame.render_widget(
                Paragraph::new(Line::from(Span::styled("  ⏎ run query", style))),
                row_area,
            );
            drawn += 1;
        }

        let offset = completion.has_sentinel as usize;
        for (i, sug) in completion.items.iter().enumerate() {
            if drawn >= max_rows {
                break;
            }
            let row_area = row_rect(drawn);
            let selected = completion.selected == i + offset;
            if selected {
                fill_bg(frame.buffer_mut(), row_area, theme.accent());
            }
            let (text_style, detail_style) = if selected {
                (
                    Style::default()
                        .fg(theme.panel_bg())
                        .add_modifier(Modifier::BOLD),
                    Style::default().fg(theme.panel_bg()),
                )
            } else {
                let kind_color = match sug.kind {
                    SuggestionKind::Attribute => theme.text(),
                    SuggestionKind::Value => theme.success(),
                    SuggestionKind::Function => theme.accent(),
                    SuggestionKind::Keyword | SuggestionKind::Operator => theme.accent_alt(),
                };
                (
                    Style::default().fg(kind_color),
                    Style::default().fg(theme.text_muted()),
                )
            };
            let line = Line::from(vec![
                Span::styled(format!("  {}", sug.text), text_style),
                Span::raw("   "),
                Span::styled(sug.detail.clone(), detail_style),
            ]);
            frame.render_widget(Paragraph::new(line), row_area);
            drawn += 1;
        }
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
        if dynamate::readonly::is_enabled() {
            show_readonly_toast(&ctx);
            return;
        }
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

    fn confirm_delete_selection(&self, ctx: crate::env::WidgetCtx) {
        if dynamate::readonly::is_enabled() {
            show_readonly_toast(&ctx);
            return;
        }
        let Some(selection) = self.selection_snapshot() else {
            self.show_error(ctx.clone(), "No items selected");
            return;
        };
        let message = self.selection_summary(&selection);
        let ctx_for_delete = ctx.clone();
        let popup = Box::new(ConfirmPopup::new_with_action(
            "Delete selection",
            message,
            "Delete",
            "cancel",
            ConfirmAction::new(
                KeyCode::Char('d'),
                KeyModifiers::CONTROL,
                "^d",
                "delete",
                "Delete selection",
            ),
            move || {
                ctx_for_delete.emit_self(DeleteSelectionRequest {
                    selection: selection.clone(),
                });
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

    fn show_export_popup(&self, mode: ExportKind, ctx: crate::env::WidgetCtx) {
        if matches!(mode, ExportKind::Item) && self.selected_item().is_err() {
            self.show_error(ctx.clone(), "No item selected");
            return;
        }
        if matches!(mode, ExportKind::Selection) && !self.selection_active() {
            self.show_error(ctx.clone(), "No items selected");
            return;
        }
        let path = self.export_path(mode);
        let option_label = matches!(mode, ExportKind::Results)
            .then_some(Cow::Borrowed("Fetch all results before exporting"));
        let ctx_for_confirm = ctx.clone();
        let popup = Box::new(ExportPopup::new(
            path,
            option_label,
            false,
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
            duration: Duration::from_hours(1),
            action: None,
        });
    }

    fn start_export(
        &self,
        mode: ExportKind,
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
            ExportKind::Item => {
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
            ExportKind::Selection => {
                self.start_selection_export(path, ctx);
            }
            ExportKind::Results => {
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
                let cancel = Arc::new(AtomicBool::new(false));
                let request = BatchActionStreamRequest {
                    scope: BatchActionScope::Results { filter },
                    start_key,
                    active_query,
                    cached_meta: self.table_meta.borrow().clone(),
                    client: self.client.clone(),
                    table_name: self.table_name.clone(),
                    cancel: Some(cancel.clone()),
                };
                self.spawn_stream_export(mode, path, items, request, cancel, ctx);
            }
        }
    }

    fn spawn_stream_export(
        &self,
        mode: ExportKind,
        path: PathBuf,
        items: Vec<HashMap<String, AttributeValue>>,
        request: BatchActionStreamRequest,
        cancel: Arc<AtomicBool>,
        ctx: crate::env::WidgetCtx,
    ) {
        let initial_count = items.len();
        let export_id = self.next_export_id();
        {
            let mut state = self.state.borrow_mut();
            state.is_prefetching = true;
            state.export_id = Some(export_id);
            state.export_cancel = Some(cancel);
        }
        self.show_export_progress_toast(ctx.clone(), initial_count);
        let ctx_for_export = ctx.clone();
        tokio::spawn(async move {
            let result = export_batch_to_path(
                path.clone(),
                items,
                Some(request),
                ctx_for_export.clone(),
                export_id,
            )
            .await
            .map(|count| ExportOutcome { mode, path, count });
            ctx_for_export.emit_self(ExportEvent { result });
        });
    }

    fn spawn_export_task<F>(
        &self,
        mode: ExportKind,
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

    fn start_selection_export(&self, path: PathBuf, ctx: crate::env::WidgetCtx) {
        let Some(selection) = self.selection_snapshot() else {
            self.show_error(ctx.clone(), "No items selected");
            return;
        };
        let table_desc = match self.table_desc() {
            Ok(table_desc) => table_desc,
            Err(err) => {
                self.show_error(ctx.clone(), &err);
                return;
            }
        };
        let items = self.selected_loaded_items(&selection, &table_desc);
        let start_key = {
            let state = self.state.borrow();
            match &selection {
                SelectionSnapshot::Query { .. } => state.last_evaluated_key.clone(),
                SelectionSnapshot::Explicit(_) => None,
            }
        };
        let Some(start_key) = start_key else {
            self.spawn_export_task(ExportKind::Selection, path, ctx, move |path| {
                export_results_to_path(&items, &path)
            });
            return;
        };
        let cancel = Arc::new(AtomicBool::new(false));
        let request = BatchActionStreamRequest {
            scope: BatchActionScope::Selection {
                selection,
                table_desc: Box::new(table_desc),
            },
            start_key,
            active_query: self.state.borrow().active_query.clone(),
            cached_meta: None,
            client: self.client.clone(),
            table_name: self.table_name.clone(),
            cancel: Some(cancel.clone()),
        };
        self.spawn_stream_export(ExportKind::Selection, path, items, request, cancel, ctx);
    }

    fn delete_selection(&self, selection: SelectionSnapshot, ctx: crate::env::WidgetCtx) {
        if dynamate::readonly::is_enabled() {
            show_readonly_toast(&ctx);
            return;
        }
        self.set_loading_state(LoadingState::Loading);
        ctx.invalidate();
        let table_desc = match self.table_desc() {
            Ok(table_desc) => table_desc,
            Err(err) => {
                self.set_loading_state(LoadingState::Loaded);
                self.show_error(ctx.clone(), &err);
                ctx.invalidate();
                return;
            }
        };
        let active_query = self.state.borrow().active_query.clone();
        let start_key = {
            let state = self.state.borrow();
            match &selection {
                SelectionSnapshot::Query { .. } => state.last_evaluated_key.clone(),
                SelectionSnapshot::Explicit(_) => None,
            }
        };
        let loaded_keys = self.selected_loaded_keys(&selection, &table_desc);
        let client = self.client.clone();
        let table_name = self.table_name.clone();
        tokio::spawn(async move {
            let request = DeleteSelectionJob {
                selection,
                loaded_keys,
                table_desc,
                start_key,
                active_query,
                client,
                table_name,
            };
            let result = delete_selection_full(request).await;
            ctx.emit_self(DeleteSelectionEvent { result });
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

    fn export_path(&self, mode: ExportKind) -> PathBuf {
        let base = export_base_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        match mode {
            ExportKind::Item => {
                let item = self.selected_item().ok();
                if let Some(item) = item
                    && let Some(name) = self.item_export_file_name(&item)
                {
                    return base.join(name);
                }
                base.join(export_file_name(&self.table_name, mode, timestamp))
            }
            ExportKind::Selection => base.join(export_file_name(&self.table_name, mode, timestamp)),
            ExportKind::Results => {
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
        for gsi in &table_info.global_secondary_indexes {
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
        for lsi in &table_info.local_secondary_indexes {
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
        if dynamate::readonly::is_enabled() {
            show_readonly_toast(&ctx);
            return;
        }
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
            if state.show_tree {
                state.scroll_tree_down();
                false
            } else {
                state.table_state.scroll_down_by(1);
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
            if state.show_tree {
                state.page_tree_down();
                false
            } else {
                let total = state.filtered_indices.len();
                if total == 0 {
                    if state.is_loading_more {
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

                    if state.is_loading_more {
                        false
                    } else {
                        self.should_load_more(&state)
                            || (state.last_evaluated_key.is_some() && new_offset == max_offset)
                    }
                }
            }
        };

        if should_load_more {
            self.load_more(ctx);
        }
    }

    fn scroll_up(&self) {
        let mut state = self.state.borrow_mut();
        if state.show_tree {
            state.scroll_tree_up();
        } else {
            state.table_state.scroll_up_by(1);
            state.clamp_table_offset();
        }
    }

    fn page_up(&self) {
        let mut state = self.state.borrow_mut();
        if state.show_tree {
            state.page_tree_up();
            return;
        }
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

    fn tree_next_item(&self, ctx: crate::env::WidgetCtx) {
        let should_load_more = {
            let mut state = self.state.borrow_mut();
            if !state.show_tree {
                return;
            }
            state.table_state.scroll_down_by(1);
            state.clamp_table_offset();
            state.reset_tree_scroll();
            self.should_load_more(&state)
        };

        if should_load_more {
            self.load_more(ctx);
        }
    }

    fn tree_prev_item(&self) {
        let mut state = self.state.borrow_mut();
        if !state.show_tree {
            return;
        }
        state.table_state.scroll_up_by(1);
        state.clamp_table_offset();
        state.reset_tree_scroll();
    }

    fn scroll_columns_left(&self) {
        let mut state = self.state.borrow_mut();
        if state.show_tree {
            return;
        }
        state.column_offset = state.column_offset.saturating_sub(1);
    }

    fn scroll_columns_right(&self) {
        let mut state = self.state.borrow_mut();
        if state.show_tree {
            return;
        }
        let total_columns = state.item_keys.visible().len();
        if total_columns == 0 {
            state.column_offset = 0;
            return;
        }
        state.column_offset = (state.column_offset + 1).min(total_columns.saturating_sub(1));
    }

    fn toggle_compact_columns(&self) {
        let mut state = self.state.borrow_mut();
        if state.show_tree {
            return;
        }
        state.compact_columns = !state.compact_columns;
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
            state.column_offset = 0;
            state.reset_tree_scroll();
            state.tree_line_count = 0;
            state.tree_render_capacity = 0;
            state.selection.clear();
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
            state.column_offset = 0;
            state.reset_tree_scroll();
            state.tree_line_count = 0;
            state.tree_render_capacity = 0;
            state.selection.clear();
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
                state.reset_tree_scroll();
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

        let all_keys: Vec<String> = state.item_keys.visible().to_vec();
        let visible_indices = if total == 0 {
            &[][..]
        } else {
            let start = state.table_state.offset();
            let end = start.saturating_add(max_rows).min(total);
            &state.filtered_indices[start..end]
        };

        let natural_widths: Vec<usize> = all_keys
            .iter()
            .map(|key| {
                let max_value = visible_indices
                    .iter()
                    .filter_map(|idx| state.items.get(*idx))
                    .map(|item| item.value_size(key))
                    .max()
                    .unwrap_or(0);
                let key_size = key.len() + 2;
                max(max_value, key_size)
            })
            .collect();
        let max_column_width = if state.compact_columns {
            TABLE_MAX_COLUMN_WIDTH_COMPACT
        } else {
            TABLE_MAX_COLUMN_WIDTH
        };
        // The selection gutter only exists while a selection is active, so
        // the data columns reclaim its width when nothing is selected.
        let selection_active = state.selection.is_active();
        let selection_budget = if selection_active {
            SELECTION_GUTTER_WIDTH.saturating_add(TABLE_COLUMN_SPACING as u16)
        } else {
            0
        };
        let (column_offset, fitted_widths) = fit_table_column_widths(
            &natural_widths,
            area.width.saturating_sub(selection_budget),
            state.column_offset,
            max_column_width,
        );
        state.column_offset = column_offset;
        let rendered_columns = fitted_widths.len();
        let column_end = column_offset
            .saturating_add(rendered_columns)
            .min(all_keys.len());
        let keys = &all_keys[column_offset..column_end];
        let mut widths = Vec::with_capacity(fitted_widths.len() + 1);
        let mut header_cells = Vec::with_capacity(keys.len() + 1);
        if selection_active {
            widths.push(Constraint::Length(SELECTION_GUTTER_WIDTH));
            header_cells.push(Line::from(""));
        }
        widths.extend(fitted_widths.into_iter().map(Constraint::Length));
        header_cells.extend(keys.iter().map(|key| Line::from(key.clone())));
        let header = Row::new(header_cells)
            .style(Style::new().bold().bg(theme.header_bg()).fg(theme.text()));

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
        let has_hidden_columns =
            !all_keys.is_empty() && (column_offset > 0 || column_end < all_keys.len());
        if has_hidden_columns {
            footer_suffix.push_str(&format!(
                " · cols {}-{column_end}/{}",
                column_offset + 1,
                all_keys.len()
            ));
        }
        if state.compact_columns {
            footer_suffix.push_str(" · compact");
        }
        if let Some(selection_status) = self.selection_status(state) {
            footer_suffix.push_str(&format!(" · {selection_status}"));
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
                String::new(),
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

        let selection = state.selection.snapshot();
        let row_offset = state.table_state.offset();
        let rows: Vec<Row> = visible_indices
            .iter()
            .filter_map(|idx| state.items.get(*idx))
            .enumerate()
            .map(|(row_pos, item)| {
                let selected = self.item_is_selected(item, table_desc.as_ref(), selection.as_ref());
                let mut cells: Vec<Line> = Vec::with_capacity(keys.len() + 1);
                if selection_active {
                    cells.push(if selected {
                        Line::from(Span::styled(
                            SELECTION_BAR,
                            Style::default().fg(theme.accent()),
                        ))
                    } else {
                        Line::from(" ")
                    });
                }
                cells.extend(keys.iter().map(|key| Line::from(item.value(key))));
                // Zebra striping keyed on the absolute row index so the bands
                // stay stable while scrolling. Even rows keep the block bg
                // (panel_bg_alt); odd rows get the subtle stripe.
                let row = Row::new(cells);
                if (row_offset + row_pos) % 2 == 1 {
                    row.style(Style::default().bg(theme.row_stripe()))
                } else {
                    row
                }
            })
            .collect();
        let visible_len = rows.len();
        let table = Table::new(rows, widths)
            .block(block)
            .header(header)
            .highlight_spacing(HighlightSpacing::Always)
            .highlight_symbol("❯ ")
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

        // Vertical scrollbar on the right border, shown only when the results
        // overflow the viewport. Inset by the block's top/bottom borders so the
        // track lines up with the data rows.
        if total > max_rows {
            let mut sb_state = ScrollbarState::new(total).position(state.table_state.offset());
            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None)
                .thumb_style(Style::default().fg(theme.scrollbar()))
                .track_style(Style::default().fg(theme.border()));
            let sb_area = area.inner(Margin {
                vertical: 1,
                horizontal: 0,
            });
            StatefulWidget::render(scrollbar, sb_area, frame.buffer_mut(), &mut sb_state);
        }

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
                String::new(),
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
            .map_or_else(
                || vec![Line::from("No item selected")],
                |item| tree::item_to_lines(&item.0, theme, Some(state.item_keys.sorted())),
            );
        let inner_area = block.inner(area);
        state.tree_render_capacity = inner_area.height as usize;
        state.tree_line_count = content.len();
        state.clamp_tree_offset();
        let paragraph = Paragraph::new(content)
            .block(block)
            .scroll((state.tree_scroll_offset.min(u16::MAX as usize) as u16, 0));
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
                parts.push(format!("GSI: {gsi_count}"));
            }
            if lsi_count > 0 {
                parts.push(format!("LSI: {lsi_count}"));
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
        if dynamate::readonly::is_enabled() {
            show_readonly_toast(&ctx);
            return;
        }
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
            EditorFormat::Plain => match json::to_json_string(&item) {
                Ok(value) => Ok((value, EditorFormat::Plain, None)),
                Err(json::JsonConversionError::UnsupportedType { attribute_type }) => {
                    json::to_dynamodb_json_string(&item)
                        .map(|value| (value, EditorFormat::DynamoDb, Some(attribute_type)))
                }
                Err(err) => Err(err),
            },
            EditorFormat::DynamoDb => json::to_dynamodb_json_string(&item)
                .map(|value| (value, EditorFormat::DynamoDb, None)),
        };
        let (initial, actual_format, fallback_attribute_type) = match initial {
            Ok(value) => value,
            Err(err) => {
                let message = err.to_string();
                self.set_loading_state(LoadingState::Error(message.clone()));
                self.show_error(ctx.clone(), &message);
                ctx.invalidate();
                return;
            }
        };
        if let Some(attribute_type) = fallback_attribute_type {
            ctx.show_toast(Toast {
                message: format!(
                    "Opened as DynamoDB JSON because the item contains {attribute_type}"
                ),
                kind: ToastKind::Info,
                duration: Duration::from_secs(3),
                action: None,
            });
        }

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

        let updated = match actual_format {
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
        if dynamate::readonly::is_enabled() {
            show_readonly_toast(&ctx);
            return;
        }
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
        if dynamate::readonly::is_enabled() {
            show_readonly_toast(&ctx);
            return;
        }
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

fn show_readonly_toast(ctx: &crate::env::WidgetCtx) {
    ctx.show_toast(Toast {
        message: dynamate::readonly::REJECT_MESSAGE.to_string(),
        kind: ToastKind::Warning,
        duration: dynamate::readonly::TOAST_DURATION,
        action: None,
    });
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
    let table_desc = if let Some(meta) = cached_meta {
        meta.table_desc
    } else {
        let meta = fetch_table_meta(client, table_name).await?;
        let desc = meta.table_desc.clone();
        ctx.emit_self(TableMetaEvent { meta });
        desc
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
            let Ok(value) = parse_single_value_token(query) else {
                return Err(parse_error_text);
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
        std::string::ToString::to_string,
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
        std::string::ToString::to_string,
    )
    .await;
    match output {
        Ok(out) => out.time_to_live_description().and_then(|desc| {
            let enabled = matches!(
                desc.time_to_live_status(),
                Some(TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling)
            );
            if enabled {
                desc.attribute_name().map(std::string::ToString::to_string)
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

pub(super) fn extract_hash_range(table: &TableDescription) -> (Option<String>, Option<String>) {
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

const BATCH_ACTION_CANCELED: &str = "Batch action canceled";

enum BatchActionScope {
    Results {
        filter: Option<String>,
    },
    Selection {
        selection: SelectionSnapshot,
        table_desc: Box<TableDescription>,
    },
}

impl BatchActionScope {
    fn table_desc(&self) -> Option<&TableDescription> {
        match self {
            Self::Results { .. } => None,
            Self::Selection { table_desc, .. } => Some(table_desc.as_ref()),
        }
    }

    fn collect_page(
        &self,
        items: &[HashMap<String, AttributeValue>],
    ) -> Result<Vec<HashMap<String, AttributeValue>>, String> {
        match self {
            Self::Results { filter } => Ok(items
                .iter()
                .filter(|item| {
                    filter
                        .as_deref()
                        .is_none_or(|needle| item_matches_filter(item, needle))
                })
                .cloned()
                .collect()),
            Self::Selection {
                selection,
                table_desc,
            } => {
                let mut selected = Vec::new();
                for item in items {
                    let item_key = ItemKey::from_item(item, table_desc.as_ref())?;
                    if selection.is_selected(&item_key) {
                        selected.push(item.clone());
                    }
                }
                Ok(selected)
            }
        }
    }
}

struct BatchActionStreamRequest {
    scope: BatchActionScope,
    start_key: HashMap<String, AttributeValue>,
    active_query: ActiveQuery,
    cached_meta: Option<TableMeta>,
    client: aws_sdk_dynamodb::Client,
    table_name: String,
    cancel: Option<Arc<AtomicBool>>,
}

struct DeleteSelectionJob {
    selection: SelectionSnapshot,
    loaded_keys: Vec<ItemKey>,
    table_desc: TableDescription,
    start_key: Option<HashMap<String, AttributeValue>>,
    active_query: ActiveQuery,
    client: aws_sdk_dynamodb::Client,
    table_name: String,
}

fn request_for_active_query(
    active_query: &ActiveQuery,
    table_desc: &TableDescription,
) -> Result<DynamoDbRequest, String> {
    match active_query {
        ActiveQuery::Text(query) => {
            let query = query.trim();
            if query.is_empty() {
                Ok(DynamoDbRequest::Scan(ScanBuilder::new()))
            } else {
                let expr = parse_query_expression(query, table_desc)?;
                Ok(DynamoDbRequest::from_expression_and_table(
                    &expr, table_desc,
                ))
            }
        }
        ActiveQuery::Index(target) => Ok(request_for_index_target(target)),
    }
}

fn batch_action_was_canceled(cancel: Option<&Arc<AtomicBool>>) -> bool {
    cancel.is_some_and(|flag| flag.load(Ordering::Relaxed))
}

async fn request_for_batch_action(
    active_query: &ActiveQuery,
    table_desc: Option<&TableDescription>,
    cached_meta: Option<TableMeta>,
    client: aws_sdk_dynamodb::Client,
    table_name: String,
) -> Result<DynamoDbRequest, String> {
    if let Some(table_desc) = table_desc {
        return request_for_active_query(active_query, table_desc);
    }

    let request = match active_query {
        ActiveQuery::Text(query) => {
            let query = query.trim();
            if query.is_empty() {
                return Ok(DynamoDbRequest::Scan(ScanBuilder::new()));
            }
            let table_desc = match cached_meta {
                Some(meta) => meta.table_desc,
                None => {
                    fetch_table_meta(client.clone(), table_name.clone())
                        .await?
                        .table_desc
                }
            };
            let expr = parse_query_expression(query, &table_desc)?;
            DynamoDbRequest::from_expression_and_table(&expr, &table_desc)
        }
        ActiveQuery::Index(target) => request_for_index_target(target),
    };
    Ok(request)
}

fn batch_action_stream(
    request: BatchActionStreamRequest,
) -> ReceiverStream<Result<Vec<HashMap<String, AttributeValue>>, String>> {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        if let Err(err) = stream_batch_action_pages(request, tx.clone()).await {
            let _ = tx.send(Err(err)).await;
        }
    });
    ReceiverStream::new(rx)
}

async fn stream_batch_action_pages(
    request: BatchActionStreamRequest,
    tx: mpsc::Sender<Result<Vec<HashMap<String, AttributeValue>>, String>>,
) -> Result<(), String> {
    let BatchActionStreamRequest {
        scope,
        start_key,
        active_query,
        cached_meta,
        client,
        table_name,
        cancel,
    } = request;

    if batch_action_was_canceled(cancel.as_ref()) {
        return Err(BATCH_ACTION_CANCELED.to_string());
    }

    let request = request_for_batch_action(
        &active_query,
        scope.table_desc(),
        cached_meta,
        client.clone(),
        table_name.clone(),
    )
    .await?;
    let mut next_key = Some(start_key);
    while let Some(start_key) = next_key {
        if batch_action_was_canceled(cancel.as_ref()) {
            return Err(BATCH_ACTION_CANCELED.to_string());
        }
        let output = execute_page(&client, &table_name, &request, Some(start_key), None)
            .await
            .map_err(|err| err.to_string())?;
        let items = scope.collect_page(output.items())?;
        if !items.is_empty() && tx.send(Ok(items)).await.is_err() {
            return Ok(());
        }
        next_key = output.last_evaluated_key().cloned();
    }

    if batch_action_was_canceled(cancel.as_ref()) {
        return Err(BATCH_ACTION_CANCELED.to_string());
    }
    Ok(())
}

async fn export_batch_to_path(
    path: PathBuf,
    items: Vec<HashMap<String, AttributeValue>>,
    stream_request: Option<BatchActionStreamRequest>,
    ctx: crate::env::WidgetCtx,
    export_id: u64,
) -> Result<usize, String> {
    let cancel = stream_request
        .as_ref()
        .and_then(|request| request.cancel.clone());
    if batch_action_was_canceled(cancel.as_ref()) {
        return Err("Export canceled".to_string());
    }

    let mut writer = StreamedJsonArrayWriter::create(&path)?;
    writer.write_items(&items)?;
    let mut count = items.len();
    if let Some(request) = stream_request {
        let mut stream = batch_action_stream(request);
        while let Some(batch) = stream.next().await {
            let items = batch.map_err(|err| {
                if err == BATCH_ACTION_CANCELED {
                    "Export canceled".to_string()
                } else {
                    err
                }
            })?;
            writer.write_items(&items)?;
            count = count.saturating_add(items.len());
            ctx.emit_self(ExportProgressEvent { export_id, count });
        }
    }
    if batch_action_was_canceled(cancel.as_ref()) {
        return Err("Export canceled".to_string());
    }
    writer.finish()
}

async fn delete_selection_full(request: DeleteSelectionJob) -> Result<usize, String> {
    let DeleteSelectionJob {
        selection,
        loaded_keys,
        table_desc,
        start_key,
        active_query,
        client,
        table_name,
    } = request;

    let keys = match &selection {
        SelectionSnapshot::Explicit(keys) => keys.iter().cloned().collect::<Vec<_>>(),
        SelectionSnapshot::Query { .. } => loaded_keys,
    };

    let mut deleted = batch_delete_keys(&client, &table_name, &keys).await?;
    if let Some(start_key) = start_key {
        let request = BatchActionStreamRequest {
            scope: BatchActionScope::Selection {
                selection,
                table_desc: Box::new(table_desc.clone()),
            },
            start_key,
            active_query,
            cached_meta: None,
            client: client.clone(),
            table_name: table_name.clone(),
            cancel: None,
        };
        let mut stream = batch_action_stream(request);
        while let Some(batch) = stream.next().await {
            let items = batch?;
            let mut keys = Vec::with_capacity(items.len());
            for item in &items {
                keys.push(ItemKey::from_item(item, &table_desc)?);
            }
            deleted = deleted.saturating_add(batch_delete_keys(&client, &table_name, &keys).await?);
        }
    }

    Ok(deleted)
}

async fn batch_delete_keys(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
    keys: &[ItemKey],
) -> Result<usize, String> {
    let mut deleted = 0usize;
    for chunk in keys.chunks(25) {
        let mut write_requests = Vec::with_capacity(chunk.len());
        for key in chunk {
            let delete_request = DeleteRequest::builder()
                .set_key(Some(key.to_key_map()))
                .build()
                .map_err(|err| err.to_string())?;
            write_requests.push(
                WriteRequest::builder()
                    .delete_request(delete_request)
                    .build(),
            );
        }

        let mut request_items = HashMap::new();
        request_items.insert(table_name.to_string(), write_requests);
        let mut pending = request_items;
        loop {
            let pending_count = pending.get(table_name).map_or(0, std::vec::Vec::len);
            let span = tracing::trace_span!(
                "BatchWriteItem",
                table = %table_name,
                items = pending_count
            );
            let result = send_dynamo_request(
                span,
                || {
                    client
                        .batch_write_item()
                        .set_request_items(Some(pending.clone()))
                        .send()
                },
                format_sdk_error,
            )
            .await;
            let output = result.map_err(|err| format_sdk_error(&err))?;
            let unprocessed = output.unprocessed_items().cloned().unwrap_or_default();
            if unprocessed.is_empty() {
                break;
            }
            pending = unprocessed;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        deleted = deleted.saturating_add(chunk.len());
    }
    Ok(deleted)
}

struct StreamedJsonArrayWriter {
    path: PathBuf,
    temp_path: PathBuf,
    writer: Option<BufWriter<File>>,
    count: usize,
}

impl StreamedJsonArrayWriter {
    fn create(path: &Path) -> Result<Self, String> {
        ensure_export_parent(path)?;
        let temp_path = export_temp_path(path);
        let file = File::create(&temp_path).map_err(|err| err.to_string())?;
        Ok(Self {
            path: path.to_path_buf(),
            temp_path,
            writer: Some(BufWriter::new(file)),
            count: 0,
        })
    }

    fn writer(&mut self) -> Result<&mut BufWriter<File>, String> {
        self.writer
            .as_mut()
            .ok_or_else(|| "Export writer is closed".to_string())
    }

    fn write_items(&mut self, items: &[HashMap<String, AttributeValue>]) -> Result<(), String> {
        for item in items {
            if self.count == 0 {
                self.writer()?
                    .write_all(b"[\n")
                    .map_err(|err| err.to_string())?;
            } else {
                self.writer()?
                    .write_all(b",\n")
                    .map_err(|err| err.to_string())?;
            }
            let value = json::to_json(item)
                .map_err(|err| format!("Failed to convert item {}: {err}", self.count + 1))?;
            write_indented_json_value(self.writer()?, &value)?;
            self.count += 1;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<usize, String> {
        let count = self.count;
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| "Export writer is closed".to_string())?;
        if count == 0 {
            writer.write_all(b"[]").map_err(|err| err.to_string())?;
        } else {
            writer.write_all(b"\n]").map_err(|err| err.to_string())?;
        }
        writer.flush().map_err(|err| err.to_string())?;
        drop(writer);
        #[cfg(windows)]
        if self.path.exists() {
            fs::remove_file(&self.path).map_err(|err| err.to_string())?;
        }
        fs::rename(&self.temp_path, &self.path).map_err(|err| err.to_string())?;
        Ok(count)
    }
}

impl Drop for StreamedJsonArrayWriter {
    fn drop(&mut self) {
        self.writer.take();
        if !self.temp_path.as_os_str().is_empty() {
            let _ = fs::remove_file(&self.temp_path);
        }
    }
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

fn write_indented_json_value<W>(writer: &mut W, value: &serde_json::Value) -> Result<(), String>
where
    W: Write,
{
    let payload = serde_json::to_string_pretty(value).map_err(|err| err.to_string())?;
    for (idx, line) in payload.lines().enumerate() {
        if idx > 0 {
            writer.write_all(b"\n").map_err(|err| err.to_string())?;
        }
        writer.write_all(b"  ").map_err(|err| err.to_string())?;
        writer
            .write_all(line.as_bytes())
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}

fn ensure_export_parent(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Failed to create export directory: {err}"))?;
    }
    Ok(())
}

fn export_temp_path(path: &Path) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id();
    let file_name = path.file_name().map_or_else(
        || "dynamate-export.json".to_string(),
        |name| name.to_string_lossy().into_owned(),
    );
    path.with_file_name(format!(".{file_name}.{pid}.{timestamp}.tmp"))
}

fn write_json_to_path(path: &Path, value: &serde_json::Value) -> Result<(), String> {
    ensure_export_parent(path)?;
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

fn export_file_name(table_name: &str, mode: ExportKind, timestamp_ms: u128) -> String {
    let table = sanitize_export_component(table_name);
    let label = match mode {
        ExportKind::Item => "item",
        ExportKind::Selection => "selection",
        ExportKind::Results => "results",
    };
    format!("dynamate-export-{table}-{label}-{timestamp_ms}.json")
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
    match output.map(dynamate::dynamodb::Output::kind) {
        Some(Kind::Scan) => " (Scan)".to_string(),
        Some(Kind::Query) => " (Query)".to_string(),
        Some(Kind::QueryGSI(index_name)) => {
            format!(" (Query GSI: {index_name})")
        }
        Some(Kind::QueryLSI(index_name)) => {
            format!(" (Query LSI: {index_name})")
        }
        None => String::new(),
    }
}

fn query_footer_label(
    output: Option<&Output>,
    active_query: &ActiveQuery,
    table_desc: Option<&TableDescription>,
) -> Option<String> {
    let (prefix, allow_query) = match output.map(dynamate::dynamodb::Output::kind) {
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
        Some(expr) => Some(expr_format::format_query_summary(&expr)),
        None => Some(trimmed.to_string()),
    }
}

/// Unique string values observed for `attr` across the loaded items, in sorted
/// order. Used to autocomplete `#`-delimited key chunks.
fn collect_attribute_values(items: &[Item], attr: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for item in items {
        if let Some(AttributeValue::S(value)) = item.0.get(attr)
            && seen.insert(value.as_str())
        {
            out.push(value.clone());
        }
    }
    out.sort();
    out
}

/// Whether the current text is a query that can be run as-is: a parseable
/// expression, a single-token partition-key shortcut, or a blank (full scan).
fn query_is_runnable(value: &str) -> bool {
    let value = value.trim();
    value.is_empty()
        || parse_dynamo_expression(value).is_ok()
        || parse_single_value_token(value).is_ok()
}

/// Whether a parse error means the expression is merely unfinished (the user is
/// still typing) rather than genuinely malformed. Used to soften the live hint.
fn parse_error_is_incomplete(err: &ParseError) -> bool {
    match err {
        ParseError::UnexpectedEndOfInput { .. } | ParseError::UnterminatedQuote { .. } => true,
        ParseError::UnexpectedToken { token, .. } => token == "EOF",
        _ => false,
    }
}

fn fit_table_column_widths(
    natural_widths: &[usize],
    area_width: u16,
    desired_offset: usize,
    max_column_width: usize,
) -> (usize, Vec<u16>) {
    if natural_widths.is_empty() {
        return (0, Vec::new());
    }

    let offset = desired_offset.min(natural_widths.len().saturating_sub(1));
    let budget = usize::from(area_width)
        .saturating_sub(TABLE_RENDER_CHROME_WIDTH)
        .max(TABLE_MIN_COLUMN_WIDTH);
    let mut used = 0usize;
    let mut widths = Vec::new();

    for &natural in natural_widths.iter().skip(offset) {
        if widths.len() >= TABLE_MAX_RENDER_COLUMNS {
            break;
        }

        let mut width = natural.clamp(TABLE_MIN_COLUMN_WIDTH, max_column_width);
        if widths.is_empty() {
            width = width.min(budget);
            widths.push(width as u16);
            used = width;
            continue;
        }

        let additional = TABLE_COLUMN_SPACING + width;
        if used.saturating_add(additional) > budget {
            break;
        }

        widths.push(width as u16);
        used += additional;
    }

    if widths.is_empty() {
        widths.push(TABLE_MIN_COLUMN_WIDTH as u16);
    }

    (offset, widths)
}

#[cfg(test)]
mod tests {
    use super::super::selection::KeyValue;
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

    fn table_description_with_hash_and_range(hash_key: &str, range_key: &str) -> TableDescription {
        let hash = KeySchemaElement::builder()
            .attribute_name(hash_key)
            .key_type(KeyType::Hash)
            .build()
            .expect("hash key schema should be valid");
        let range = KeySchemaElement::builder()
            .attribute_name(range_key)
            .key_type(KeyType::Range)
            .build()
            .expect("range key schema should be valid");
        TableDescription::builder()
            .table_name("demo")
            .key_schema(hash)
            .key_schema(range)
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
        let name = export_file_name("My Table", ExportKind::Results, 12345);
        assert_eq!(name, "dynamate-export-my_table-results-12345.json");
    }

    #[test]
    fn export_selection_file_name_is_stable() {
        let name = export_file_name("My Table", ExportKind::Selection, 12345);
        assert_eq!(name, "dynamate-export-my_table-selection-12345.json");
    }

    #[test]
    fn streamed_json_array_writer_preserves_array_shape() {
        let path = env::temp_dir().join(format!(
            "dynamate-export-test-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let first = HashMap::from([("PK".to_string(), AttributeValue::S("USER#1".to_string()))]);
        let second = HashMap::from([("PK".to_string(), AttributeValue::S("USER#2".to_string()))]);

        let mut writer = StreamedJsonArrayWriter::create(&path).expect("writer should be created");
        writer
            .write_items(&[first, second])
            .expect("items should be written");
        let count = writer.finish().expect("writer should finish");

        let payload = fs::read_to_string(&path).expect("export file should exist");
        let value: serde_json::Value =
            serde_json::from_str(&payload).expect("export payload should be json");

        assert_eq!(count, 2);
        assert_eq!(value.as_array().map(Vec::len), Some(2));

        let _ = fs::remove_file(path);
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

    #[test]
    fn item_key_round_trips_to_dynamodb_key_map() {
        let table_desc = table_description_with_hash_and_range("PK", "SK");
        let mut item = HashMap::new();
        item.insert("PK".to_string(), AttributeValue::S("USER#1".to_string()));
        item.insert("SK".to_string(), AttributeValue::N("42".to_string()));
        item.insert("name".to_string(), AttributeValue::S("Ada".to_string()));

        let item_key = ItemKey::from_item(&item, &table_desc).expect("item key should parse");
        let key_map = item_key.to_key_map();

        assert_eq!(
            key_map.get("PK"),
            Some(&AttributeValue::S("USER#1".to_string()))
        );
        assert_eq!(
            key_map.get("SK"),
            Some(&AttributeValue::N("42".to_string()))
        );
        assert_eq!(key_map.len(), 2);
    }

    #[test]
    fn query_all_selection_respects_exclusions() {
        let selected = ItemKey {
            hash_key: "PK".to_string(),
            hash_value: KeyValue::String("USER#1".to_string()),
            range: None,
        };
        let excluded = ItemKey {
            hash_key: "PK".to_string(),
            hash_value: KeyValue::String("USER#2".to_string()),
            range: None,
        };
        let selection = SelectionSnapshot::Query {
            excluded: HashSet::from([excluded.clone()]),
        };

        assert!(selection.is_selected(&selected));
        assert!(!selection.is_selected(&excluded));
    }

    fn item_key(value: &str) -> ItemKey {
        ItemKey {
            hash_key: "PK".to_string(),
            hash_value: KeyValue::String(value.to_string()),
            range: None,
        }
    }

    #[test]
    fn invert_loaded_from_none_selects_all_loaded() {
        let loaded = [item_key("A"), item_key("B")];
        let mut selection = SelectionMode::None;
        selection.invert_loaded(loaded.clone());
        match selection {
            SelectionMode::Explicit(keys) => {
                assert_eq!(keys, loaded.into_iter().collect());
            }
            other => panic!("expected Explicit, got {other:?}"),
        }
    }

    #[test]
    fn invert_loaded_toggles_explicit_membership() {
        let (a, b) = (item_key("A"), item_key("B"));
        let mut selection = SelectionMode::Explicit(HashSet::from([a.clone()]));
        // A was selected (drops out), B was not (gets added).
        selection.invert_loaded([a.clone(), b.clone()]);
        match selection {
            SelectionMode::Explicit(keys) => {
                assert_eq!(keys, HashSet::from([b]));
            }
            other => panic!("expected Explicit, got {other:?}"),
        }
    }

    #[test]
    fn invert_loaded_collapses_to_none_when_empty() {
        let a = item_key("A");
        let mut selection = SelectionMode::Explicit(HashSet::from([a.clone()]));
        selection.invert_loaded([a]);
        assert!(matches!(selection, SelectionMode::None));
    }

    #[test]
    fn invert_loaded_toggles_query_exclusions() {
        let (a, b) = (item_key("A"), item_key("B"));
        // A already excluded; inverting flips A back in and excludes B.
        let mut selection = SelectionMode::Query {
            excluded: HashSet::from([a.clone()]),
        };
        selection.invert_loaded([a, b.clone()]);
        match selection {
            SelectionMode::Query { excluded } => {
                assert_eq!(excluded, HashSet::from([b]));
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn fit_table_column_widths_caps_rendered_columns() {
        let widths = vec![3; 64];
        let (offset, fitted) = fit_table_column_widths(&widths, 400, 0, TABLE_MAX_COLUMN_WIDTH);
        assert_eq!(offset, 0);
        assert_eq!(fitted.len(), TABLE_MAX_RENDER_COLUMNS);
    }

    #[test]
    fn fit_table_column_widths_respects_width_budget() {
        let widths = vec![20, 20, 20];
        let (_, fitted) = fit_table_column_widths(&widths, 40, 0, TABLE_MAX_COLUMN_WIDTH);
        assert_eq!(fitted, vec![20]);
    }

    #[test]
    fn fit_table_column_widths_keeps_first_column_when_area_is_tiny() {
        let widths = vec![20, 5];
        let (_, fitted) = fit_table_column_widths(&widths, 4, 0, TABLE_MAX_COLUMN_WIDTH);
        assert_eq!(fitted, vec![TABLE_MIN_COLUMN_WIDTH as u16]);
    }

    #[test]
    fn fit_table_column_widths_clamps_maximum_column_width() {
        let widths = vec![usize::MAX];
        let (_, fitted) = fit_table_column_widths(&widths, 200, 0, TABLE_MAX_COLUMN_WIDTH);
        assert_eq!(fitted, vec![TABLE_MAX_COLUMN_WIDTH as u16]);
    }

    #[test]
    fn fit_table_column_widths_uses_requested_offset() {
        let widths = vec![8, 8, 8];
        let (offset, fitted) = fit_table_column_widths(&widths, 40, 1, TABLE_MAX_COLUMN_WIDTH);
        assert_eq!(offset, 1);
        assert_eq!(fitted, vec![8, 8]);
    }

    #[test]
    fn fit_table_column_widths_clamps_offset_to_last_column() {
        let widths = vec![8, 8, 8];
        let (offset, fitted) = fit_table_column_widths(&widths, 40, 99, TABLE_MAX_COLUMN_WIDTH);
        assert_eq!(offset, 2);
        assert_eq!(fitted, vec![8]);
    }

    #[test]
    fn fit_table_column_widths_compact_mode_reduces_column_width() {
        let widths = vec![80];
        let (_, fitted) = fit_table_column_widths(&widths, 200, 0, TABLE_MAX_COLUMN_WIDTH_COMPACT);
        assert_eq!(fitted, vec![TABLE_MAX_COLUMN_WIDTH_COMPACT as u16]);
    }

    #[test]
    fn clamp_tree_offset_limits_scroll_to_last_visible_page() {
        let mut state = QueryState {
            tree_scroll_offset: 99,
            tree_render_capacity: 4,
            tree_line_count: 10,
            ..QueryState::default()
        };

        state.clamp_tree_offset();

        assert_eq!(state.tree_scroll_offset, 6);
    }

    #[test]
    fn page_tree_scroll_uses_visible_height() {
        let mut state = QueryState {
            tree_scroll_offset: 0,
            tree_render_capacity: 3,
            tree_line_count: 10,
            ..QueryState::default()
        };

        state.page_tree_down();
        assert_eq!(state.tree_scroll_offset, 3);

        state.page_tree_down();
        assert_eq!(state.tree_scroll_offset, 6);

        state.page_tree_up();
        assert_eq!(state.tree_scroll_offset, 3);
    }
}
