use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    cmp::{max, min},
    collections::{HashMap, HashSet},
    env, fs,
    process::Command,
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
use throbber_widgets_tui::{Throbber, ThrobberState};
use throbber_widgets_tui::symbols::throbber::BRAILLE_ONE;

use super::{input, item_keys, keys_widget, tree};
use keys_widget::KeysWidget;

use crate::{
    env::{Toast, ToastKind},
    help,
    util::pad,
    widgets::{WidgetInner, error::ErrorPopup, theme::Theme},
};
use chrono::{DateTime, Utc};
use dynamate::dynamodb::json;
use dynamate::dynamodb::size::estimate_item_size_bytes;
use dynamate::dynamodb::{SecondaryIndex, TableInfo};
use dynamate::{
    dynamodb::{DynamoDbRequest, Kind, Output, ScanBuilder, execute_page},
    expr::parse_dynamo_expression,
};
use humansize::{BINARY, format_size};
use unicode_width::UnicodeWidthStr;

pub struct QueryWidget {
    inner: WidgetInner,
    client: aws_sdk_dynamodb::Client,
    table_name: String,
    state: RefCell<QueryState>,
    table_meta: RefCell<Option<TableMeta>>,
    meta_started: Cell<bool>,
    request_seq: Cell<u64>,
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
    is_loading_more: bool,
    show_tree: bool,
    reopen_tree: Option<usize>,
    scanned_total: i64,
    matched_total: i64,
    throbber: ThrobberState,
    last_render_capacity: usize,
    is_prefetching: bool,
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
    query: String,
    reopen_tree: Option<usize>,
    result: Result<(), String>,
}

struct KeyVisibilityEvent {
    name: String,
    hidden: bool,
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
                .filter(|(_, item)| item_matches_filter(item, &needle))
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

    fn start(&self, ctx: crate::env::WidgetCtx) {
        self.start_query(None, ctx);
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let mut state = self.state.borrow_mut();

        if state.show_tree {
            self.render_tree(frame, area, theme, &mut state);
        } else {
            let filter_active = state.filter.is_active();
            let layout = if filter_active {
                Layout::vertical([
                    Constraint::Length(3),
                    Constraint::Length(3),
                    Constraint::Fill(1),
                ])
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

    fn handle_event(&self, ctx: crate::env::WidgetCtx, event: &Event) -> bool {
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
                KeyCode::Tab | KeyCode::BackTab => {
                    self.state.borrow_mut().input.toggle_active()
                }
                KeyCode::Esc if input_is_active => {
                    self.state.borrow_mut().input.toggle_active()
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
                    } else if matches!(state.loading_state, LoadingState::Loading)
                        || state.is_prefetching
                    {
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
                KeyCode::Char('j') | KeyCode::Down => self.scroll_down(ctx.clone()),
                KeyCode::Char('k') | KeyCode::Up => self.scroll_up(),
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
                                ctx_for_keys.emit_self(KeyVisibilityEvent {
                                    name,
                                    hidden: true,
                                });
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
                KeyCode::Char('t') => {
                    let mut state = self.state.borrow_mut();
                    state.show_tree = !state.show_tree;
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
        if (matches!(state.loading_state, LoadingState::Loading) || state.is_prefetching)
            && !state.input.is_active()
            && !state.filter.is_active()
        {
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
        self.state.borrow().filter.is_active()
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

        if let Some(put_event) = event.payload::<PutItemEvent>() {
            match put_event.result.as_ref() {
                Ok(()) => {
                    self.start_query_with_reopen(
                        Some(&put_event.query),
                        ctx.clone(),
                        put_event.reopen_tree,
                    );
                }
                Err(err) => {
                    self.set_loading_state(LoadingState::Error(err.clone()));
                    self.show_error(ctx.clone(), err);
                    ctx.invalidate();
                }
            }
        }
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
    const HELP_LOADING: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("cancel"),
            long: Cow::Borrowed("Cancel request"),
            ctrl: None,
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
    pub fn new(
        client: aws_sdk_dynamodb::Client,
        table_name: &str,
        parent: crate::env::WidgetId,
    ) -> Self {
        let page_size = env_u64("DYNAMATE_PAGE_SIZE")
            .and_then(|value| i32::try_from(value).ok())
            .filter(|value| *value > 0)
            .unwrap_or(100);
        Self {
            inner: WidgetInner::new::<Self>(parent),
            client,
            table_name: table_name.to_string(),
            state: RefCell::new(QueryState::default()),
            table_meta: RefCell::new(None),
            meta_started: Cell::new(false),
            request_seq: Cell::new(0),
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
            });
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

    fn scroll_up(&self) {
        let mut state = self.state.borrow_mut();
        state.table_state.scroll_up_by(1);
        state.clamp_table_offset();
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
        let (query, start_key) = {
            let mut state = self.state.borrow_mut();
            if state.is_loading_more {
                return;
            }
            let Some(start_key) = state.last_evaluated_key.clone() else {
                return;
            };
            state.is_loading_more = true;
            (state.last_query.clone(), start_key)
        };

        let request_id = self.active_request_id();
        self.start_query_page(query, Some(start_key), true, ctx, request_id);
    }

    fn start_query(&self, query: Option<&str>, ctx: crate::env::WidgetCtx) {
        self.start_query_with_reopen(query, ctx, None);
    }

    fn start_query_with_reopen(
        &self,
        query: Option<&str>,
        ctx: crate::env::WidgetCtx,
        reopen_tree: Option<usize>,
    ) {
        self.maybe_start_meta_fetch(ctx.clone());
        let query = query.unwrap_or("").to_string();
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
            state.last_query = query.clone();
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

    fn bump_request_id(&self) -> u64 {
        let next = self.request_seq.get() + 1;
        self.request_seq.set(next);
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
    ) {
        tracing::trace!(
            table = %self.table_name,
            area_width = area.width,
            area_height = area.height,
            items = state.items.len(),
            filtered = state.filtered_indices.len(),
            offset = state.table_state.offset(),
            loading = ?state.loading_state,
            "render_table"
        );
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
        let header = Row::new(keys.iter().map(|key| Line::from(key.clone())))
        .style(Style::new().bold());

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
                        approx_total
                            .as_ref()
                            .map(|value| format!(" · {value}"))
                            .unwrap_or_default()
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
                        state.scanned_total,
                        state.matched_total,
                        more_marker,
                        approx_total
                            .as_ref()
                            .map(|value| format!(" · {value}"))
                            .unwrap_or_default()
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
        if matches!(state.loading_state, LoadingState::Loading) || state.is_prefetching {
            render_loading_throbber(frame, area, theme, &mut state.throbber);
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
    ) {
        tracing::trace!(
            table = %self.table_name,
            area_width = area.width,
            area_height = area.height,
            items = state.items.len(),
            filtered = state.filtered_indices.len(),
            loading = ?state.loading_state,
            "render_tree"
        );
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
        if matches!(state.loading_state, LoadingState::Loading) || state.is_prefetching {
            render_loading_throbber(frame, area, theme, &mut state.throbber);
        }
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
            format!(" Item: {} ", parts.join(", "))
        }
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

    fn edit_selected(&self, format: EditorFormat, ctx: crate::env::WidgetCtx) {
        let (item, query, reopen_tree) = {
            let state = self.state.borrow();
            let selected = state.table_state.selected();
            let item_index = selected.and_then(|index| state.filtered_indices.get(index).copied());
            let item = item_index
                .and_then(|index| state.items.get(index))
                .map(|item| item.0.clone());
            let reopen_tree = if state.show_tree { item_index } else { None };
            (item, state.last_query.clone(), reopen_tree)
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

        self.put_item(updated, query, ctx, reopen_tree);
    }

    fn create_item(&self, format: EditorFormat, ctx: crate::env::WidgetCtx) {
        let query = self.state.borrow().last_query.clone();
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

        self.put_item(updated, query, ctx, None);
    }

    fn open_editor(&self, initial: &str, ctx: crate::env::WidgetCtx) -> Result<String, String> {
        let editor = env::var("EDITOR").map_err(|_| "EDITOR is not set".to_string())?;
        let temp_path = self.temp_path();
        fs::write(&temp_path, initial).map_err(|err| err.to_string())?;

        let keyboard_support =
            crossterm::terminal::supports_keyboard_enhancement().unwrap_or(false);
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
        query: String,
        ctx: crate::env::WidgetCtx,
        reopen_tree: Option<usize>,
    ) {
        self.set_loading_state(LoadingState::Loading);
        ctx.invalidate();
        let client = self.client.clone();
        let table_name = self.table_name.clone();
        tokio::spawn(async move {
            let result = client
                .put_item()
                .table_name(&table_name)
                .set_item(Some(item))
                .send()
                .await;
            let event_result = result.map(|_| ()).map_err(|err| err.to_string());
            ctx.emit_self(PutItemEvent {
                query,
                reopen_tree,
                result: event_result,
            });
        });
    }
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
    let expr = parse_dynamo_expression(query).map_err(|e| e.to_string())?;
    Ok(DynamoDbRequest::from_expression_and_table(
        &expr,
        &table_desc,
    ))
}

async fn fetch_table_description(
    client: aws_sdk_dynamodb::Client,
    table_name: String,
) -> Result<TableDescription, String> {
    let out = client
        .describe_table()
        .table_name(&table_name)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let table = out
        .table()
        .ok_or_else(|| "DescribeTable: missing table".to_string())?;
    Ok(table.clone())
}

async fn fetch_ttl_attribute(
    client: aws_sdk_dynamodb::Client,
    table_name: String,
) -> Option<String> {
    let output = client
        .describe_time_to_live()
        .table_name(&table_name)
        .send()
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
    env::var(name).ok().and_then(|value| value.parse::<u64>().ok())
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
fn render_loading_throbber(
    frame: &mut Frame,
    area: Rect,
    theme: &Theme,
    state: &mut ThrobberState,
) {
    if area.width < 4 {
        return;
    }
    state.calc_next();
    let rect = Rect::new(area.x + area.width.saturating_sub(4), area.y, 3, 1);
    let throbber = Throbber::default()
        .throbber_set(BRAILLE_ONE)
        .style(Style::default().fg(theme.text_muted()))
        .throbber_style(Style::default().fg(theme.warning()));
    frame.render_stateful_widget(throbber, rect, state);
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
