use std::{borrow::Cow, cell::RefCell, collections::HashMap, time::Duration};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::RequestId;
use aws_sdk_dynamodb::types::{
    AttributeValue, DeleteRequest, KeySchemaElement, KeyType, TableDescription, WriteRequest,
};
use crossterm::event::{Event, KeyCode, KeyModifiers};
use humansize::{format_size, BINARY};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Layout, Rect},
    prelude::Widget,
    style::Style,
    text::{Line, Span, Text},
    widgets::{Block, Cell, HighlightSpacing, Paragraph, Row, StatefulWidget, Table, TableState},
};
use unicode_width::UnicodeWidthStr;

use dynamate::dynamodb::send_dynamo_request;

use crate::{
    env::{Toast, ToastKind},
    help,
    util::pad,
    widgets::{
        QueryWidget,
        WidgetInner,
        confirm::{ConfirmAction, ConfirmPopup},
        create_table::{CreateTablePopup, TableCreatedEvent},
        error::ErrorPopup,
        theme::Theme,
    },
};

pub struct TablePickerWidget {
    inner: WidgetInner,
    client: Client,
    state: RefCell<TablePickerState>,
}

#[derive(Debug, Clone)]
struct TableMeta {
    status: String,
    item_count: Option<i64>,
    size_bytes: Option<i64>,
    gsi_count: usize,
    lsi_count: usize,
}

impl TableMeta {
    fn placeholder() -> Self {
        Self {
            status: "unknown".to_string(),
            item_count: None,
            size_bytes: None,
            gsi_count: 0,
            lsi_count: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct TableEntry {
    name: String,
    meta: TableMeta,
}

impl TableEntry {
    fn new(name: String, meta: TableMeta) -> Self {
        Self { name, meta }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum LoadingState {
    #[default]
    Idle,
    Loading,
    Loaded,
    Busy(String),
    Error(String),
}

#[derive(Debug, Clone, Copy)]
enum TableAction {
    Delete,
    Purge,
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
    tables: Vec<TableEntry>,
    filtered_indices: Vec<usize>,
    table_state: TableState,
    filter: FilterInput,
    last_render_capacity: usize,
}

struct TableListPayload {
    tables: Vec<TableEntry>,
    warnings: Vec<String>,
}

struct TableListEvent {
    result: Result<TableListPayload, String>,
}

struct DeleteTableRequest {
    table_name: String,
}

struct DeleteTableEvent {
    table_name: String,
    result: Result<(), String>,
}

struct PurgeTableRequest {
    table_name: String,
}

struct PurgeTableEvent {
    result: Result<usize, String>,
}

impl TablePickerState {
    fn apply_filter(&mut self) {
        let filter = self.filter.value.trim().to_lowercase();
        let current = self
            .table_state
            .selected()
            .and_then(|idx| self.filtered_indices.get(idx).copied());

        if filter.is_empty() {
            self.filtered_indices = (0..self.tables.len()).collect();
        } else {
            self.filtered_indices = self
                .tables
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.name.to_lowercase().contains(&filter))
                .map(|(idx, _)| idx)
                .collect();
        }

        if self.filtered_indices.is_empty() {
            self.table_state.select(None);
            *self.table_state.offset_mut() = 0;
            return;
        }

        if let Some(current) = current
            && let Some(index) = self
                .filtered_indices
                .iter()
                .position(|idx| *idx == current)
        {
            self.table_state.select(Some(index));
            self.clamp_offset();
            return;
        }

        self.table_state.select(Some(0));
        self.clamp_offset();
    }

    fn selected_table_name(&self) -> Option<&str> {
        self.selected_table().map(|entry| entry.name.as_str())
    }

    fn selected_table(&self) -> Option<&TableEntry> {
        self.table_state
            .selected()
            .and_then(|idx| self.filtered_indices.get(idx).copied())
            .and_then(|idx| self.tables.get(idx))
    }

    fn clamp_offset(&mut self) {
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
            keys: Cow::Borrowed("j/k/↑/↓/PgUp/PgDn"),
            short: Cow::Borrowed("move"),
            long: Cow::Borrowed("Move selection"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed(""),
            short: Cow::Borrowed(""),
            long: Cow::Borrowed(""),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^n")),
                short: Some(Cow::Borrowed("new")),
                long: Some(Cow::Borrowed("Create table")),
            }),
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed(""),
            short: Cow::Borrowed(""),
            long: Cow::Borrowed(""),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^r")),
                short: Some(Cow::Borrowed("refresh")),
                long: Some(Cow::Borrowed("Refresh tables")),
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
                long: Some(Cow::Borrowed("Delete table")),
            }),
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed(""),
            short: Cow::Borrowed(""),
            long: Cow::Borrowed(""),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^p")),
                short: Some(Cow::Borrowed("purge")),
                long: Some(Cow::Borrowed("Purge table")),
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
            keys: Cow::Borrowed("⏎"),
            short: Cow::Borrowed("select"),
            long: Cow::Borrowed("Open table"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("j/k/↑/↓/PgUp/PgDn"),
            short: Cow::Borrowed("move"),
            long: Cow::Borrowed("Move selection"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed(""),
            short: Cow::Borrowed(""),
            long: Cow::Borrowed(""),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^n")),
                short: Some(Cow::Borrowed("new")),
                long: Some(Cow::Borrowed("Create table")),
            }),
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed(""),
            short: Cow::Borrowed(""),
            long: Cow::Borrowed(""),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^r")),
                short: Some(Cow::Borrowed("refresh")),
                long: Some(Cow::Borrowed("Refresh tables")),
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
                long: Some(Cow::Borrowed("Delete table")),
            }),
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed(""),
            short: Cow::Borrowed(""),
            long: Cow::Borrowed(""),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^p")),
                short: Some(Cow::Borrowed("purge")),
                long: Some(Cow::Borrowed("Purge table")),
            }),
            shift: None,
            alt: None,
        },
    ];

    pub fn new(client: Client, parent: crate::env::WidgetId) -> Self {
        Self {
            inner: WidgetInner::new::<Self>(parent),
            client,
            state: RefCell::new(TablePickerState::default()),
        }
    }

    async fn fetch_tables(client: Client) -> Result<TableListPayload, String> {
        let mut table_names = Vec::new();
        let mut last_evaluated_table_name = None;

        loop {
            let span = tracing::trace_span!(
                "ListTables",
                start_table = ?last_evaluated_table_name.as_deref()
            );
            let result = send_dynamo_request(
                span,
                || {
                    client
                        .list_tables()
                        .set_exclusive_start_table_name(last_evaluated_table_name)
                        .send()
                },
                |err| err.to_string(),
            )
            .await;
            let output = result.map_err(|err| err.to_string())?;
            table_names.extend(output.table_names().iter().cloned());

            if output.last_evaluated_table_name().is_none() {
                break;
            }
            last_evaluated_table_name = output.last_evaluated_table_name().map(|s| s.to_string());
        }

        table_names.sort();
        let mut tables = Vec::with_capacity(table_names.len());
        let mut warnings = Vec::new();
        for name in table_names {
            match fetch_table_meta(&client, &name).await {
                Ok(meta) => tables.push(TableEntry::new(name, meta)),
                Err(err) => {
                    warnings.push(format!("{name}: {err}"));
                    tables.push(TableEntry::new(name, TableMeta::placeholder()));
                }
            }
        }

        Ok(TableListPayload { tables, warnings })
    }

    fn select_next(&self) {
        let mut state = self.state.borrow_mut();
        let len = state.filtered_indices.len();
        if len == 0 {
            return;
        }
        let next = match state.table_state.selected() {
            Some(index) => (index + 1).min(len - 1),
            None => 0,
        };
        state.table_state.select(Some(next));
        state.clamp_offset();
    }

    fn select_previous(&self) {
        let mut state = self.state.borrow_mut();
        let len = state.filtered_indices.len();
        if len == 0 {
            return;
        }
        let next = match state.table_state.selected() {
            Some(index) => index.saturating_sub(1),
            None => 0,
        };
        state.table_state.select(Some(next));
        state.clamp_offset();
    }

    fn page_down(&self) {
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
        let max_offset = total.saturating_sub(page);
        let new_offset = offset.saturating_add(page).min(max_offset);
        let mut new_selected = new_offset.saturating_add(rel);
        if new_selected >= total {
            new_selected = total.saturating_sub(1);
        }
        *state.table_state.offset_mut() = new_offset;
        state.table_state.select(Some(new_selected));
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

    fn handle_selection(&self, ctx: crate::env::WidgetCtx) -> bool {
        let selected = {
            self.state
                .borrow()
                .selected_table_name()
                .map(str::to_string)
        };
        if let Some(table_name) = selected {
            let widget = Box::new(QueryWidget::new(
                self.client.clone(),
                &table_name,
                self.inner.id(),
            ));
            ctx.push_widget(widget);
            return true;
        }
        false
    }

    fn reload_tables(&self, ctx: crate::env::WidgetCtx) {
        {
            let mut state = self.state.borrow_mut();
            state.loading_state = LoadingState::Loading;
        }
        ctx.invalidate();
        let client = self.client.clone();
        let ctx_clone = ctx.clone();
        tokio::spawn(async move {
            let result = Self::fetch_tables(client).await;
            ctx_clone.emit_self(TableListEvent { result });
        });
    }

    fn show_error(&self, ctx: crate::env::WidgetCtx, message: &str) {
        let is_empty = self.state.borrow().tables.is_empty();
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

    fn confirm_table_action(&self, ctx: crate::env::WidgetCtx, action: TableAction) {
        let selected = {
            self.state
                .borrow()
                .selected_table()
                .cloned()
        };
        let Some(entry) = selected else {
            self.show_error(ctx, "No table selected");
            return;
        };

        let mut lines = vec![format!("Table={}", entry.name)];
        if let Some(count) = entry.meta.item_count {
            lines.push(format!("Items=~{count}"));
        }
        let message = lines.join("\n");

        let (title, confirm_label, confirm_key) = match action {
            TableAction::Delete => (
                "Delete table",
                "Delete",
                ConfirmAction::new(
                    KeyCode::Char('d'),
                    KeyModifiers::CONTROL,
                    "^d",
                    "delete",
                    "Delete table",
                ),
            ),
            TableAction::Purge => (
                "Purge table",
                "Purge",
                ConfirmAction::new(
                    KeyCode::Char('p'),
                    KeyModifiers::CONTROL,
                    "^p",
                    "purge",
                    "Purge table",
                ),
            ),
        };

        let table_name = entry.name.clone();
        let ctx_for_action = ctx.clone();
        let popup = Box::new(ConfirmPopup::new_with_action(
            title,
            message,
            confirm_label,
            "cancel",
            confirm_key,
            move || match action {
                TableAction::Delete => {
                    ctx_for_action.emit_self(DeleteTableRequest {
                        table_name: table_name.clone(),
                    });
                }
                TableAction::Purge => {
                    ctx_for_action.emit_self(PurgeTableRequest {
                        table_name: table_name.clone(),
                    });
                }
            },
            self.inner.id(),
        ));
        ctx.set_popup(popup);
    }

    fn delete_table(&self, table_name: String, ctx: crate::env::WidgetCtx) {
        {
            let mut state = self.state.borrow_mut();
            state.loading_state = LoadingState::Busy(format!("Deleting {table_name}..."));
        }
        ctx.invalidate();
        let client = self.client.clone();
        let ctx_clone = ctx.clone();
        tokio::spawn(async move {
            let span = tracing::trace_span!("DeleteTable", table = %table_name);
            let result = send_dynamo_request(
                span,
                || {
                    client
                        .delete_table()
                        .table_name(&table_name)
                        .send()
                },
                format_sdk_error,
            )
            .await;
            let event_result = result.map(|_| ()).map_err(|err| format_sdk_error(&err));
            ctx_clone.emit_self(DeleteTableEvent {
                table_name,
                result: event_result,
            });
        });
    }

    fn purge_table(&self, table_name: String, ctx: crate::env::WidgetCtx) {
        {
            let mut state = self.state.borrow_mut();
            state.loading_state = LoadingState::Busy(format!("Purging {table_name}..."));
        }
        ctx.invalidate();
        let client = self.client.clone();
        let ctx_clone = ctx.clone();
        tokio::spawn(async move {
            let result = purge_table_items(client, &table_name).await;
            ctx_clone.emit_self(PurgeTableEvent {
                result,
            });
        });
    }

    fn show_create_table(&self, ctx: crate::env::WidgetCtx) {
        let popup = Box::new(CreateTablePopup::new(self.client.clone(), self.inner.id()));
        ctx.set_popup(popup);
    }
}

impl crate::widgets::Widget for TablePickerWidget {
    fn inner(&self) -> &WidgetInner {
        &self.inner
    }

    fn navigation_title(&self) -> Option<String> {
        Some("tables".to_string())
    }

    fn is_loading(&self) -> bool {
        let state = self.state.borrow();
        matches!(state.loading_state, LoadingState::Loading | LoadingState::Busy(_))
    }

    fn start(&self, ctx: crate::env::WidgetCtx) {
        self.reload_tables(ctx);
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
        let filter_active = state.filter.is_active();
        let list_area = if filter_active {
            let layout = Layout::vertical([Constraint::Length(3), Constraint::Fill(1)]);
            let [filter_area, list_area] = area.layout(&layout);
            state.filter.render(frame, filter_area, theme);
            list_area
        } else {
            area
        };

        let title = if let Some(back_title) = nav.back_title.as_ref() {
            Line::from(vec![
                Span::styled(
                    format!("← {back_title} "),
                    Style::default().fg(theme.text_muted()),
                ),
                Span::styled("Tables", Style::default().fg(theme.text())),
            ])
        } else {
            Line::styled("Tables", Style::default().fg(theme.text()))
        };

        let total_tables = state.tables.len();
        let filtered_tables = state.filtered_indices.len();
        let count_label = format_table_count_label(total_tables, filtered_tables);

        let block = Block::bordered()
            .title_top(title)
            .title_bottom(Line::styled(
                pad(count_label, 2),
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
            LoadingState::Busy(message) => {
                let text = Paragraph::new(message.as_str())
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
                if state.filtered_indices.is_empty() {
                    let empty = Paragraph::new("").block(block);
                    frame.render_widget(empty, list_area);
                } else {
                    let header = Row::new(vec![
                        Cell::from("Table"),
                        Cell::from("Status"),
                        Cell::from(Text::from("Items").alignment(Alignment::Right)),
                        Cell::from(Text::from("Size").alignment(Alignment::Right)),
                        Cell::from(Text::from("Indexes").alignment(Alignment::Right)),
                    ])
                    .style(
                        Style::default()
                            .fg(theme.text_muted())
                            .add_modifier(ratatui::style::Modifier::BOLD),
                    );

                    let rows: Vec<Row> = state
                        .filtered_indices
                        .iter()
                        .filter_map(|idx| state.tables.get(*idx))
                        .map(|entry| {
                            let status_style = status_style(&entry.meta.status, theme);
                            let items = format_count(entry.meta.item_count);
                            let size = format_size_bytes(entry.meta.size_bytes);
                            let idx_label = format!("G{}/L{}", entry.meta.gsi_count, entry.meta.lsi_count);
                            Row::new(vec![
                                Cell::from(entry.name.clone()),
                                Cell::from(entry.meta.status.clone()).style(status_style),
                                Cell::from(Text::from(items).alignment(Alignment::Right)),
                                Cell::from(Text::from(size).alignment(Alignment::Right)),
                                Cell::from(Text::from(idx_label).alignment(Alignment::Right)),
                            ])
                        })
                        .collect();

                    let inner = block.inner(list_area);
                    let table = Table::new(
                        rows,
                        [
                            Constraint::Fill(1),
                            Constraint::Length(10),
                            Constraint::Length(9),
                            Constraint::Length(10),
                            Constraint::Length(8),
                        ],
                    )
                    .block(block)
                    .header(header)
                    .highlight_spacing(HighlightSpacing::Always)
                    .highlight_symbol(">> ")
                    .row_highlight_style(
                        Style::default()
                            .bg(theme.selection_bg())
                            .fg(theme.selection_fg()),
                    );

                    let visible_rows = inner.height.saturating_sub(1) as usize;
                    state.last_render_capacity = visible_rows;
                    state.clamp_offset();

                    StatefulWidget::render(table, list_area, frame.buffer_mut(), &mut state.table_state);
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

    fn on_self_event(&self, ctx: crate::env::WidgetCtx, event: &crate::env::AppEvent) {
        if let Some(list_event) = event.payload::<TableListEvent>() {
            let mut state = self.state.borrow_mut();
            match list_event.result.as_ref() {
                Ok(payload) => {
                    state.tables = payload.tables.clone();
                    state.apply_filter();
                    state.loading_state = LoadingState::Loaded;
                    if !payload.warnings.is_empty() {
                        ctx.show_toast(Toast {
                            message: format!(
                                "{} missing metadata",
                                format_table_count(payload.warnings.len())
                            ),
                            kind: ToastKind::Warning,
                            duration: Duration::from_secs(4),
                            action: None,
                        });
                    }
                    ctx.invalidate();
                }
                Err(err) => {
                    state.loading_state = LoadingState::Error(err.clone());
                    let is_empty = state.tables.is_empty();
                    drop(state);
                    if is_empty {
                        ctx.set_popup(Box::new(ErrorPopup::new(
                            "Error",
                            err.clone(),
                            self.inner.id(),
                        )));
                    } else {
                        ctx.show_toast(Toast {
                            message: err.clone(),
                            kind: ToastKind::Error,
                            duration: Duration::from_secs(4),
                            action: None,
                        });
                    }
                    ctx.invalidate();
                }
            }
            return;
        }

        if let Some(request) = event.payload::<DeleteTableRequest>() {
            self.delete_table(request.table_name.clone(), ctx);
            return;
        }

        if let Some(request) = event.payload::<PurgeTableRequest>() {
            self.purge_table(request.table_name.clone(), ctx);
            return;
        }

        if let Some(result) = event.payload::<DeleteTableEvent>() {
            match result.result.as_ref() {
                Ok(()) => {
                    ctx.show_toast(Toast {
                        message: format!("Table {} deleted", result.table_name),
                        kind: ToastKind::Info,
                        duration: Duration::from_secs(3),
                        action: None,
                    });
                    self.reload_tables(ctx);
                }
                Err(err) => {
                    let message = format!("Failed to delete table: {err}");
                    {
                        let mut state = self.state.borrow_mut();
                        state.loading_state = LoadingState::Loaded;
                    }
                    self.show_error(ctx.clone(), &message);
                    ctx.invalidate();
                }
            }
            return;
        }

        if let Some(result) = event.payload::<PurgeTableEvent>() {
            match result.result.as_ref() {
                Ok(count) => {
                    ctx.show_toast(Toast {
                        message: format!("Purged {count} items"),
                        kind: ToastKind::Info,
                        duration: Duration::from_secs(3),
                        action: None,
                    });
                    self.reload_tables(ctx);
                }
                Err(err) => {
                    let message = format!("Failed to purge table: {err}");
                    {
                        let mut state = self.state.borrow_mut();
                        state.loading_state = LoadingState::Loaded;
                    }
                    self.show_error(ctx.clone(), &message);
                    ctx.invalidate();
                }
            }
        }
    }

    fn handle_event(&self, ctx: crate::env::WidgetCtx, event: &Event) -> bool {
        let (filter_active, filter_applied, busy) = {
            let state = self.state.borrow();
            (
                state.filter.is_active(),
                !state.filter.value.is_empty(),
                matches!(state.loading_state, LoadingState::Busy(_)),
            )
        };
        if busy {
            return true;
        }
        if let Some(key) = event.as_key_press_event()
            && key.code == KeyCode::Char('r')
            && key.modifiers.contains(KeyModifiers::CONTROL)
        {
            self.reload_tables(ctx);
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
                KeyCode::Char('/') => {
                    let mut state = self.state.borrow_mut();
                    state.filter.set_active(true);
                    return true;
                }
                KeyCode::Enter if !filter_active => {
                    return self.handle_selection(ctx);
                }
                KeyCode::Esc if !filter_active && filter_applied => {
                    let mut state = self.state.borrow_mut();
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
                KeyCode::PageDown => {
                    self.page_down();
                    return true;
                }
                KeyCode::PageUp => {
                    self.page_up();
                    return true;
                }
                KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.confirm_table_action(ctx, TableAction::Delete);
                    return true;
                }
                KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.confirm_table_action(ctx, TableAction::Purge);
                    return true;
                }
                KeyCode::Char('n') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.show_create_table(ctx);
                    return true;
                }
                _ => {}
            }
        }
        false
    }

    fn on_app_event(&self, ctx: crate::env::WidgetCtx, event: &crate::env::AppEvent) {
        if let Some(created) = event.payload::<TableCreatedEvent>() {
            tracing::debug!(table = %created.table_name, "table_created");
            self.reload_tables(ctx);
        }
    }

    fn help(&self) -> Option<&[help::Entry<'_>]> {
        let state = self.state.borrow();
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
        self.state.borrow().filter.is_active()
    }
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

async fn fetch_table_meta(client: &Client, table_name: &str) -> Result<TableMeta, String> {
    let span = tracing::trace_span!("DescribeTable", table = %table_name);
    let result = send_dynamo_request(
        span,
        || client.describe_table().table_name(table_name).send(),
        format_sdk_error,
    )
    .await;
    let output = result.map_err(|err| format_sdk_error(&err))?;
    let table_desc = output
        .table()
        .ok_or_else(|| "DescribeTable: missing table".to_string())?;

    let status = table_desc
        .table_status()
        .map(|status| status.as_str().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let item_count = table_desc.item_count();
    let size_bytes = table_desc.table_size_bytes();
    let gsi_count = table_desc.global_secondary_indexes().len();
    let lsi_count = table_desc.local_secondary_indexes().len();

    Ok(TableMeta {
        status,
        item_count,
        size_bytes,
        gsi_count,
        lsi_count,
    })
}

fn format_count(count: Option<i64>) -> String {
    count.map(|value| value.to_string()).unwrap_or_else(|| "—".to_string())
}

fn format_table_count(count: usize) -> String {
    match count {
        0 => "no tables".to_string(),
        1 => "1 table".to_string(),
        _ => format!("{count} tables"),
    }
}

fn format_table_count_label(total: usize, filtered: usize) -> String {
    if total == filtered {
        format_table_count(total)
    } else {
        let total_label = if total == 1 { "table" } else { "tables" };
        format!("{filtered} of {total} {total_label}")
    }
}

fn format_size_bytes(size: Option<i64>) -> String {
    size
        .and_then(|value| u64::try_from(value).ok())
        .map(|value| format_size(value, BINARY))
        .unwrap_or_else(|| "—".to_string())
}

fn status_style(status: &str, theme: &Theme) -> Style {
    let lower = status.to_ascii_lowercase();
    if lower == "active" {
        Style::default().fg(theme.success())
    } else if lower.contains("create") || lower.contains("update") || lower.contains("delete") {
        Style::default().fg(theme.warning())
    } else if lower.contains("error") {
        Style::default().fg(theme.error())
    } else {
        Style::default().fg(theme.text_muted())
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

async fn purge_table_items(client: Client, table_name: &str) -> Result<usize, String> {
    let span = tracing::trace_span!("DescribeTable", table = %table_name);
    let result = send_dynamo_request(
        span,
        || client.describe_table().table_name(table_name).send(),
        format_sdk_error,
    )
    .await;
    let output = result.map_err(|err| format_sdk_error(&err))?;
    let table_desc = output
        .table()
        .ok_or_else(|| "DescribeTable: missing table".to_string())?;
    let (hash_key, range_key) = extract_hash_range(table_desc);
    let Some(hash_key) = hash_key else {
        return Err("Table is missing a partition key".to_string());
    };

    let mut deleted = 0usize;
    let mut last_evaluated_key: Option<HashMap<String, AttributeValue>> = None;

    loop {
        let mut expr_names = HashMap::new();
        expr_names.insert("#hk".to_string(), hash_key.clone());
        let mut projection = "#hk".to_string();
        if let Some(range_key) = range_key.as_ref() {
            expr_names.insert("#rk".to_string(), range_key.clone());
            projection.push_str(", #rk");
        }

        let start_key_present = last_evaluated_key.is_some();
        if let Some(start_key) = last_evaluated_key.as_ref() {
            tracing::trace!(
                table=%table_name,
                start_key=?start_key,
                "Scan pagination start key"
            );
        }
        let span = tracing::trace_span!(
            "Scan",
            table = %table_name,
            projection = %projection,
            start_key = ?last_evaluated_key,
            start_key_present = start_key_present,
            limit = 25
        );
        let result = send_dynamo_request(
            span,
            || {
                client
                    .scan()
                    .table_name(table_name)
                    .projection_expression(projection)
                    .set_expression_attribute_names(Some(expr_names))
                    .limit(25)
                    .set_exclusive_start_key(last_evaluated_key.clone())
                    .send()
            },
            format_sdk_error,
        )
        .await;
        let output = result.map_err(|err| format_sdk_error(&err))?;

        let items = output.items();
        if items.is_empty() {
            if output.last_evaluated_key().is_none() {
                break;
            }
            last_evaluated_key = output.last_evaluated_key().cloned();
            continue;
        }

        let mut write_requests = Vec::with_capacity(items.len());
        for item in items {
            let hash_value = item
                .get(&hash_key)
                .ok_or_else(|| format!("Missing {hash_key} in item"))?
                .clone();
            let mut key = HashMap::new();
            key.insert(hash_key.clone(), hash_value);
            if let Some(range_key) = range_key.as_ref() {
                let range_value = item
                    .get(range_key)
                    .ok_or_else(|| format!("Missing {range_key} in item"))?
                    .clone();
                key.insert(range_key.clone(), range_value);
            }
            let delete_request = DeleteRequest::builder()
                .set_key(Some(key))
                .build()
                .map_err(|err| err.to_string())?;
            write_requests.push(WriteRequest::builder().delete_request(delete_request).build());
        }

        let batch_count = write_requests.len();
        let mut request_items = HashMap::new();
        request_items.insert(table_name.to_string(), write_requests);
        let mut pending = request_items;
        loop {
            let pending_count = pending
                .get(table_name)
                .map(|items| items.len())
                .unwrap_or(0);
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

        deleted = deleted.saturating_add(batch_count);

        last_evaluated_key = output.last_evaluated_key().cloned();
        if last_evaluated_key.is_none() {
            break;
        }

    }

    Ok(deleted)
}
