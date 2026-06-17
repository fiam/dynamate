use std::{borrow::Cow, cell::RefCell, sync::Arc, time::Duration};

use crossterm::event::{Event, KeyCode, KeyModifiers};
use humansize::{BINARY, format_size};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Layout, Rect},
    style::Style,
    text::{Line, Span, Text},
    widgets::{Block, Cell, HighlightSpacing, Paragraph, Row, StatefulWidget, Table, TableState},
};
use unicode_width::UnicodeWidthStr;

use dynamate::core::datastore::Datastore;
use dynamate::core::query::{Key, Page, QueryPlan};
use dynamate::core::schema::CollectionSchema;
use dynamate::core::value::Item;

use crate::{
    env::{Toast, ToastKind},
    help,
    util::pad,
    widgets::{
        QueryWidget, WidgetInner,
        confirm::{ConfirmAction, ConfirmPopup},
        create_table::{CreateTablePopup, TableCreatedEvent},
        error::ErrorPopup,
        filter_input::FilterInput,
        schema_popup::{SchemaNavEvent, SchemaPopup},
        theme::Theme,
    },
};

pub struct TablePickerWidget {
    inner: WidgetInner,
    db: Arc<dyn Datastore>,
    state: RefCell<TablePickerState>,
    /// Help lines, tuned to the backend's capabilities (computed once).
    help_base: Vec<help::Entry<'static>>,
    help_filter_applied: Vec<help::Entry<'static>>,
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
    schema: CollectionSchema,
}

impl TableEntry {
    fn new(name: String, schema: CollectionSchema) -> Self {
        Self {
            name,
            meta: table_meta_from(&schema),
            schema,
        }
    }

    /// An entry whose schema couldn't be described.
    fn placeholder(name: String) -> Self {
        let schema = CollectionSchema {
            name: name.clone(),
            ..CollectionSchema::default()
        };
        Self {
            name,
            meta: TableMeta::placeholder(),
            schema,
        }
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
            && let Some(index) = self.filtered_indices.iter().position(|idx| *idx == current)
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

    pub fn new(db: Arc<dyn Datastore>, parent: crate::env::WidgetId) -> Self {
        let caps = db.capabilities();
        let help_base = build_help(caps, false);
        let help_filter_applied = build_help(caps, true);
        Self {
            inner: WidgetInner::new::<Self>(parent),
            db,
            state: RefCell::new(TablePickerState::default()),
            help_base,
            help_filter_applied,
        }
    }

    async fn fetch_tables(db: Arc<dyn Datastore>) -> Result<TableListPayload, String> {
        let mut table_names = db.list_collections().await.map_err(|err| err.to_string())?;
        table_names.sort();

        let mut tables = Vec::with_capacity(table_names.len());
        let mut warnings = Vec::new();
        for name in table_names {
            match db.describe_collection(&name).await {
                Ok(schema) => tables.push(TableEntry::new(name, schema)),
                Err(err) => {
                    warnings.push(format!("{name}: {err}"));
                    tables.push(TableEntry::placeholder(name));
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
                self.db.clone(),
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
        let db = self.db.clone();
        let ctx_clone = ctx.clone();
        tokio::spawn(async move {
            let result = Self::fetch_tables(db).await;
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
        let selected = { self.state.borrow().selected_table().cloned() };
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
        if self.db.is_read_only() {
            show_readonly_toast(&ctx);
            return;
        }
        {
            let mut state = self.state.borrow_mut();
            state.loading_state = LoadingState::Busy(format!("Deleting {table_name}..."));
        }
        ctx.invalidate();
        let db = self.db.clone();
        let ctx_clone = ctx.clone();
        tokio::spawn(async move {
            let event_result = db
                .drop_collection(&table_name)
                .await
                .map_err(|err| err.to_string());
            ctx_clone.emit_self(DeleteTableEvent {
                table_name,
                result: event_result,
            });
        });
    }

    fn purge_table(&self, table_name: String, ctx: crate::env::WidgetCtx) {
        if self.db.is_read_only() {
            show_readonly_toast(&ctx);
            return;
        }
        {
            let mut state = self.state.borrow_mut();
            state.loading_state = LoadingState::Busy(format!("Purging {table_name}..."));
        }
        ctx.invalidate();
        let db = self.db.clone();
        let ctx_clone = ctx.clone();
        tokio::spawn(async move {
            let result = purge_table_items(db, &table_name).await;
            ctx_clone.emit_self(PurgeTableEvent { result });
        });
    }

    fn show_create_table(&self, ctx: crate::env::WidgetCtx) {
        if self.db.is_read_only() {
            show_readonly_toast(&ctx);
            return;
        }
        let popup = Box::new(CreateTablePopup::new(self.db.clone(), self.inner.id()));
        ctx.set_popup(popup);
    }

    /// Whether this backend offers a free-form database-level query (SQL).
    fn is_sql(&self) -> bool {
        self.db.capabilities().raw_query
    }

    /// Open the schema popup for the current selection; up/down navigates the
    /// other tables (in the current filtered order) without leaving it.
    fn show_schema_popup(&self, ctx: crate::env::WidgetCtx) {
        let (schemas, index) = {
            let state = self.state.borrow();
            let schemas: Vec<CollectionSchema> = state
                .filtered_indices
                .iter()
                .filter_map(|idx| state.tables.get(*idx))
                .map(|entry| entry.schema.clone())
                .collect();
            (schemas, state.table_state.selected().unwrap_or(0))
        };
        if schemas.is_empty() {
            return;
        }
        ctx.set_popup(Box::new(SchemaPopup::new(schemas, index, self.inner.id())));
    }

    /// Open the free-form SQL query view (a dynamic action; the view focuses its
    /// input and autocompletes table/column names).
    fn open_sql_query(&self, ctx: crate::env::WidgetCtx) {
        let widget = Box::new(QueryWidget::new_raw_sql(self.db.clone(), self.inner.id()));
        ctx.push_widget(widget);
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

impl crate::widgets::Widget for TablePickerWidget {
    fn inner(&self) -> &WidgetInner {
        &self.inner
    }

    fn navigation_title(&self) -> Option<String> {
        Some("tables".to_string())
    }

    fn is_loading(&self) -> bool {
        let state = self.state.borrow();
        matches!(
            state.loading_state,
            LoadingState::Loading | LoadingState::Busy(_)
        )
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
        let sql = self.is_sql();
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
                    let header_style = Style::default()
                        .fg(theme.text_muted())
                        .add_modifier(ratatui::style::Modifier::BOLD);
                    let header = if sql {
                        Row::new(vec![
                            Cell::from("Table"),
                            Cell::from(Text::from("Columns").alignment(Alignment::Right)),
                            Cell::from("Primary key"),
                            Cell::from(Text::from("Indexes").alignment(Alignment::Right)),
                        ])
                        .style(header_style)
                    } else {
                        Row::new(vec![
                            Cell::from("Table"),
                            Cell::from("Status"),
                            Cell::from(Text::from("Items").alignment(Alignment::Right)),
                            Cell::from(Text::from("Size").alignment(Alignment::Right)),
                            Cell::from(Text::from("Indexes").alignment(Alignment::Right)),
                        ])
                        .style(header_style)
                    };

                    let rows: Vec<Row> = state
                        .filtered_indices
                        .iter()
                        .filter_map(|idx| state.tables.get(*idx))
                        .map(|entry| {
                            if sql {
                                let columns = entry.schema.columns.len().to_string();
                                let pk = sql_primary_key(&entry.schema);
                                let indexes = entry.schema.indexes.len().to_string();
                                Row::new(vec![
                                    Cell::from(entry.name.clone()),
                                    Cell::from(Text::from(columns).alignment(Alignment::Right)),
                                    Cell::from(pk),
                                    Cell::from(Text::from(indexes).alignment(Alignment::Right)),
                                ])
                            } else {
                                let status_style = status_style(&entry.meta.status, theme);
                                let items = format_count(entry.meta.item_count);
                                let size = format_size_bytes(entry.meta.size_bytes);
                                let idx_label =
                                    format!("G{}/L{}", entry.meta.gsi_count, entry.meta.lsi_count);
                                Row::new(vec![
                                    Cell::from(entry.name.clone()),
                                    Cell::from(entry.meta.status.clone()).style(status_style),
                                    Cell::from(Text::from(items).alignment(Alignment::Right)),
                                    Cell::from(Text::from(size).alignment(Alignment::Right)),
                                    Cell::from(Text::from(idx_label).alignment(Alignment::Right)),
                                ])
                            }
                        })
                        .collect();

                    let inner = block.inner(list_area);
                    let widths: &[Constraint] = if sql {
                        &[
                            Constraint::Fill(1),
                            Constraint::Length(9),
                            Constraint::Length(24),
                            Constraint::Length(9),
                        ]
                    } else {
                        &[
                            Constraint::Fill(1),
                            Constraint::Length(10),
                            Constraint::Length(9),
                            Constraint::Length(10),
                            Constraint::Length(8),
                        ]
                    };
                    let table = Table::new(rows, widths.iter().copied())
                        .block(block)
                        .header(header)
                        .highlight_spacing(HighlightSpacing::Always)
                        .highlight_symbol("❯ ")
                        .row_highlight_style(
                            Style::default()
                                .bg(theme.selection_bg())
                                .fg(theme.selection_fg()),
                        );

                    let visible_rows = inner.height.saturating_sub(1) as usize;
                    state.last_render_capacity = visible_rows;
                    state.clamp_offset();

                    StatefulWidget::render(
                        table,
                        list_area,
                        frame.buffer_mut(),
                        &mut state.table_state,
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

    fn on_self_event(&self, ctx: crate::env::WidgetCtx, event: &crate::env::AppEvent) {
        if let Some(list_event) = event.payload::<TableListEvent>() {
            let mut state = self.state.borrow_mut();
            match list_event.result.as_ref() {
                Ok(payload) => {
                    state.tables.clone_from(&payload.tables);
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
                KeyCode::Char('q') if !filter_active && self.is_sql() => {
                    self.open_sql_query(ctx);
                    return true;
                }
                KeyCode::Tab if !filter_active => {
                    self.show_schema_popup(ctx);
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
                    if self.db.is_read_only() {
                        show_readonly_toast(&ctx);
                    } else {
                        self.confirm_table_action(ctx, TableAction::Delete);
                    }
                    return true;
                }
                KeyCode::Char('p')
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && self.db.capabilities().purge =>
                {
                    if self.db.is_read_only() {
                        show_readonly_toast(&ctx);
                    } else {
                        self.confirm_table_action(ctx, TableAction::Purge);
                    }
                    return true;
                }
                KeyCode::Char('n')
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && self.db.capabilities().create_collection =>
                {
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
            return;
        }
        // Keep the list selection in sync as the schema popup pages through tables.
        if let Some(nav) = event.payload::<SchemaNavEvent>() {
            let mut state = self.state.borrow_mut();
            if let Some(pos) = state
                .filtered_indices
                .iter()
                .position(|idx| state.tables.get(*idx).is_some_and(|e| e.name == nav.table))
            {
                state.table_state.select(Some(pos));
                state.clamp_offset();
                drop(state);
                ctx.invalidate();
            }
        }
    }

    fn help(&self) -> Option<&[help::Entry<'_>]> {
        let state = self.state.borrow();
        if state.filter.is_active() {
            Some(Self::HELP_FILTER_EDIT)
        } else if !state.filter.value.is_empty() {
            Some(&self.help_filter_applied)
        } else {
            Some(&self.help_base)
        }
    }

    fn suppress_global_help(&self) -> bool {
        self.state.borrow().filter.is_active()
    }
}

/// A plain (no-modifier) help entry.
fn help_entry(keys: &'static str, short: &'static str, long: &'static str) -> help::Entry<'static> {
    help::Entry {
        keys: Cow::Borrowed(keys),
        short: Cow::Borrowed(short),
        long: Cow::Borrowed(long),
        ctrl: None,
        shift: None,
        alt: None,
    }
}

/// A Ctrl-modified help entry.
fn help_ctrl(keys: &'static str, short: &'static str, long: &'static str) -> help::Entry<'static> {
    help::Entry {
        keys: Cow::Borrowed(""),
        short: Cow::Borrowed(""),
        long: Cow::Borrowed(""),
        ctrl: Some(help::Variant {
            keys: Some(Cow::Borrowed(keys)),
            short: Some(Cow::Borrowed(short)),
            long: Some(Cow::Borrowed(long)),
        }),
        shift: None,
        alt: None,
    }
}

/// The picker's help line, tuned to the backend's capabilities. `applied` is the
/// variant shown when a filter is already in effect.
fn build_help(
    caps: &dynamate::core::capabilities::Capabilities,
    applied: bool,
) -> Vec<help::Entry<'static>> {
    let mut entries = Vec::new();
    if applied {
        entries.push(help_entry("/", "filter", "Edit filter"));
        entries.push(help_entry("esc", "clear filter", "Clear filter"));
    } else {
        entries.push(help_entry("/", "filter", "Filter tables"));
    }
    entries.push(help_entry("⏎", "select", "Open table"));
    entries.push(help_entry("j/k/↑/↓/PgUp/PgDn", "move", "Move selection"));
    entries.push(help_entry("⇥", "schema", "View schema"));
    if caps.raw_query {
        entries.push(help_entry("q", "query", "Run SQL query"));
    }
    if caps.create_collection {
        entries.push(help_ctrl("^n", "new", "Create table"));
    }
    entries.push(help_ctrl("^r", "refresh", "Refresh tables"));
    entries.push(help_ctrl("^d", "delete", "Delete table"));
    if caps.purge {
        entries.push(help_ctrl("^p", "purge", "Purge table"));
    }
    entries
}

fn table_meta_from(schema: &dynamate::core::schema::CollectionSchema) -> TableMeta {
    TableMeta {
        status: schema
            .status
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        item_count: schema.item_count,
        size_bytes: schema.size_bytes,
        gsi_count: schema.global_secondary_index_count(),
        lsi_count: schema.local_secondary_index_count(),
    }
}

fn format_count(count: Option<i64>) -> String {
    count.map_or_else(|| "—".to_string(), |value| value.to_string())
}

/// The comma-joined primary-key column names of a SQL table (or `—` if none).
fn sql_primary_key(schema: &CollectionSchema) -> String {
    if schema.key.fields.is_empty() {
        "—".to_string()
    } else {
        schema
            .key
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
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
    size.and_then(|value| u64::try_from(value).ok())
        .map_or_else(|| "—".to_string(), |value| format_size(value, BINARY))
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

async fn purge_table_items(db: Arc<dyn Datastore>, table_name: &str) -> Result<usize, String> {
    let schema = db
        .describe_collection(table_name)
        .await
        .map_err(|err| err.to_string())?;
    let key_fields: Vec<String> = schema.key.fields.iter().map(|f| f.name.clone()).collect();
    if key_fields.is_empty() {
        return Err("Table is missing a partition key".to_string());
    }

    let mut deleted = 0usize;
    let mut cursor = None;
    loop {
        let page = db
            .query(
                table_name,
                &QueryPlan::default(),
                Page {
                    cursor,
                    limit: Some(25),
                },
            )
            .await
            .map_err(|err| err.to_string())?;

        let mut keys = Vec::with_capacity(page.items.len());
        for item in &page.items {
            let mut key_item = Item::new();
            for field in &key_fields {
                let value = item
                    .get(field)
                    .ok_or_else(|| format!("Missing {field} in item"))?
                    .clone();
                key_item.insert(field.clone(), value);
            }
            keys.push(Key(key_item));
        }

        if !keys.is_empty() {
            let outcome = db
                .batch_delete(table_name, keys)
                .await
                .map_err(|err| err.to_string())?;
            deleted = deleted.saturating_add(outcome.deleted as usize);
        }

        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }

    Ok(deleted)
}
