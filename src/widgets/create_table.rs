use std::{borrow::Cow, cell::RefCell, time::Duration};

use aws_sdk_dynamodb::Client;
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Layout, Margin, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Clear, Paragraph},
};

use dynamate::dynamodb::{
    AttributeType, CreateTableSpec, GsiSpec, IndexProjection, KeySpec, LsiSpec, create_table,
};

use crate::{
    env::{Toast, ToastKind},
    help,
    util::fill_bg,
    widgets::{Popup, WidgetInner, theme::Theme},
};

#[derive(Debug, Clone)]
pub struct TableCreatedEvent {
    pub table_name: String,
}

pub struct CreateTablePopup {
    inner: WidgetInner,
    client: Client,
    state: RefCell<CreateTableState>,
    help_entries: Vec<help::Entry<'static>>,
}

impl CreateTablePopup {
    pub fn new(client: Client, parent: crate::env::WidgetId) -> Self {
        let help_entries = vec![
            help::Entry {
                keys: Cow::Borrowed("tab/shift+tab"),
                short: Cow::Borrowed("move"),
                long: Cow::Borrowed("Next/previous field"),
                ctrl: None,
                shift: None,
                alt: None,
            },
            help::Entry {
                keys: Cow::Borrowed("pgup/pgdn"),
                short: Cow::Borrowed("scroll"),
                long: Cow::Borrowed("Scroll form"),
                ctrl: None,
                shift: None,
                alt: None,
            },
            help::Entry {
                keys: Cow::Borrowed("space"),
                short: Cow::Borrowed("toggle"),
                long: Cow::Borrowed("Change type/projection"),
                ctrl: None,
                shift: None,
                alt: None,
            },
            help::Entry {
                keys: Cow::Borrowed("^g"),
                short: Cow::Borrowed("add gsi"),
                long: Cow::Borrowed("Add GSI"),
                ctrl: None,
                shift: None,
                alt: None,
            },
            help::Entry {
                keys: Cow::Borrowed("^l"),
                short: Cow::Borrowed("add lsi"),
                long: Cow::Borrowed("Add LSI"),
                ctrl: None,
                shift: None,
                alt: None,
            },
            help::Entry {
                keys: Cow::Borrowed("^x"),
                short: Cow::Borrowed("remove index"),
                long: Cow::Borrowed("Remove index"),
                ctrl: None,
                shift: None,
                alt: None,
            },
            help::Entry {
                keys: Cow::Borrowed("^enter"),
                short: Cow::Borrowed("create"),
                long: Cow::Borrowed("Create table"),
                ctrl: None,
                shift: None,
                alt: None,
            },
            help::Entry {
                keys: Cow::Borrowed("esc"),
                short: Cow::Borrowed("cancel"),
                long: Cow::Borrowed("Cancel"),
                ctrl: None,
                shift: None,
                alt: None,
            },
        ];
        let mut state = CreateTableState::default();
        state.sync_active();
        Self {
            inner: WidgetInner::new::<Self>(parent),
            client,
            state: RefCell::new(state),
            help_entries,
        }
    }

    fn submit(&self, ctx: crate::env::WidgetCtx) {
        let spec_result = {
            let state = self.state.borrow();
            build_spec(&state)
        };
        let spec = match spec_result {
            Ok(spec) => spec,
            Err(err) => {
                let mut state = self.state.borrow_mut();
                state.error = Some(err);
                ctx.invalidate();
                return;
            }
        };

        let table_name = spec.table_name.clone();
        {
            let mut state = self.state.borrow_mut();
            state.status = CreateStatus::Submitting;
            state.error = None;
        }
        ctx.invalidate();

        let client = self.client.clone();
        let ctx_clone = ctx.clone();
        tokio::spawn(async move {
            let result = create_table(client, spec).await;
            ctx_clone.emit_self(CreateTableResult { table_name, result });
        });
    }

    fn add_gsi(&self) {
        let mut state = self.state.borrow_mut();
        state.add_gsi();
        state.error = None;
    }

    fn add_lsi(&self) {
        let mut state = self.state.borrow_mut();
        state.add_lsi();
        state.error = None;
    }
}

impl crate::widgets::Widget for CreateTablePopup {
    fn inner(&self) -> &WidgetInner {
        &self.inner
    }

    fn help(&self) -> Option<&[help::Entry<'_>]> {
        Some(self.help_entries.as_slice())
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        fill_bg(frame.buffer_mut(), area, theme.panel_bg());
        let title = Line::from(vec![
            Span::raw(" "),
            Span::styled(
                "Create table",
                Style::default()
                    .fg(theme.accent())
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" "),
        ])
        .centered();
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .title(title)
            .border_style(Style::default().fg(theme.border()))
            .style(Style::default().bg(theme.panel_bg()).fg(theme.text()));
        frame.render_widget(block.clone(), area);
        let inner = block.inner(area).inner(Margin::new(1, 1));

        let footer_height = match inner.height {
            0 => 0,
            1 => 1,
            _ => 2,
        };
        let content_height = inner.height.saturating_sub(footer_height);
        let content_area = Rect::new(inner.x, inner.y, inner.width, content_height);

        let (rows, scroll_offset, scroll_info) = {
            let mut state = self.state.borrow_mut();
            state.last_viewport_height = content_height;
            let rows = build_rows(&state);
            if state.user_scrolled {
                state.clamp_scroll(&rows, content_height);
            } else {
                state.ensure_visible(&rows, content_height);
            }
            let scroll_info = scroll_indicator(&rows, content_height, state.scroll_offset);
            (rows, state.scroll_offset, scroll_info)
        };

        let state = self.state.borrow();
        let mut y_offset: u16 = 0;
        let view_bottom = scroll_offset.saturating_add(content_height);
        for row in &rows {
            let row_start = y_offset;
            let row_end = row_start.saturating_add(row.height);
            y_offset = row_end;

            if content_height == 0 {
                continue;
            }
            if row_start < scroll_offset || row_end > view_bottom {
                continue;
            }

            let row_y = content_area.y + row_start.saturating_sub(scroll_offset);
            let row_area = Rect::new(content_area.x, row_y, content_area.width, row.height);
            render_row(frame, row_area, row.kind, &state, theme);
        }

        if footer_height >= 2 {
            let actions_area = Rect::new(inner.x, inner.y + content_height, inner.width, 1);
            render_actions(
                frame,
                actions_area,
                theme,
                matches!(state.active_field, FieldId::Actions),
                state.selected_action,
            );
            let status_area = Rect::new(inner.x, inner.y + content_height + 1, inner.width, 1);
            render_status(frame, status_area, &state, theme, scroll_info.clone());
        } else if footer_height == 1 {
            let footer_area = Rect::new(inner.x, inner.y + content_height, inner.width, 1);
            if state.error.is_some() || matches!(state.status, CreateStatus::Submitting) {
                render_status(frame, footer_area, &state, theme, scroll_info.clone());
            } else {
                render_actions(
                    frame,
                    footer_area,
                    theme,
                    matches!(state.active_field, FieldId::Actions),
                    state.selected_action,
                );
                if scroll_info.is_some() {
                    render_status(frame, footer_area, &state, theme, scroll_info.clone());
                }
            }
        }

        if let Some(picker) = state.remove_picker.as_ref() {
            let picker_area = remove_picker_rect(area, picker.items.len());
            render_remove_picker(frame, picker_area, picker, theme);
        }
    }

    fn handle_event(&self, ctx: crate::env::WidgetCtx, event: &Event) -> bool {
        if matches!(self.state.borrow().status, CreateStatus::Submitting) {
            return true;
        }

        let Some(key) = event.as_key_press_event() else {
            return true;
        };

        if key.code == KeyCode::Enter && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.submit(ctx);
            return true;
        }

        if self.state.borrow().remove_picker.is_some() {
            let mut state = self.state.borrow_mut();
            let action = state.handle_remove_picker(key);
            match action {
                RemovePickerAction::None => return true,
                RemovePickerAction::Close => {
                    state.remove_picker = None;
                    ctx.invalidate();
                    return true;
                }
                RemovePickerAction::Remove(focus) => {
                    let keep_actions = matches!(state.active_field, FieldId::Actions);
                    state.remove_picker = None;
                    if state.remove_index(Some(focus), keep_actions) {
                        state.error = None;
                    } else {
                        state.error = Some("Failed to remove index".to_string());
                    }
                    ctx.invalidate();
                    return true;
                }
            }
        }

        if key.code == KeyCode::Esc {
            ctx.dismiss_popup();
            ctx.invalidate();
            return true;
        }

        if matches!(key.code, KeyCode::PageUp | KeyCode::PageDown) {
            let mut state = self.state.borrow_mut();
            let rows = build_rows(&state);
            let viewport = state.last_viewport_height.max(1);
            let page = viewport.saturating_sub(1).max(1) as i16;
            let delta = if key.code == KeyCode::PageDown {
                page
            } else {
                -page
            };
            state.scroll_by(delta, &rows, viewport);
            state.user_scrolled = true;
            ctx.invalidate();
            return true;
        }

        if key.code == KeyCode::Char('g') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.add_gsi();
            ctx.invalidate();
            return true;
        }

        if key.code == KeyCode::Char('l') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.add_lsi();
            ctx.invalidate();
            return true;
        }

        if key.code == KeyCode::Char('x') && key.modifiers.contains(KeyModifiers::CONTROL) {
            let mut state = self.state.borrow_mut();
            if let Err(err) = state.open_remove_picker() {
                state.error = Some(err);
            }
            ctx.invalidate();
            return true;
        }

        if matches!(self.state.borrow().active_field, FieldId::Actions) {
            match key.code {
                KeyCode::Left => {
                    let mut state = self.state.borrow_mut();
                    state.selected_action = state.selected_action.prev();
                    ctx.invalidate();
                    return true;
                }
                KeyCode::Right => {
                    let mut state = self.state.borrow_mut();
                    state.selected_action = state.selected_action.next();
                    ctx.invalidate();
                    return true;
                }
                KeyCode::Tab => {
                    let mut state = self.state.borrow_mut();
                    if state.selected_action == ActionId::Create {
                        state.next_field();
                    } else {
                        state.selected_action = state.selected_action.next();
                    }
                    ctx.invalidate();
                    return true;
                }
                KeyCode::BackTab => {
                    let mut state = self.state.borrow_mut();
                    if state.selected_action == ActionId::AddGsi {
                        state.prev_field();
                    } else {
                        state.selected_action = state.selected_action.prev();
                    }
                    ctx.invalidate();
                    return true;
                }
                KeyCode::Enter => {
                    let state = self.state.borrow();
                    let action = state.selected_action;
                    drop(state);
                    match action {
                        ActionId::AddGsi => {
                            self.add_gsi();
                            ctx.invalidate();
                        }
                        ActionId::AddLsi => {
                            self.add_lsi();
                            ctx.invalidate();
                        }
                        ActionId::Remove => {
                            let mut state = self.state.borrow_mut();
                            if let Err(err) = state.open_remove_picker() {
                                state.error = Some(err);
                            }
                            ctx.invalidate();
                        }
                        ActionId::Create => {
                            self.submit(ctx);
                        }
                    }
                    return true;
                }
                _ => {}
            }
        }

        match key.code {
            KeyCode::Tab => {
                let mut state = self.state.borrow_mut();
                state.next_field();
                ctx.invalidate();
                return true;
            }
            KeyCode::BackTab => {
                let mut state = self.state.borrow_mut();
                state.prev_field();
                ctx.invalidate();
                return true;
            }
            KeyCode::Enter => {
                let mut state = self.state.borrow_mut();
                state.next_field();
                ctx.invalidate();
                return true;
            }
            _ => {}
        }

        let mut state = self.state.borrow_mut();
        let handled = match state.active_field {
            FieldId::TableName => state.table_name.handle_event(event),
            FieldId::HashKeyName => state.hash_key.name.handle_event(event),
            FieldId::HashKeyType => state.hash_key.key_type.handle_event(event),
            FieldId::SortKeyName => state.sort_key.name.handle_event(event),
            FieldId::SortKeyType => state.sort_key.key_type.handle_event(event),
            FieldId::GsiName(idx) => state.gsis[idx].name.handle_event(event),
            FieldId::GsiHashName(idx) => state.gsis[idx].hash_key.name.handle_event(event),
            FieldId::GsiHashType(idx) => state.gsis[idx].hash_key.key_type.handle_event(event),
            FieldId::GsiSortName(idx) => state.gsis[idx].sort_key.name.handle_event(event),
            FieldId::GsiSortType(idx) => state.gsis[idx].sort_key.key_type.handle_event(event),
            FieldId::GsiProjectionKind(idx) => state.gsis[idx].projection.kind.handle_event(event),
            FieldId::GsiProjectionAttrs(idx) => {
                state.gsis[idx].projection.include_attrs.handle_event(event)
            }
            FieldId::LsiName(idx) => state.lsis[idx].name.handle_event(event),
            FieldId::LsiSortName(idx) => state.lsis[idx].sort_key.name.handle_event(event),
            FieldId::LsiSortType(idx) => state.lsis[idx].sort_key.key_type.handle_event(event),
            FieldId::LsiProjectionKind(idx) => state.lsis[idx].projection.kind.handle_event(event),
            FieldId::LsiProjectionAttrs(idx) => {
                state.lsis[idx].projection.include_attrs.handle_event(event)
            }
            FieldId::Actions => false,
        };
        if handled {
            state.error = None;
            ctx.invalidate();
        }
        handled
    }

    fn on_self_event(&self, ctx: crate::env::WidgetCtx, event: &crate::env::AppEvent) {
        let Some(result) = event.payload::<CreateTableResult>() else {
            return;
        };
        match result.result.as_ref() {
            Ok(()) => {
                ctx.show_toast(Toast {
                    message: format!("Table {} created", result.table_name),
                    kind: ToastKind::Info,
                    duration: Duration::from_secs(3),
                    action: None,
                });
                ctx.dismiss_popup();
                ctx.broadcast_event(TableCreatedEvent {
                    table_name: result.table_name.clone(),
                });
                ctx.invalidate();
            }
            Err(err) => {
                let mut state = self.state.borrow_mut();
                state.status = CreateStatus::Idle;
                state.error = Some(err.clone());
                ctx.invalidate();
            }
        }
    }
}

impl Popup for CreateTablePopup {
    fn rect(&self, area: Rect) -> Rect {
        let width = (area.width as f32 * 0.8) as u16;
        let height = (area.height as f32 * 0.8) as u16;
        let width = width.max(72).min(area.width.saturating_sub(4));
        let height = height.max(18).min(area.height.saturating_sub(4));
        let x = area.x + (area.width - width) / 2;
        let y = area.y + (area.height - height) / 2;
        Rect {
            x,
            y,
            width,
            height,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldId {
    TableName,
    HashKeyName,
    HashKeyType,
    SortKeyName,
    SortKeyType,
    GsiName(usize),
    GsiHashName(usize),
    GsiHashType(usize),
    GsiSortName(usize),
    GsiSortType(usize),
    GsiProjectionKind(usize),
    GsiProjectionAttrs(usize),
    LsiName(usize),
    LsiSortName(usize),
    LsiSortType(usize),
    LsiProjectionKind(usize),
    LsiProjectionAttrs(usize),
    Actions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActionId {
    AddGsi,
    AddLsi,
    Remove,
    Create,
}

impl ActionId {
    fn next(self) -> Self {
        match self {
            ActionId::AddGsi => ActionId::AddLsi,
            ActionId::AddLsi => ActionId::Remove,
            ActionId::Remove => ActionId::Create,
            ActionId::Create => ActionId::AddGsi,
        }
    }

    fn prev(self) -> Self {
        match self {
            ActionId::AddGsi => ActionId::Create,
            ActionId::AddLsi => ActionId::AddGsi,
            ActionId::Remove => ActionId::AddLsi,
            ActionId::Create => ActionId::Remove,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SectionKind {
    Gsi,
    Lsi,
}

impl SectionKind {
    fn label(self) -> &'static str {
        match self {
            SectionKind::Gsi => "Global Secondary Indexes",
            SectionKind::Lsi => "Local Secondary Indexes",
        }
    }

    fn empty_label(self) -> &'static str {
        match self {
            SectionKind::Gsi | SectionKind::Lsi => "None",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RowKind {
    TableName,
    HashKey,
    SortKey,
    SectionHeader(SectionKind),
    EmptyHint(SectionKind),
    GsiLabel(usize),
    GsiName(usize),
    GsiHash(usize),
    GsiSort(usize),
    GsiProjection(usize),
    LsiLabel(usize),
    LsiName(usize),
    LsiSort(usize),
    LsiProjection(usize),
}

impl RowKind {
    fn matches_active(self, active: FieldId) -> bool {
        match self {
            RowKind::TableName => matches!(active, FieldId::TableName),
            RowKind::HashKey => matches!(active, FieldId::HashKeyName | FieldId::HashKeyType),
            RowKind::SortKey => matches!(active, FieldId::SortKeyName | FieldId::SortKeyType),
            RowKind::GsiLabel(_) | RowKind::SectionHeader(_) | RowKind::EmptyHint(_) => false,
            RowKind::GsiName(idx) => matches!(active, FieldId::GsiName(i) if i == idx),
            RowKind::GsiHash(idx) => matches!(
                active,
                FieldId::GsiHashName(i) | FieldId::GsiHashType(i) if i == idx
            ),
            RowKind::GsiSort(idx) => matches!(
                active,
                FieldId::GsiSortName(i) | FieldId::GsiSortType(i) if i == idx
            ),
            RowKind::GsiProjection(idx) => matches!(
                active,
                FieldId::GsiProjectionKind(i) | FieldId::GsiProjectionAttrs(i) if i == idx
            ),
            RowKind::LsiLabel(_) => false,
            RowKind::LsiName(idx) => matches!(active, FieldId::LsiName(i) if i == idx),
            RowKind::LsiSort(idx) => matches!(
                active,
                FieldId::LsiSortName(i) | FieldId::LsiSortType(i) if i == idx
            ),
            RowKind::LsiProjection(idx) => matches!(
                active,
                FieldId::LsiProjectionKind(i) | FieldId::LsiProjectionAttrs(i) if i == idx
            ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RowSpec {
    kind: RowKind,
    height: u16,
}

impl RowSpec {
    fn new(kind: RowKind, height: u16) -> Self {
        Self { kind, height }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreateStatus {
    Idle,
    Submitting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionKind {
    All,
    KeysOnly,
    Include,
}

impl ProjectionKind {
    fn label(self) -> &'static str {
        match self {
            ProjectionKind::All => "ALL",
            ProjectionKind::KeysOnly => "KEYS_ONLY",
            ProjectionKind::Include => "INCLUDE",
        }
    }

    fn description(self) -> &'static str {
        match self {
            ProjectionKind::All => "all attrs",
            ProjectionKind::KeysOnly => "keys only",
            ProjectionKind::Include => "include attrs",
        }
    }

    fn next(self) -> Self {
        match self {
            ProjectionKind::All => ProjectionKind::KeysOnly,
            ProjectionKind::KeysOnly => ProjectionKind::Include,
            ProjectionKind::Include => ProjectionKind::All,
        }
    }

    fn prev(self) -> Self {
        match self {
            ProjectionKind::All => ProjectionKind::Include,
            ProjectionKind::KeysOnly => ProjectionKind::All,
            ProjectionKind::Include => ProjectionKind::KeysOnly,
        }
    }
}

struct CreateTableState {
    table_name: TextInput,
    hash_key: KeyInput,
    sort_key: KeyInput,
    gsis: Vec<GsiInput>,
    lsis: Vec<LsiInput>,
    active_field: FieldId,
    status: CreateStatus,
    error: Option<String>,
    next_gsi_id: usize,
    next_lsi_id: usize,
    selected_action: ActionId,
    last_index_focus: Option<IndexFocus>,
    scroll_offset: u16,
    last_viewport_height: u16,
    user_scrolled: bool,
    remove_picker: Option<RemovePickerState>,
}

impl Default for CreateTableState {
    fn default() -> Self {
        Self {
            table_name: TextInput::new("Table name", ""),
            hash_key: KeyInput::new("Partition key name", AttributeType::String, "PK"),
            sort_key: KeyInput::new("Sort key name (optional)", AttributeType::String, "SK"),
            gsis: Vec::new(),
            lsis: Vec::new(),
            active_field: FieldId::TableName,
            status: CreateStatus::Idle,
            error: None,
            next_gsi_id: 0,
            next_lsi_id: 0,
            selected_action: ActionId::Create,
            last_index_focus: None,
            scroll_offset: 0,
            last_viewport_height: 0,
            user_scrolled: false,
            remove_picker: None,
        }
    }
}

impl CreateTableState {
    fn field_order(&self) -> Vec<FieldId> {
        let mut fields = vec![
            FieldId::TableName,
            FieldId::HashKeyName,
            FieldId::HashKeyType,
            FieldId::SortKeyName,
            FieldId::SortKeyType,
        ];
        for idx in 0..self.gsis.len() {
            fields.extend([
                FieldId::GsiName(idx),
                FieldId::GsiHashName(idx),
                FieldId::GsiHashType(idx),
                FieldId::GsiSortName(idx),
                FieldId::GsiSortType(idx),
                FieldId::GsiProjectionKind(idx),
                FieldId::GsiProjectionAttrs(idx),
            ]);
        }
        for idx in 0..self.lsis.len() {
            fields.extend([
                FieldId::LsiName(idx),
                FieldId::LsiSortName(idx),
                FieldId::LsiSortType(idx),
                FieldId::LsiProjectionKind(idx),
                FieldId::LsiProjectionAttrs(idx),
            ]);
        }
        fields.push(FieldId::Actions);
        fields
    }

    fn set_active(&mut self, field: FieldId) {
        self.active_field = field;
        self.user_scrolled = false;
        if let Some(focus) = IndexFocus::from_field(field) {
            self.last_index_focus = Some(focus);
        }
        self.sync_active();
    }

    fn ensure_visible(&mut self, rows: &[RowSpec], viewport_height: u16) {
        if viewport_height == 0 {
            self.scroll_offset = 0;
            return;
        }
        let total_height = rows_total_height(rows);
        if total_height <= viewport_height {
            self.scroll_offset = 0;
            return;
        }
        let max_offset = total_height.saturating_sub(viewport_height);
        if let Some((row_start, row_end)) = active_row_range(rows, self.active_field)
            && (row_start < self.scroll_offset || row_end > self.scroll_offset + viewport_height)
        {
            self.scroll_offset = row_start.min(max_offset);
        }
        if self.scroll_offset > max_offset {
            self.scroll_offset = max_offset;
        }
    }

    fn clamp_scroll(&mut self, rows: &[RowSpec], viewport_height: u16) {
        if viewport_height == 0 {
            self.scroll_offset = 0;
            return;
        }
        let total_height = rows_total_height(rows);
        let max_offset = total_height.saturating_sub(viewport_height);
        if self.scroll_offset > max_offset {
            self.scroll_offset = max_offset;
        }
    }

    fn scroll_by(&mut self, delta: i16, rows: &[RowSpec], viewport_height: u16) {
        if viewport_height == 0 {
            self.scroll_offset = 0;
            return;
        }
        let total_height = rows_total_height(rows);
        let max_offset = total_height.saturating_sub(viewport_height);
        let offset = self.scroll_offset as i32 + delta as i32;
        let clamped = offset.clamp(0, max_offset as i32);
        self.scroll_offset = clamped as u16;
    }

    fn open_remove_picker(&mut self) -> Result<(), String> {
        let items = build_remove_targets(self);
        if items.is_empty() {
            return Err("No indices to remove".to_string());
        }
        let selected = selected_remove_index(self, &items).unwrap_or(0);
        self.remove_picker = Some(RemovePickerState { items, selected });
        Ok(())
    }

    fn next_field(&mut self) {
        let order = self.field_order();
        let idx = order
            .iter()
            .position(|f| *f == self.active_field)
            .unwrap_or(0);
        let next = order[(idx + 1) % order.len()];
        if matches!(next, FieldId::Actions) {
            self.selected_action = ActionId::AddGsi;
        }
        self.set_active(next);
    }

    fn prev_field(&mut self) {
        let order = self.field_order();
        let idx = order
            .iter()
            .position(|f| *f == self.active_field)
            .unwrap_or(0);
        let prev = if idx == 0 {
            order[order.len() - 1]
        } else {
            order[idx - 1]
        };
        if matches!(prev, FieldId::Actions) {
            self.selected_action = ActionId::Create;
        }
        self.set_active(prev);
    }

    fn sync_active(&mut self) {
        self.table_name
            .set_active(matches!(self.active_field, FieldId::TableName));
        self.hash_key
            .name
            .set_active(matches!(self.active_field, FieldId::HashKeyName));
        self.hash_key
            .key_type
            .set_active(matches!(self.active_field, FieldId::HashKeyType));
        self.sort_key
            .name
            .set_active(matches!(self.active_field, FieldId::SortKeyName));
        self.sort_key
            .key_type
            .set_active(matches!(self.active_field, FieldId::SortKeyType));

        for (idx, gsi) in self.gsis.iter_mut().enumerate() {
            gsi.name
                .set_active(matches!(self.active_field, FieldId::GsiName(i) if i == idx));
            gsi.hash_key
                .name
                .set_active(matches!(self.active_field, FieldId::GsiHashName(i) if i == idx));
            gsi.hash_key
                .key_type
                .set_active(matches!(self.active_field, FieldId::GsiHashType(i) if i == idx));
            gsi.sort_key
                .name
                .set_active(matches!(self.active_field, FieldId::GsiSortName(i) if i == idx));
            gsi.sort_key
                .key_type
                .set_active(matches!(self.active_field, FieldId::GsiSortType(i) if i == idx));
            gsi.projection
                .kind
                .set_active(matches!(self.active_field, FieldId::GsiProjectionKind(i) if i == idx));
            gsi.projection.include_attrs.set_active(
                matches!(self.active_field, FieldId::GsiProjectionAttrs(i) if i == idx),
            );
        }

        for (idx, lsi) in self.lsis.iter_mut().enumerate() {
            lsi.name
                .set_active(matches!(self.active_field, FieldId::LsiName(i) if i == idx));
            lsi.sort_key
                .name
                .set_active(matches!(self.active_field, FieldId::LsiSortName(i) if i == idx));
            lsi.sort_key
                .key_type
                .set_active(matches!(self.active_field, FieldId::LsiSortType(i) if i == idx));
            lsi.projection
                .kind
                .set_active(matches!(self.active_field, FieldId::LsiProjectionKind(i) if i == idx));
            lsi.projection.include_attrs.set_active(
                matches!(self.active_field, FieldId::LsiProjectionAttrs(i) if i == idx),
            );
        }
    }

    fn add_gsi(&mut self) {
        if self.gsis.is_empty() {
            self.next_gsi_id = 0;
        }
        self.next_gsi_id = self.next_gsi_id.saturating_add(1);
        let gsi = GsiInput::new(self.next_gsi_id);
        self.gsis.push(gsi);
        let idx = self.gsis.len().saturating_sub(1);
        self.set_active(FieldId::GsiName(idx));
    }

    fn add_lsi(&mut self) {
        if self.lsis.is_empty() {
            self.next_lsi_id = 0;
        }
        self.next_lsi_id = self.next_lsi_id.saturating_add(1);
        let lsi = LsiInput::new(self.next_lsi_id);
        self.lsis.push(lsi);
        let idx = self.lsis.len().saturating_sub(1);
        self.set_active(FieldId::LsiName(idx));
    }

    fn remove_index(&mut self, focus: Option<IndexFocus>, keep_actions: bool) -> bool {
        let Some(focus) = focus else {
            return false;
        };
        match focus {
            IndexFocus::Gsi(idx) => {
                if idx >= self.gsis.len() {
                    return false;
                }
                self.gsis.remove(idx);
                if self.gsis.is_empty() {
                    self.last_index_focus = None;
                    self.next_gsi_id = 0;
                    if !keep_actions {
                        self.set_active(FieldId::TableName);
                    }
                } else {
                    let next = idx.min(self.gsis.len() - 1);
                    self.last_index_focus = Some(IndexFocus::Gsi(next));
                    if !keep_actions {
                        self.set_active(FieldId::GsiName(next));
                    }
                }
                true
            }
            IndexFocus::Lsi(idx) => {
                if idx >= self.lsis.len() {
                    return false;
                }
                self.lsis.remove(idx);
                if self.lsis.is_empty() {
                    self.last_index_focus = None;
                    self.next_lsi_id = 0;
                    if !keep_actions {
                        self.set_active(FieldId::TableName);
                    }
                } else {
                    let next = idx.min(self.lsis.len() - 1);
                    self.last_index_focus = Some(IndexFocus::Lsi(next));
                    if !keep_actions {
                        self.set_active(FieldId::LsiName(next));
                    }
                }
                true
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum IndexFocus {
    Gsi(usize),
    Lsi(usize),
}

impl IndexFocus {
    fn from_field(field: FieldId) -> Option<Self> {
        match field {
            FieldId::GsiName(idx)
            | FieldId::GsiHashName(idx)
            | FieldId::GsiHashType(idx)
            | FieldId::GsiSortName(idx)
            | FieldId::GsiSortType(idx)
            | FieldId::GsiProjectionKind(idx)
            | FieldId::GsiProjectionAttrs(idx) => Some(IndexFocus::Gsi(idx)),
            FieldId::LsiName(idx)
            | FieldId::LsiSortName(idx)
            | FieldId::LsiSortType(idx)
            | FieldId::LsiProjectionKind(idx)
            | FieldId::LsiProjectionAttrs(idx) => Some(IndexFocus::Lsi(idx)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct RemoveTarget {
    label: String,
    focus: IndexFocus,
}

#[derive(Debug, Clone)]
struct RemovePickerState {
    items: Vec<RemoveTarget>,
    selected: usize,
}

#[derive(Debug, Clone, Copy)]
enum RemovePickerAction {
    None,
    Close,
    Remove(IndexFocus),
}

impl CreateTableState {
    fn handle_remove_picker(&mut self, key: KeyEvent) -> RemovePickerAction {
        let Some(picker) = self.remove_picker.as_mut() else {
            return RemovePickerAction::None;
        };
        match key.code {
            KeyCode::Esc => RemovePickerAction::Close,
            KeyCode::Up | KeyCode::Char('k') => {
                if picker.selected > 0 {
                    picker.selected -= 1;
                }
                RemovePickerAction::None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if picker.selected + 1 < picker.items.len() {
                    picker.selected += 1;
                }
                RemovePickerAction::None
            }
            KeyCode::Enter => picker
                .items
                .get(picker.selected)
                .map(|item| RemovePickerAction::Remove(item.focus))
                .unwrap_or(RemovePickerAction::Close),
            _ => RemovePickerAction::None,
        }
    }
}

fn build_remove_targets(state: &CreateTableState) -> Vec<RemoveTarget> {
    let mut targets = Vec::new();
    for (idx, gsi) in state.gsis.iter().enumerate() {
        let name = gsi.name.value().trim();
        let label = if name.is_empty() {
            format!("GSI {}", idx + 1)
        } else {
            format!("GSI {} - {}", idx + 1, name)
        };
        targets.push(RemoveTarget {
            label,
            focus: IndexFocus::Gsi(idx),
        });
    }
    for (idx, lsi) in state.lsis.iter().enumerate() {
        let name = lsi.name.value().trim();
        let label = if name.is_empty() {
            format!("LSI {}", idx + 1)
        } else {
            format!("LSI {} - {}", idx + 1, name)
        };
        targets.push(RemoveTarget {
            label,
            focus: IndexFocus::Lsi(idx),
        });
    }
    targets
}

fn selected_remove_index(state: &CreateTableState, items: &[RemoveTarget]) -> Option<usize> {
    let focus = IndexFocus::from_field(state.active_field)
        .or(state.last_index_focus)
        .or_else(|| items.first().map(|_| items[0].focus));
    let focus = focus?;
    let gsis_len = state.gsis.len();
    let index = match focus {
        IndexFocus::Gsi(idx) => idx,
        IndexFocus::Lsi(idx) => gsis_len.saturating_add(idx),
    };
    if index < items.len() {
        Some(index)
    } else {
        None
    }
}

fn build_rows(state: &CreateTableState) -> Vec<RowSpec> {
    const INPUT_HEIGHT: u16 = 3;
    const LABEL_HEIGHT: u16 = 1;

    let mut rows = vec![
        RowSpec::new(RowKind::TableName, INPUT_HEIGHT),
        RowSpec::new(RowKind::HashKey, INPUT_HEIGHT),
        RowSpec::new(RowKind::SortKey, INPUT_HEIGHT),
        RowSpec::new(RowKind::SectionHeader(SectionKind::Gsi), LABEL_HEIGHT),
    ];
    if state.gsis.is_empty() {
        rows.push(RowSpec::new(
            RowKind::EmptyHint(SectionKind::Gsi),
            LABEL_HEIGHT,
        ));
    } else {
        for idx in 0..state.gsis.len() {
            rows.push(RowSpec::new(RowKind::GsiLabel(idx), LABEL_HEIGHT));
            rows.push(RowSpec::new(RowKind::GsiName(idx), INPUT_HEIGHT));
            rows.push(RowSpec::new(RowKind::GsiHash(idx), INPUT_HEIGHT));
            rows.push(RowSpec::new(RowKind::GsiSort(idx), INPUT_HEIGHT));
            rows.push(RowSpec::new(RowKind::GsiProjection(idx), INPUT_HEIGHT));
        }
    }

    rows.push(RowSpec::new(
        RowKind::SectionHeader(SectionKind::Lsi),
        LABEL_HEIGHT,
    ));
    if state.lsis.is_empty() {
        rows.push(RowSpec::new(
            RowKind::EmptyHint(SectionKind::Lsi),
            LABEL_HEIGHT,
        ));
    } else {
        for idx in 0..state.lsis.len() {
            rows.push(RowSpec::new(RowKind::LsiLabel(idx), LABEL_HEIGHT));
            rows.push(RowSpec::new(RowKind::LsiName(idx), INPUT_HEIGHT));
            rows.push(RowSpec::new(RowKind::LsiSort(idx), INPUT_HEIGHT));
            rows.push(RowSpec::new(RowKind::LsiProjection(idx), INPUT_HEIGHT));
        }
    }

    rows
}

fn rows_total_height(rows: &[RowSpec]) -> u16 {
    rows.iter()
        .fold(0, |total, row| total.saturating_add(row.height))
}

fn active_row_range(rows: &[RowSpec], active: FieldId) -> Option<(u16, u16)> {
    let mut y: u16 = 0;
    for row in rows {
        let start = y;
        let end = start.saturating_add(row.height);
        if row.kind.matches_active(active) {
            return Some((start, end));
        }
        y = end;
    }
    None
}

fn scroll_indicator(rows: &[RowSpec], viewport_height: u16, scroll_offset: u16) -> Option<String> {
    if viewport_height == 0 {
        return None;
    }
    let total_height = rows_total_height(rows);
    if total_height <= viewport_height {
        return None;
    }
    let pages = total_height.saturating_add(viewport_height.saturating_sub(1)) / viewport_height;
    let page = (scroll_offset / viewport_height)
        .saturating_add(1)
        .min(pages);
    Some(format!("Scroll {page}/{pages}"))
}

struct KeyInput {
    name: TextInput,
    key_type: TypeSelect,
}

impl KeyInput {
    fn new(label: impl Into<String>, attr_type: AttributeType, value: impl Into<String>) -> Self {
        Self {
            name: TextInput::new(label, value),
            key_type: TypeSelect::new("Type", attr_type),
        }
    }
}

struct GsiInput {
    name: TextInput,
    hash_key: KeyInput,
    sort_key: KeyInput,
    projection: ProjectionInput,
}

impl GsiInput {
    fn new(id: usize) -> Self {
        Self {
            name: TextInput::new("GSI name", format!("GSI{id}")),
            hash_key: KeyInput::new(
                "GSI partition key name",
                AttributeType::String,
                format!("GSI{id}PK"),
            ),
            sort_key: KeyInput::new(
                "GSI sort key name (optional)",
                AttributeType::String,
                format!("GSI{id}SK"),
            ),
            projection: ProjectionInput::new(),
        }
    }
}

struct LsiInput {
    name: TextInput,
    sort_key: KeyInput,
    projection: ProjectionInput,
}

impl LsiInput {
    fn new(id: usize) -> Self {
        Self {
            name: TextInput::new("LSI name", format!("LSI{id}")),
            sort_key: KeyInput::new(
                "LSI sort key name",
                AttributeType::String,
                format!("LSI{id}SK"),
            ),
            projection: ProjectionInput::new(),
        }
    }
}

struct ProjectionInput {
    kind: ProjectionSelect,
    include_attrs: TextInput,
}

impl ProjectionInput {
    fn new() -> Self {
        Self {
            kind: ProjectionSelect::new("Projection", ProjectionKind::All),
            include_attrs: TextInput::new("Include attrs (comma)", ""),
        }
    }
}

struct ProjectionSelect {
    label: String,
    value: ProjectionKind,
    active: bool,
}

impl ProjectionSelect {
    fn new(label: impl Into<String>, value: ProjectionKind) -> Self {
        Self {
            label: label.into(),
            value,
            active: false,
        }
    }

    fn set_active(&mut self, active: bool) {
        self.active = active;
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let border = if self.active {
            theme.accent()
        } else {
            theme.border()
        };
        let block = Block::bordered()
            .title(self.label.as_str())
            .style(Style::default().bg(theme.panel_bg_alt()).fg(theme.text()))
            .border_style(Style::default().fg(border));
        let value = format!("{} ({})", self.value.label(), self.value.description());
        let input = Paragraph::new(Line::from(Span::styled(
            value,
            Style::default().fg(theme.text()),
        )))
        .block(block)
        .alignment(Alignment::Center);
        frame.render_widget(input, area);
    }

    fn handle_event(&mut self, evt: &Event) -> bool {
        if !self.active {
            return false;
        }
        let Some(key) = evt.as_key_press_event() else {
            return false;
        };
        match key.code {
            KeyCode::Char(' ') | KeyCode::Right => {
                self.value = self.value.next();
                true
            }
            KeyCode::Left => {
                self.value = self.value.prev();
                true
            }
            _ => false,
        }
    }
}

struct TypeSelect {
    label: String,
    value: AttributeType,
    active: bool,
}

impl TypeSelect {
    fn new(label: impl Into<String>, value: AttributeType) -> Self {
        Self {
            label: label.into(),
            value,
            active: false,
        }
    }

    fn set_active(&mut self, active: bool) {
        self.active = active;
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let border = if self.active {
            theme.accent()
        } else {
            theme.border()
        };
        let block = Block::bordered()
            .title(self.label.as_str())
            .style(Style::default().bg(theme.panel_bg_alt()).fg(theme.text()))
            .border_style(Style::default().fg(border));
        let value = format!("{} ({})", self.value.label(), self.value.description());
        let input = Paragraph::new(Line::from(Span::styled(
            value,
            Style::default().fg(theme.text()),
        )))
        .block(block)
        .alignment(Alignment::Center);
        frame.render_widget(input, area);
    }

    fn handle_event(&mut self, evt: &Event) -> bool {
        if !self.active {
            return false;
        }
        let Some(key) = evt.as_key_press_event() else {
            return false;
        };
        match key.code {
            KeyCode::Char(' ') | KeyCode::Right => {
                self.value = match self.value {
                    AttributeType::String => AttributeType::Number,
                    AttributeType::Number => AttributeType::Binary,
                    AttributeType::Binary => AttributeType::String,
                };
                true
            }
            KeyCode::Left => {
                self.value = match self.value {
                    AttributeType::String => AttributeType::Binary,
                    AttributeType::Number => AttributeType::String,
                    AttributeType::Binary => AttributeType::Number,
                };
                true
            }
            _ => false,
        }
    }
}

struct TextInput {
    label: String,
    value: String,
    cursor: usize,
    active: bool,
}

impl TextInput {
    fn new(label: impl Into<String>, value: impl Into<String>) -> Self {
        let value = value.into();
        let cursor = value.len();
        Self {
            label: label.into(),
            value,
            cursor,
            active: false,
        }
    }

    fn value(&self) -> &str {
        &self.value
    }

    fn set_active(&mut self, active: bool) {
        self.active = active;
        if self.cursor > self.value.len() {
            self.cursor = self.value.len();
        }
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let border = if self.active {
            theme.accent()
        } else {
            theme.border()
        };
        let block = Block::bordered()
            .title(self.label.as_str())
            .style(Style::default().bg(theme.panel_bg_alt()).fg(theme.text()))
            .border_style(Style::default().fg(border));
        let value = if self.value.is_empty() {
            Span::styled("(required)", Style::default().fg(theme.text_muted()))
        } else {
            Span::styled(self.value.as_str(), Style::default().fg(theme.text()))
        };
        let input = Paragraph::new(Line::from(value)).block(block);
        frame.render_widget(input, area);
        if self.active {
            frame.set_cursor_position(ratatui::layout::Position::new(
                area.x + self.cursor as u16 + 1,
                area.y + 1,
            ));
        }
    }

    fn handle_event(&mut self, evt: &Event) -> bool {
        if !self.active {
            return false;
        }
        let Some(key) = evt.as_key_press_event() else {
            return false;
        };
        match key.code {
            KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.cursor = 0;
            }
            KeyCode::Char('e') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.cursor = self.value.len();
            }
            KeyCode::Char(c) => {
                self.value.insert(self.cursor, c);
                self.cursor += 1;
            }
            KeyCode::Backspace => {
                if self.cursor > 0 && !self.value.is_empty() {
                    self.value.remove(self.cursor - 1);
                    self.cursor -= 1;
                }
            }
            KeyCode::Delete => {
                if self.cursor < self.value.len() && !self.value.is_empty() {
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
            KeyCode::Home => {
                self.cursor = 0;
            }
            KeyCode::End => {
                self.cursor = self.value.len();
            }
            _ => {
                return false;
            }
        }
        true
    }
}

fn render_key_row(frame: &mut Frame, area: Rect, key: &KeyInput, theme: &Theme) {
    let layout = Layout::horizontal([Constraint::Percentage(70), Constraint::Percentage(30)]);
    let [name_area, type_area] = area.layout(&layout);
    key.name.render(frame, name_area, theme);
    key.key_type.render(frame, type_area, theme);
}

fn render_projection_row(
    frame: &mut Frame,
    area: Rect,
    projection: &ProjectionInput,
    theme: &Theme,
) {
    let layout = Layout::horizontal([Constraint::Percentage(35), Constraint::Percentage(65)]);
    let [kind_area, attrs_area] = area.layout(&layout);
    projection.kind.render(frame, kind_area, theme);
    projection.include_attrs.render(frame, attrs_area, theme);
}

fn render_row(
    frame: &mut Frame,
    area: Rect,
    kind: RowKind,
    state: &CreateTableState,
    theme: &Theme,
) {
    match kind {
        RowKind::TableName => state.table_name.render(frame, area, theme),
        RowKind::HashKey => render_key_row(frame, area, &state.hash_key, theme),
        RowKind::SortKey => render_key_row(frame, area, &state.sort_key, theme),
        RowKind::SectionHeader(section) => {
            render_section_header(frame, area, section.label(), theme)
        }
        RowKind::EmptyHint(section) => render_empty_hint(frame, area, section.empty_label(), theme),
        RowKind::GsiLabel(idx) => {
            let label = format!("GSI {}", idx + 1);
            render_index_label(frame, area, &label, theme);
        }
        RowKind::GsiName(idx) => state.gsis[idx].name.render(frame, area, theme),
        RowKind::GsiHash(idx) => render_key_row(frame, area, &state.gsis[idx].hash_key, theme),
        RowKind::GsiSort(idx) => render_key_row(frame, area, &state.gsis[idx].sort_key, theme),
        RowKind::GsiProjection(idx) => {
            render_projection_row(frame, area, &state.gsis[idx].projection, theme);
        }
        RowKind::LsiLabel(idx) => {
            let label = format!("LSI {}", idx + 1);
            render_index_label(frame, area, &label, theme);
        }
        RowKind::LsiName(idx) => state.lsis[idx].name.render(frame, area, theme),
        RowKind::LsiSort(idx) => render_key_row(frame, area, &state.lsis[idx].sort_key, theme),
        RowKind::LsiProjection(idx) => {
            render_projection_row(frame, area, &state.lsis[idx].projection, theme);
        }
    }
}

fn render_section_header(frame: &mut Frame, area: Rect, label: &str, theme: &Theme) {
    let text = Line::from(Span::styled(
        label,
        Style::default()
            .fg(theme.text_muted())
            .add_modifier(Modifier::BOLD),
    ));
    frame.render_widget(Paragraph::new(text), area);
}

fn render_index_label(frame: &mut Frame, area: Rect, label: &str, theme: &Theme) {
    let text = Line::from(Span::styled(label, Style::default().fg(theme.text_muted())));
    frame.render_widget(Paragraph::new(text), area);
}

fn render_empty_hint(frame: &mut Frame, area: Rect, label: &str, theme: &Theme) {
    let text = Line::from(Span::styled(label, Style::default().fg(theme.text_muted())));
    frame.render_widget(Paragraph::new(text), area);
}

fn render_actions(frame: &mut Frame, area: Rect, theme: &Theme, active: bool, selected: ActionId) {
    let base_style = Style::default()
        .fg(theme.accent())
        .add_modifier(Modifier::BOLD);
    let selected_style = Style::default()
        .fg(theme.panel_bg())
        .bg(theme.accent())
        .add_modifier(Modifier::BOLD);

    let style_for = |action| {
        if active && action == selected {
            selected_style
        } else {
            base_style
        }
    };

    let line = Line::from(vec![
        Span::styled("[+ GSI]", style_for(ActionId::AddGsi)),
        Span::raw("  "),
        Span::styled("[+ LSI]", style_for(ActionId::AddLsi)),
        Span::raw("  "),
        Span::styled("[- Index]", style_for(ActionId::Remove)),
        Span::raw("  "),
        Span::styled("[Create]", style_for(ActionId::Create)),
    ]);
    let actions = Paragraph::new(line).alignment(Alignment::Center);
    frame.render_widget(actions, area);
}

fn render_status(
    frame: &mut Frame,
    area: Rect,
    state: &CreateTableState,
    theme: &Theme,
    scroll_info: Option<String>,
) {
    let status = if let Some(error) = state.error.as_ref() {
        Some((
            format!("Error: {error}"),
            Style::default().fg(theme.error()),
        ))
    } else if matches!(state.status, CreateStatus::Submitting) {
        Some((
            "Creating table...".to_string(),
            Style::default().fg(theme.warning()),
        ))
    } else {
        None
    };

    if status.is_none() && scroll_info.is_none() {
        return;
    }

    if let Some(scroll) = scroll_info {
        let scroll_width = scroll.len() as u16;
        let layout = Layout::horizontal([Constraint::Fill(1), Constraint::Length(scroll_width)]);
        let [left, right] = area.layout(&layout);
        if let Some((text, style)) = status {
            let status = Paragraph::new(text).style(style);
            frame.render_widget(status, left);
        }
        let scroll = Paragraph::new(scroll)
            .style(Style::default().fg(theme.text_muted()))
            .alignment(Alignment::Right);
        frame.render_widget(scroll, right);
    } else if let Some((text, style)) = status {
        let status = Paragraph::new(text).style(style);
        frame.render_widget(status, area);
    }
}

fn render_remove_picker(frame: &mut Frame, area: Rect, picker: &RemovePickerState, theme: &Theme) {
    frame.render_widget(Clear, area);
    fill_bg(frame.buffer_mut(), area, theme.panel_bg());
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .title(Line::styled(
            " Remove index ",
            Style::default()
                .fg(theme.accent())
                .add_modifier(Modifier::BOLD),
        ))
        .border_style(Style::default().fg(theme.border()))
        .style(Style::default().bg(theme.panel_bg()).fg(theme.text()));

    frame.render_widget(block.clone(), area);
    let inner = block.inner(area).inner(Margin::new(1, 1));
    fill_bg(frame.buffer_mut(), inner, theme.panel_bg());

    let mut lines = Vec::new();
    for (idx, item) in picker.items.iter().enumerate() {
        let selected = idx == picker.selected;
        let style = if selected {
            Style::default()
                .bg(theme.selection_bg())
                .fg(theme.selection_fg())
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(theme.text())
        };
        let prefix = if selected { "> " } else { "  " };
        lines.push(Line::styled(format!("{prefix}{}", item.label), style));
    }
    let body = Paragraph::new(lines).style(Style::default().bg(theme.panel_bg()));
    frame.render_widget(body, inner);
}

fn remove_picker_rect(area: Rect, item_count: usize) -> Rect {
    let width = (area.width as f32 * 0.6) as u16;
    let width = width.max(36).min(area.width.saturating_sub(4));
    let list_height = item_count.saturating_add(2) as u16;
    let height = (area.height as f32 * 0.6) as u16;
    let height = height
        .max(6)
        .min(area.height.saturating_sub(4))
        .min(list_height.saturating_add(2));
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    Rect {
        x,
        y,
        width,
        height,
    }
}

#[derive(Debug, Clone)]
struct CreateTableResult {
    table_name: String,
    result: Result<(), String>,
}

fn build_spec(state: &CreateTableState) -> Result<CreateTableSpec, String> {
    let table_name = state.table_name.value().trim().to_string();
    let hash_key_name = state.hash_key.name.value().trim().to_string();
    let sort_key_name = state.sort_key.name.value().trim().to_string();

    let hash_key = KeySpec {
        name: hash_key_name,
        attr_type: state.hash_key.key_type.value,
    };

    let sort_key = if sort_key_name.is_empty() {
        None
    } else {
        Some(KeySpec {
            name: sort_key_name,
            attr_type: state.sort_key.key_type.value,
        })
    };

    let mut gsis = Vec::new();
    for gsi in &state.gsis {
        let name = gsi.name.value().trim().to_string();
        let hash_name = gsi.hash_key.name.value().trim().to_string();
        let sort_name = gsi.sort_key.name.value().trim().to_string();
        let sort_key = if sort_name.is_empty() {
            None
        } else {
            Some(KeySpec {
                name: sort_name,
                attr_type: gsi.sort_key.key_type.value,
            })
        };
        let projection = projection_from_input(
            gsi.projection.kind.value,
            gsi.projection.include_attrs.value(),
        )?;
        gsis.push(GsiSpec {
            name,
            hash_key: KeySpec {
                name: hash_name,
                attr_type: gsi.hash_key.key_type.value,
            },
            sort_key,
            projection,
        });
    }

    let mut lsis = Vec::new();
    for lsi in &state.lsis {
        let name = lsi.name.value().trim().to_string();
        let sort_name = lsi.sort_key.name.value().trim().to_string();
        let projection = projection_from_input(
            lsi.projection.kind.value,
            lsi.projection.include_attrs.value(),
        )?;
        lsis.push(LsiSpec {
            name,
            sort_key: KeySpec {
                name: sort_name,
                attr_type: lsi.sort_key.key_type.value,
            },
            projection,
        });
    }

    let spec = CreateTableSpec {
        table_name,
        hash_key,
        sort_key,
        gsis,
        lsis,
    };
    spec.validate()?;
    Ok(spec)
}

fn projection_from_input(kind: ProjectionKind, attrs: &str) -> Result<IndexProjection, String> {
    match kind {
        ProjectionKind::All => Ok(IndexProjection::All),
        ProjectionKind::KeysOnly => Ok(IndexProjection::KeysOnly),
        ProjectionKind::Include => {
            let attrs = parse_attr_list(attrs);
            if attrs.is_empty() {
                return Err("Include projection requires attributes".to_string());
            }
            Ok(IndexProjection::Include(attrs))
        }
    }
}

fn parse_attr_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .collect()
}
