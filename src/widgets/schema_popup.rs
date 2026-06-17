//! A popup that shows the schema of the selected collection — columns (for SQL
//! tables), key fields, and secondary indexes. `←/→` switch between tables
//! (kept in sync with the table list underneath via [`SchemaNavEvent`]); `↑/↓`
//! and PageUp/PageDown scroll long schemas.

use std::cell::Cell;

use crossterm::event::{Event, KeyCode};
use ratatui::{
    Frame,
    layout::{Margin, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Paragraph},
};

use dynamate::core::schema::{CollectionSchema, KeyRole};

use crate::{
    util::{fill_bg, pad},
    widgets::{self, theme::Theme},
};

/// Broadcast when the popup switches tables, so the table list can follow.
pub struct SchemaNavEvent {
    pub table: String,
}

pub struct SchemaPopup {
    inner: widgets::WidgetInner,
    schemas: Vec<CollectionSchema>,
    index: Cell<usize>,
    scroll: Cell<usize>,
    /// Visible content rows and total rows, recorded on render for clamping.
    viewport: Cell<usize>,
    content_len: Cell<usize>,
}

impl SchemaPopup {
    pub fn new(schemas: Vec<CollectionSchema>, index: usize, parent: crate::env::WidgetId) -> Self {
        let index = index.min(schemas.len().saturating_sub(1));
        Self {
            inner: widgets::WidgetInner::new::<Self>(parent),
            schemas,
            index: Cell::new(index),
            scroll: Cell::new(0),
            viewport: Cell::new(0),
            content_len: Cell::new(0),
        }
    }

    /// Switch tables by `delta`; resets scroll and announces the new table.
    fn switch(&self, delta: isize, ctx: &crate::env::WidgetCtx) -> bool {
        let len = self.schemas.len();
        if len <= 1 {
            return false;
        }
        let current = self.index.get() as isize;
        let next = (current + delta).clamp(0, len as isize - 1);
        if next == current {
            return false;
        }
        self.index.set(next as usize);
        self.scroll.set(0);
        if let Some(schema) = self.schemas.get(next as usize) {
            ctx.broadcast_event(SchemaNavEvent {
                table: schema.name.clone(),
            });
        }
        true
    }

    fn scroll_by(&self, delta: isize) -> bool {
        let max = self.content_len.get().saturating_sub(self.viewport.get());
        let current = self.scroll.get() as isize;
        let next = (current + delta).clamp(0, max as isize);
        if next == current {
            return false;
        }
        self.scroll.set(next as usize);
        true
    }
}

/// Build the body lines for one collection's schema.
fn schema_lines(schema: &CollectionSchema, theme: &Theme) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    let label =
        |text: &str| Span::styled(text.to_string(), Style::default().fg(theme.text_muted()));

    let key_names: Vec<&str> = schema.key.fields.iter().map(|f| f.name.as_str()).collect();
    if !key_names.is_empty() {
        lines.push(Line::from(vec![
            label("Primary key  "),
            Span::styled(
                key_names.join(", "),
                Style::default()
                    .fg(theme.accent())
                    .add_modifier(Modifier::BOLD),
            ),
        ]));
        lines.push(Line::raw(""));
    }

    if schema.columns.is_empty() {
        if schema.key.fields.is_empty() {
            lines.push(Line::styled(
                "No schema information available.",
                Style::default().fg(theme.text_muted()),
            ));
        } else {
            lines.push(label_line("Key fields", theme));
            for field in &schema.key.fields {
                let role = match field.role {
                    KeyRole::Partition => "partition",
                    KeyRole::Sort => "sort",
                };
                lines.push(Line::from(vec![
                    Span::raw("  "),
                    Span::styled(field.name.clone(), Style::default().fg(theme.text())),
                    Span::raw("  "),
                    Span::styled(
                        format!("{:?}", field.ty).to_lowercase(),
                        Style::default().fg(theme.text_muted()),
                    ),
                    Span::raw("  "),
                    Span::styled(role.to_string(), Style::default().fg(theme.text_muted())),
                ]));
            }
        }
    } else {
        let key_set: std::collections::HashSet<&str> = key_names.iter().copied().collect();
        let name_width = schema
            .columns
            .iter()
            .map(|c| c.name.len())
            .max()
            .unwrap_or(0)
            .max(4);
        let type_width = schema
            .columns
            .iter()
            .map(|c| c.data_type.len())
            .max()
            .unwrap_or(0)
            .max(4);
        lines.push(label_line(
            &format!("Columns ({})", schema.columns.len()),
            theme,
        ));
        for col in &schema.columns {
            let is_key = key_set.contains(col.name.as_str());
            let name_style = if is_key {
                Style::default()
                    .fg(theme.accent())
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(theme.text())
            };
            let null = if col.nullable { "null" } else { "not null" };
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(format!("{:<name_width$}", col.name), name_style),
                Span::raw("  "),
                Span::styled(
                    format!("{:<type_width$}", col.data_type),
                    Style::default().fg(theme.success()),
                ),
                Span::raw("  "),
                Span::styled(null.to_string(), Style::default().fg(theme.text_muted())),
            ]));
        }
    }

    if !schema.indexes.is_empty() {
        lines.push(Line::raw(""));
        lines.push(label_line(
            &format!("Indexes ({})", schema.indexes.len()),
            theme,
        ));
        for index in &schema.indexes {
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(index.name.clone(), Style::default().fg(theme.text())),
            ]));
        }
    }

    if let Some(ttl) = schema.ttl_attribute.as_ref() {
        lines.push(Line::raw(""));
        lines.push(Line::from(vec![
            label("TTL attribute  "),
            Span::styled(ttl.clone(), Style::default().fg(theme.text())),
        ]));
    }

    lines
}

fn label_line(text: &str, theme: &Theme) -> Line<'static> {
    Line::styled(
        text.to_string(),
        Style::default()
            .fg(theme.text_muted())
            .add_modifier(Modifier::BOLD),
    )
}

impl widgets::Widget for SchemaPopup {
    fn inner(&self) -> &widgets::WidgetInner {
        &self.inner
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        fill_bg(frame.buffer_mut(), area, theme.panel_bg());
        let Some(schema) = self.schemas.get(self.index.get()) else {
            return;
        };

        let position = if self.schemas.len() > 1 {
            format!(" ({}/{})", self.index.get() + 1, self.schemas.len())
        } else {
            String::new()
        };
        let title = Line::styled(
            pad(format!("Schema: {}{position}", schema.name), 2),
            Style::default()
                .fg(theme.accent())
                .add_modifier(Modifier::BOLD),
        )
        .centered();
        let footer_text = if self.schemas.len() > 1 {
            "←/→ table · ↑/↓ scroll · esc close"
        } else {
            "↑/↓ scroll · esc close"
        };
        let footer = Line::styled(pad(footer_text, 2), Style::default().fg(theme.text_muted()));
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .title(title)
            .title_bottom(footer)
            .border_style(Style::default().fg(theme.border()))
            .style(Style::default().bg(theme.panel_bg()).fg(theme.text()));

        let inner = area.inner(Margin::new(1, 1));
        let lines = schema_lines(schema, theme);
        // Record dimensions for scroll clamping, then clamp the offset.
        self.content_len.set(lines.len());
        self.viewport.set(inner.height as usize);
        let max = lines.len().saturating_sub(inner.height as usize);
        if self.scroll.get() > max {
            self.scroll.set(max);
        }
        let body = Paragraph::new(lines)
            .scroll((self.scroll.get() as u16, 0))
            .block(block);
        frame.render_widget(body, inner);
    }

    fn handle_event(&self, ctx: crate::env::WidgetCtx, event: &Event) -> bool {
        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Left | KeyCode::Char('h') => {
                    if self.switch(-1, &ctx) {
                        ctx.invalidate();
                    }
                    return true;
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    if self.switch(1, &ctx) {
                        ctx.invalidate();
                    }
                    return true;
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if self.scroll_by(1) {
                        ctx.invalidate();
                    }
                    return true;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if self.scroll_by(-1) {
                        ctx.invalidate();
                    }
                    return true;
                }
                KeyCode::PageDown => {
                    if self.scroll_by(self.viewport.get().max(1) as isize) {
                        ctx.invalidate();
                    }
                    return true;
                }
                KeyCode::PageUp => {
                    if self.scroll_by(-(self.viewport.get().max(1) as isize)) {
                        ctx.invalidate();
                    }
                    return true;
                }
                _ => {}
            }
        }
        false
    }
}

impl widgets::Popup for SchemaPopup {
    fn rect(&self, area: Rect) -> Rect {
        let rows = self.schemas.get(self.index.get()).map_or(8, |s| {
            s.columns.len().max(s.key.fields.len()) + s.indexes.len() + 8
        });
        let width = (area.width as f32 * 0.6) as u16;
        let width = width.max(44).min(area.width.saturating_sub(4));
        // Cap height to most of the screen; longer schemas scroll.
        let max_height = (area.height as f32 * 0.8) as u16;
        let height = (rows as u16 + 2)
            .clamp(8, max_height.max(8))
            .min(area.height.saturating_sub(4));
        let x = area.x + (area.width - width) / 2;
        let y = area.y + (area.height.saturating_sub(height)) / 2;
        Rect {
            x,
            y,
            width,
            height,
        }
    }
}
