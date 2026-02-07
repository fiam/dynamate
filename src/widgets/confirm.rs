use std::{borrow::Cow, cell::Cell};

use crossterm::event::{KeyCode, KeyModifiers};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Layout, Margin, Rect},
    style::{Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Paragraph, Wrap},
};

use crate::{
    help,
    util::{fill_bg, pad},
    widgets::{Popup, WidgetInner, theme::Theme},
};

pub struct ConfirmPopup {
    inner: WidgetInner,
    title: String,
    message: String,
    confirm_label: String,
    cancel_label: String,
    on_confirm: Box<dyn Fn() + Send + 'static>,
    selection: Cell<Selection>,
    confirm_action: ConfirmAction,
    help_entries: Vec<help::Entry<'static>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Selection {
    Confirm,
    Cancel,
}

#[derive(Clone, Debug)]
pub struct ConfirmHotkey {
    pub code: KeyCode,
    pub modifiers: KeyModifiers,
    pub label: String,
}

#[derive(Clone, Debug)]
pub struct ConfirmAction {
    pub hotkey: ConfirmHotkey,
    pub short: String,
    pub long: String,
}

impl ConfirmAction {
    pub fn new(
        code: KeyCode,
        modifiers: KeyModifiers,
        label: impl Into<String>,
        short: impl Into<String>,
        long: impl Into<String>,
    ) -> Self {
        Self {
            hotkey: ConfirmHotkey {
                code,
                modifiers,
                label: label.into(),
            },
            short: short.into(),
            long: long.into(),
        }
    }
}

impl ConfirmPopup {
    fn delete_item_action() -> ConfirmAction {
        ConfirmAction {
            hotkey: ConfirmHotkey {
                code: KeyCode::Char('d'),
                modifiers: KeyModifiers::CONTROL,
                label: "^d".to_string(),
            },
            short: "delete".to_string(),
            long: "Delete item".to_string(),
        }
    }

    pub fn new(
        title: impl Into<String>,
        message: impl Into<String>,
        confirm_label: impl Into<String>,
        cancel_label: impl Into<String>,
        on_confirm: impl Fn() + Send + 'static,
        parent: crate::env::WidgetId,
    ) -> Self {
        Self::new_with_action(
            title,
            message,
            confirm_label,
            cancel_label,
            Self::delete_item_action(),
            on_confirm,
            parent,
        )
    }

    pub fn new_with_action(
        title: impl Into<String>,
        message: impl Into<String>,
        confirm_label: impl Into<String>,
        cancel_label: impl Into<String>,
        confirm_action: ConfirmAction,
        on_confirm: impl Fn() + Send + 'static,
        parent: crate::env::WidgetId,
    ) -> Self {
        let help_entries = vec![
            help::Entry {
                keys: Cow::Borrowed("tab/←/→"),
                short: Cow::Borrowed("move"),
                long: Cow::Borrowed("Move between actions"),
                ctrl: None,
                shift: None,
                alt: None,
            },
            help::Entry {
                keys: Cow::Borrowed("⏎"),
                short: Cow::Borrowed("select"),
                long: Cow::Borrowed("Select action"),
                ctrl: None,
                shift: None,
                alt: None,
            },
            help::Entry {
                keys: Cow::Owned(confirm_action.hotkey.label.clone()),
                short: Cow::Owned(confirm_action.short.clone()),
                long: Cow::Owned(confirm_action.long.clone()),
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
        Self {
            inner: WidgetInner::new::<Self>(parent),
            title: title.into(),
            message: message.into(),
            confirm_label: confirm_label.into(),
            cancel_label: cancel_label.into(),
            on_confirm: Box::new(on_confirm),
            selection: Cell::new(Selection::Cancel),
            confirm_action,
            help_entries,
        }
    }
}

impl crate::widgets::Widget for ConfirmPopup {
    fn inner(&self) -> &WidgetInner {
        &self.inner
    }

    fn help(&self) -> Option<&[help::Entry<'_>]> {
        Some(self.help_entries.as_slice())
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        fill_bg(frame.buffer_mut(), area, theme.panel_bg());
        let title = Line::styled(
            pad(self.title.as_str(), 1),
            Style::default()
                .fg(theme.error())
                .add_modifier(Modifier::BOLD),
        )
        .centered();
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .title(title)
            .border_style(Style::default().fg(theme.error()))
            .style(Style::default().bg(theme.panel_bg()).fg(theme.text()));

        frame.render_widget(block.clone(), area);
        let inner = block.inner(area).inner(Margin::new(1, 1));
        let layout = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).split(inner);

        let mut lines = Vec::new();
        for line in self.message.lines() {
            if let Some((key, value)) = line.split_once('=') {
                let key_part = format!("{key}=");
                let value_part = value.to_string();
                lines.push(Line::from(vec![
                    Span::styled(
                        key_part,
                        Style::default()
                            .fg(theme.text_muted())
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(value_part, Style::default().fg(theme.text())),
                ]));
            } else {
                lines.push(Line::from(Span::styled(
                    line,
                    Style::default().fg(theme.text()),
                )));
            }
        }
        if lines.is_empty() {
            lines.push(Line::from(""));
        }
        let body = Paragraph::new(Text::from(lines))
            .style(Style::default().fg(theme.text()))
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: true });
        frame.render_widget(body, layout[0]);

        let confirm_selected = self.selection.get() == Selection::Confirm;
        let cancel_selected = self.selection.get() == Selection::Cancel;
        let confirm_style = if confirm_selected {
            Style::default()
                .bg(theme.error())
                .fg(theme.selection_fg())
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
                .fg(theme.error())
                .add_modifier(Modifier::BOLD)
        };
        let cancel_style = if cancel_selected {
            Style::default()
                .bg(theme.selection_bg())
                .fg(theme.selection_fg())
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(theme.text())
        };
        let confirm_button = Span::styled(format!("[ {} ]", self.confirm_label), confirm_style);
        let cancel_button = Span::styled(format!("[ {} ]", self.cancel_label), cancel_style);
        let buttons = Line::from(vec![confirm_button, Span::raw("  "), cancel_button]);
        let footer = Paragraph::new(Text::from(buttons)).alignment(Alignment::Center);
        frame.render_widget(footer, layout[1]);
    }

    fn handle_event(&self, ctx: crate::env::WidgetCtx, event: &crossterm::event::Event) -> bool {
        let Some(key) = event.as_key_press_event() else {
            return true;
        };

        if key.code == self.confirm_action.hotkey.code
            && key.modifiers.contains(self.confirm_action.hotkey.modifiers)
        {
            (self.on_confirm)();
            ctx.dismiss_popup();
            ctx.invalidate();
            return true;
        }

        match key.code {
            KeyCode::Left | KeyCode::Right | KeyCode::Tab | KeyCode::BackTab => {
                let next = match self.selection.get() {
                    Selection::Confirm => Selection::Cancel,
                    Selection::Cancel => Selection::Confirm,
                };
                self.selection.set(next);
                ctx.invalidate();
                true
            }
            KeyCode::Enter => {
                if self.selection.get() == Selection::Confirm {
                    (self.on_confirm)();
                }
                ctx.dismiss_popup();
                ctx.invalidate();
                true
            }
            KeyCode::Esc => {
                ctx.dismiss_popup();
                ctx.invalidate();
                true
            }
            _ => true,
        }
    }
}

impl Popup for ConfirmPopup {
    fn rect(&self, area: Rect) -> Rect {
        let width = (area.width as f32 * 0.4) as u16;
        let height = (area.height as f32 * 0.18) as u16;
        let width = width.max(34).min(area.width.saturating_sub(4));
        let height = height.max(7).min(area.height.saturating_sub(4));
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
