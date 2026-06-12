use std::borrow::Cow;
use std::cell::Cell;

use crossterm::event::{KeyCode, KeyModifiers};
use ratatui::{
    Frame,
    layout::{Margin, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Paragraph, Wrap},
};

use dynamate::expr::builtins;

use crate::{
    env::WidgetId,
    help,
    util::{fill_bg, pad},
    widgets::{Popup, WidgetInner, theme::Theme},
};

pub(crate) struct ReferencePopup {
    inner: WidgetInner,
    scroll: Cell<u16>,
}

impl ReferencePopup {
    pub(crate) fn new(parent: WidgetId) -> Self {
        Self {
            inner: WidgetInner::new::<Self>(parent),
            scroll: Cell::new(0),
        }
    }

    fn lines(&self, theme: &Theme) -> Vec<Line<'static>> {
        let heading = |text: &str| {
            Line::from(Span::styled(
                text.to_string(),
                Style::default()
                    .fg(theme.accent())
                    .add_modifier(Modifier::BOLD),
            ))
        };
        let muted = |text: String| Line::from(Span::styled(text, Style::default().fg(theme.text_muted())));

        let mut lines: Vec<Line<'static>> = Vec::new();

        lines.push(heading("Functions"));
        for f in builtins::FUNCTIONS {
            lines.push(Line::from(Span::styled(
                f.signature.to_string(),
                Style::default().fg(theme.text()),
            )));
            lines.push(muted(format!("    {}", f.summary)));
            lines.push(muted(format!("    e.g. {}", f.example)));
        }
        lines.push(Line::from(""));

        lines.push(heading("Operators"));
        for op in builtins::OPERATORS {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("  {:<8}", op.symbols),
                    Style::default().fg(theme.text()),
                ),
                Span::styled(op.summary.to_string(), Style::default().fg(theme.text_muted())),
            ]));
        }
        lines.push(Line::from(""));

        lines.push(heading("Keywords"));
        for k in builtins::KEYWORDS {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("  {:<14}", k.word),
                    Style::default().fg(theme.text()),
                ),
                Span::styled(k.summary.to_string(), Style::default().fg(theme.text_muted())),
            ]));
            lines.push(muted(format!("    e.g. {}", k.example)));
        }
        lines.push(Line::from(""));

        lines.push(heading("Value forms"));
        for (form, desc) in builtins::VALUE_FORMS {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("  {:<20}", form),
                    Style::default().fg(theme.text()),
                ),
                Span::styled(desc.to_string(), Style::default().fg(theme.text_muted())),
            ]));
        }
        lines.push(Line::from(""));

        lines.push(heading("Single-token shortcut"));
        lines.push(muted(
            "    A single bare value targets the table partition key:".to_string(),
        ));
        for (input, expands) in builtins::PK_SHORTCUT {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("    {:<14}", input),
                    Style::default().fg(theme.text()),
                ),
                Span::styled(
                    format!("→ {}", expands),
                    Style::default().fg(theme.text_muted()),
                ),
            ]));
        }
        lines.push(Line::from(""));
        lines.push(muted("    A blank query runs a full table scan.".to_string()));

        lines
    }

    const HELP: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("↑/↓"),
            short: Cow::Borrowed("scroll"),
            long: Cow::Borrowed("Scroll the reference"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("close"),
            long: Cow::Borrowed("Close the reference"),
            ctrl: None,
            shift: None,
            alt: None,
        },
    ];
}

impl crate::widgets::Widget for ReferencePopup {
    fn inner(&self) -> &WidgetInner {
        &self.inner
    }

    fn help(&self) -> Option<&[help::Entry<'_>]> {
        Some(Self::HELP)
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        fill_bg(frame.buffer_mut(), area, theme.panel_bg());
        let title = Line::styled(
            pad("Query Reference", 1),
            Style::default()
                .fg(theme.accent())
                .add_modifier(Modifier::BOLD),
        )
        .centered();
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .title(title)
            .border_style(Style::default().fg(theme.border()))
            .style(Style::default().bg(theme.panel_bg()).fg(theme.text()));
        frame.render_widget(block.clone(), area);
        let inner = block.inner(area).inner(Margin::new(1, 0));

        let lines = self.lines(theme);
        let total = lines.len() as u16;
        let view = inner.height;
        let max_scroll = total.saturating_sub(view);
        if self.scroll.get() > max_scroll {
            self.scroll.set(max_scroll);
        }

        let paragraph = Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .scroll((self.scroll.get(), 0));
        frame.render_widget(paragraph, inner);
    }

    fn handle_event(&self, ctx: crate::env::WidgetCtx, event: &crossterm::event::Event) -> bool {
        let Some(key) = event.as_key_press_event() else {
            return true;
        };
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                ctx.dismiss_popup();
                ctx.invalidate();
            }
            KeyCode::Char('g') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                ctx.dismiss_popup();
                ctx.invalidate();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.scroll.set(self.scroll.get().saturating_sub(1));
                ctx.invalidate();
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.scroll.set(self.scroll.get().saturating_add(1));
                ctx.invalidate();
            }
            KeyCode::PageUp => {
                self.scroll.set(self.scroll.get().saturating_sub(10));
                ctx.invalidate();
            }
            KeyCode::PageDown => {
                self.scroll.set(self.scroll.get().saturating_add(10));
                ctx.invalidate();
            }
            KeyCode::Home => {
                self.scroll.set(0);
                ctx.invalidate();
            }
            _ => {}
        }
        true
    }
}

impl Popup for ReferencePopup {
    fn rect(&self, area: Rect) -> Rect {
        let min_width = 60;
        let max_width = 100;
        let mut width = (area.width as f32 * 0.7) as u16;
        width = width.clamp(min_width, max_width);
        width = width.min(area.width.saturating_sub(4)).max(1);
        let height = area.height.saturating_sub(4).clamp(1, 32);
        let x = area.x + (area.width.saturating_sub(width)) / 2;
        let y = area.y + (area.height.saturating_sub(height)) / 2;
        Rect {
            x,
            y,
            width,
            height,
        }
    }
}
