use std::borrow::Cow;

use crossterm::event::KeyModifiers;
use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{Paragraph, Wrap},
};
use unicode_width::UnicodeWidthStr;

mod widget;

pub use widget::Widget;

use crate::widgets::theme::Theme;

#[derive(Clone)]
pub struct Entry<'a> {
    pub keys: Cow<'a, str>,
    pub short: Cow<'a, str>,
    pub long: Cow<'a, str>,
    pub ctrl: Option<Variant<'a>>,
    pub shift: Option<Variant<'a>>,
    pub alt: Option<Variant<'a>>,
}

#[derive(Clone)]
pub struct Variant<'a> {
    pub keys: Option<Cow<'a, str>>,
    pub short: Option<Cow<'a, str>>,
    pub long: Option<Cow<'a, str>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModDisplay {
    #[allow(dead_code)]
    Swap,
    Both,
}

#[derive(Clone)]
pub struct DisplayEntry<'a> {
    pub keys: Cow<'a, str>,
    pub short: Cow<'a, str>,
    pub long: Cow<'a, str>,
}

impl<'a> Entry<'a> {
    fn to_owned_entry(&self) -> Entry<'static> {
        Entry {
            keys: Cow::Owned(self.keys.as_ref().to_owned()),
            short: Cow::Owned(self.short.as_ref().to_owned()),
            long: Cow::Owned(self.long.as_ref().to_owned()),
            ctrl: self.ctrl.as_ref().map(|variant| Variant {
                keys: variant
                    .keys
                    .as_ref()
                    .map(|value| Cow::Owned(value.as_ref().to_owned())),
                short: variant
                    .short
                    .as_ref()
                    .map(|value| Cow::Owned(value.as_ref().to_owned())),
                long: variant
                    .long
                    .as_ref()
                    .map(|value| Cow::Owned(value.as_ref().to_owned())),
            }),
            shift: self.shift.as_ref().map(|variant| Variant {
                keys: variant
                    .keys
                    .as_ref()
                    .map(|value| Cow::Owned(value.as_ref().to_owned())),
                short: variant
                    .short
                    .as_ref()
                    .map(|value| Cow::Owned(value.as_ref().to_owned())),
                long: variant
                    .long
                    .as_ref()
                    .map(|value| Cow::Owned(value.as_ref().to_owned())),
            }),
            alt: self.alt.as_ref().map(|variant| Variant {
                keys: variant
                    .keys
                    .as_ref()
                    .map(|value| Cow::Owned(value.as_ref().to_owned())),
                short: variant
                    .short
                    .as_ref()
                    .map(|value| Cow::Owned(value.as_ref().to_owned())),
                long: variant
                    .long
                    .as_ref()
                    .map(|value| Cow::Owned(value.as_ref().to_owned())),
            }),
        }
    }

    pub fn display_entries(
        &self,
        modifiers: KeyModifiers,
        mode: ModDisplay,
    ) -> Vec<DisplayEntry<'a>> {
        match mode {
            ModDisplay::Swap => vec![self.display_swap(modifiers)],
            ModDisplay::Both => self.display_both(),
        }
    }

    fn display_swap(&self, modifiers: KeyModifiers) -> DisplayEntry<'a> {
        if modifiers.contains(KeyModifiers::CONTROL) {
            if let Some(variant) = self.ctrl.as_ref() {
                return self.make_display_entry(Some(variant));
            }
        } else if modifiers.contains(KeyModifiers::SHIFT) {
            if let Some(variant) = self.shift.as_ref() {
                return self.make_display_entry(Some(variant));
            }
        } else if modifiers.contains(KeyModifiers::ALT)
            && let Some(variant) = self.alt.as_ref()
        {
            return self.make_display_entry(Some(variant));
        }

        self.make_display_entry(None)
    }

    fn display_both(&self) -> Vec<DisplayEntry<'a>> {
        let mut entries = Vec::new();
        entries.push(self.make_display_entry(None));
        if let Some(variant) = self.ctrl.as_ref() {
            entries.push(self.make_display_entry(Some(variant)));
        }
        if let Some(variant) = self.shift.as_ref() {
            entries.push(self.make_display_entry(Some(variant)));
        }
        if let Some(variant) = self.alt.as_ref() {
            entries.push(self.make_display_entry(Some(variant)));
        }
        entries
    }

    fn make_display_entry(&self, variant: Option<&Variant<'a>>) -> DisplayEntry<'a> {
        let base_keys = self.keys.as_ref();
        let base_short = self.short.as_ref();
        let base_long = self.long.as_ref();

        let keys = variant
            .and_then(|v| v.keys.as_ref())
            .map(|v| v.as_ref())
            .unwrap_or(base_keys);
        let short = variant
            .and_then(|v| v.short.as_ref())
            .map(|v| v.as_ref())
            .unwrap_or(base_short);
        let long = variant
            .and_then(|v| v.long.as_ref())
            .map(|v| v.as_ref())
            .unwrap_or(base_long);

        DisplayEntry {
            keys: Cow::Owned(keys.to_string()),
            short: Cow::Owned(short.to_string()),
            long: Cow::Owned(long.to_string()),
        }
    }
}

fn make_spans<'a>(
    entries: &'a [&Entry<'a>],
    theme: &Theme,
    modifiers: KeyModifiers,
    mode: ModDisplay,
) -> Vec<Span<'a>> {
    let mut spans: Vec<_> = entries
        .iter()
        .flat_map(|entry| entry.display_entries(modifiers, mode))
        .filter_map(|display| {
            if display.keys.is_empty() {
                return None;
            }
            let keys = display.keys.as_ref();
            Some([
                Span::styled(
                    format!("[{keys}]"),
                    Style::default().bold().fg(theme.accent()),
                ),
                Span::raw(" "),
                Span::styled(display.short.to_string(), Style::default().fg(theme.text())),
                Span::styled(" • ", Style::default().fg(theme.text_muted())),
            ])
        })
        .flatten()
        .collect();
    // Remove the last span
    if !spans.is_empty() {
        spans.pop();
    }
    spans
}

pub fn height<'a>(
    entries: &'a [&Entry<'a>],
    area: Rect,
    modifiers: KeyModifiers,
    mode: ModDisplay,
) -> u16 {
    let theme = Theme::default();
    let total_width: usize = make_spans(entries, &theme, modifiers, mode)
        .iter()
        .map(|s| s.content.width())
        .sum();
    let available_width = area.width as usize;

    // number of rows needed = ceil(total_width / available_width)
    total_width.div_ceil(available_width) as u16
}

pub fn render<'a>(
    entries: &'a [&Entry<'a>],
    frame: &mut Frame,
    area: Rect,
    theme: &Theme,
    modifiers: KeyModifiers,
    mode: ModDisplay,
) {
    let spans = make_spans(entries, theme, modifiers, mode);
    let footer = Paragraph::new(Line::from(spans))
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true });

    frame.render_widget(footer, area);
}
