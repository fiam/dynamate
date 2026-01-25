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
                keys: variant.keys.as_ref().map(|value| Cow::Owned(value.as_ref().to_owned())),
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
                keys: variant.keys.as_ref().map(|value| Cow::Owned(value.as_ref().to_owned())),
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
                keys: variant.keys.as_ref().map(|value| Cow::Owned(value.as_ref().to_owned())),
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

    pub fn display(&self, modifiers: KeyModifiers, mode: ModDisplay) -> DisplayEntry<'a> {
        match mode {
            ModDisplay::Swap => self.display_swap(modifiers),
            ModDisplay::Both => self.display_both(),
        }
    }

    fn display_swap(&self, modifiers: KeyModifiers) -> DisplayEntry<'a> {
        let mut keys = self.keys.as_ref();
        let mut short = self.short.as_ref();
        let mut long = self.long.as_ref();

        if modifiers.contains(KeyModifiers::CONTROL) {
            if let Some(variant) = self.ctrl.as_ref() {
                if let Some(value) = variant.keys.as_ref() {
                    keys = value.as_ref();
                }
                if let Some(value) = variant.short.as_ref() {
                    short = value.as_ref();
                }
                if let Some(value) = variant.long.as_ref() {
                    long = value.as_ref();
                }
            }
        } else if modifiers.contains(KeyModifiers::SHIFT) {
            if let Some(variant) = self.shift.as_ref() {
                if let Some(value) = variant.keys.as_ref() {
                    keys = value.as_ref();
                }
                if let Some(value) = variant.short.as_ref() {
                    short = value.as_ref();
                }
                if let Some(value) = variant.long.as_ref() {
                    long = value.as_ref();
                }
            }
        } else if modifiers.contains(KeyModifiers::ALT) {
            if let Some(variant) = self.alt.as_ref() {
                if let Some(value) = variant.keys.as_ref() {
                    keys = value.as_ref();
                }
                if let Some(value) = variant.short.as_ref() {
                    short = value.as_ref();
                }
                if let Some(value) = variant.long.as_ref() {
                    long = value.as_ref();
                }
            }
        }

        DisplayEntry {
            keys: Cow::Owned(keys.to_string()),
            short: Cow::Owned(short.to_string()),
            long: Cow::Owned(long.to_string()),
        }
    }

    fn display_both(&self) -> DisplayEntry<'a> {
        let variant = self
            .ctrl
            .as_ref()
            .or(self.shift.as_ref())
            .or(self.alt.as_ref());

        let base_keys = self.keys.as_ref();
        let base_short = self.short.as_ref();
        let base_long = self.long.as_ref();

        let (mut keys, mut short, mut long) = (
            base_keys.to_string(),
            base_short.to_string(),
            base_long.to_string(),
        );

        if let Some(variant) = variant {
            if let Some(vk) = variant.keys.as_ref() {
                let vk = vk.as_ref();
                keys = if keys.is_empty() {
                    vk.to_string()
                } else {
                    format!("{keys}/{vk}")
                };
            }
            if let Some(vs) = variant.short.as_ref() {
                let vs = vs.as_ref();
                short = if short.is_empty() {
                    vs.to_string()
                } else {
                    format!("{short} / {vs}")
                };
            }
            if let Some(vl) = variant.long.as_ref() {
                let vl = vl.as_ref();
                long = if long.is_empty() {
                    vl.to_string()
                } else {
                    format!("{long} / {vl}")
                };
            }
        }

        DisplayEntry {
            keys: Cow::Owned(keys),
            short: Cow::Owned(short),
            long: Cow::Owned(long),
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
        .filter_map(|entry| {
            let display = entry.display(modifiers, mode);
            if display.keys.is_empty() {
                return None;
            }
            let keys = display.keys.as_ref();
            Some([
                Span::styled(format!("[{keys}]"), Style::default().bold()),
                Span::raw(" "),
                Span::raw(display.short.to_string()),
                Span::styled(" • ", Style::default().fg(theme.neutral())),
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
