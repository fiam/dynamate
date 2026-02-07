use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    path::{Path, PathBuf},
};

use crossterm::event::KeyCode;
use directories::BaseDirs;
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Layout, Margin, Rect},
    style::{Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Paragraph},
};

use crate::{
    help,
    util::{abbreviate_home, fill_bg, pad},
    widgets::{Popup, WidgetInner, theme::Theme},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExportMode {
    Item,
    Results,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Focus {
    Directory,
    Filename,
    Checkbox,
    Export,
    Cancel,
}

struct FormInput {
    value: String,
    cursor: usize,
}

impl FormInput {
    fn new(value: String) -> Self {
        let cursor = value.chars().count();
        Self { value, cursor }
    }

    fn value(&self) -> &str {
        &self.value
    }

    fn handle_key(&mut self, key: &crossterm::event::KeyEvent) -> bool {
        match key.code {
            KeyCode::Char('a')
                if key
                    .modifiers
                    .contains(crossterm::event::KeyModifiers::CONTROL) =>
            {
                self.cursor = 0;
                true
            }
            KeyCode::Char('e')
                if key
                    .modifiers
                    .contains(crossterm::event::KeyModifiers::CONTROL) =>
            {
                self.cursor = self.value.chars().count();
                true
            }
            KeyCode::Char(c) => {
                let idx = char_to_byte_idx(&self.value, self.cursor);
                self.value.insert(idx, c);
                self.cursor += 1;
                true
            }
            KeyCode::Backspace => {
                if self.cursor > 0 {
                    let remove_idx = self.cursor - 1;
                    let start = char_to_byte_idx(&self.value, remove_idx);
                    let end = char_to_byte_idx(&self.value, remove_idx + 1);
                    self.value.replace_range(start..end, "");
                    self.cursor -= 1;
                }
                true
            }
            KeyCode::Delete => {
                let len = self.value.chars().count();
                if self.cursor < len {
                    let start = char_to_byte_idx(&self.value, self.cursor);
                    let end = char_to_byte_idx(&self.value, self.cursor + 1);
                    self.value.replace_range(start..end, "");
                }
                true
            }
            KeyCode::Left => {
                if self.cursor > 0 {
                    self.cursor -= 1;
                }
                true
            }
            KeyCode::Right => {
                let len = self.value.chars().count();
                if self.cursor < len {
                    self.cursor += 1;
                }
                true
            }
            KeyCode::Home => {
                self.cursor = 0;
                true
            }
            KeyCode::End => {
                self.cursor = self.value.chars().count();
                true
            }
            _ => false,
        }
    }

    fn visible_text(&self, width: usize) -> (String, usize) {
        if width == 0 {
            return (String::new(), 0);
        }
        let len = self.value.chars().count();
        let cursor = self.cursor.min(len);
        let mut start = 0usize;
        if cursor >= width {
            start = cursor + 1 - width;
        }
        let text: String = self.value.chars().skip(start).take(width).collect();
        let cursor_pos = cursor.saturating_sub(start).min(width.saturating_sub(1));
        (text, cursor_pos)
    }
}

fn char_to_byte_idx(value: &str, char_idx: usize) -> usize {
    value
        .char_indices()
        .nth(char_idx)
        .map(|(idx, _)| idx)
        .unwrap_or_else(|| value.len())
}

pub(crate) struct ExportPopup {
    inner: WidgetInner,
    mode: ExportMode,
    dir_input: RefCell<FormInput>,
    file_input: RefCell<FormInput>,
    fetch_all: Cell<bool>,
    focus: Cell<Focus>,
    on_confirm: Box<dyn Fn(PathBuf, bool) + Send + 'static>,
    help_entries: Vec<help::Entry<'static>>,
}

impl ExportPopup {
    const LABEL_WIDTH: u16 = 10;
    pub(crate) fn new(
        mode: ExportMode,
        path: PathBuf,
        fetch_all: bool,
        on_confirm: impl Fn(PathBuf, bool) + Send + 'static,
        parent: crate::env::WidgetId,
    ) -> Self {
        let (dir, file) = split_path(&path);
        let mut help_entries = vec![
            help::Entry {
                keys: Cow::Borrowed("tab/shift+tab"),
                short: Cow::Borrowed("move"),
                long: Cow::Borrowed("Cycle fields"),
                ctrl: None,
                shift: None,
                alt: None,
            },
            help::Entry {
                keys: Cow::Borrowed("⏎"),
                short: Cow::Borrowed("select"),
                long: Cow::Borrowed("Confirm export"),
                ctrl: None,
                shift: None,
                alt: None,
            },
            help::Entry {
                keys: Cow::Borrowed("esc"),
                short: Cow::Borrowed("cancel"),
                long: Cow::Borrowed("Cancel export"),
                ctrl: None,
                shift: None,
                alt: None,
            },
        ];
        if matches!(mode, ExportMode::Results) {
            help_entries.insert(
                1,
                help::Entry {
                    keys: Cow::Borrowed("space/f"),
                    short: Cow::Borrowed("toggle"),
                    long: Cow::Borrowed("Fetch all results"),
                    ctrl: None,
                    shift: None,
                    alt: None,
                },
            );
        }
        Self {
            inner: WidgetInner::new::<Self>(parent),
            mode,
            dir_input: RefCell::new(FormInput::new(dir)),
            file_input: RefCell::new(FormInput::new(file)),
            fetch_all: Cell::new(fetch_all),
            focus: Cell::new(Focus::Export),
            on_confirm: Box::new(on_confirm),
            help_entries,
        }
    }

    fn next_focus(&self) {
        let has_checkbox = matches!(self.mode, ExportMode::Results);
        let next = match (self.focus.get(), has_checkbox) {
            (Focus::Directory, _) => Focus::Filename,
            (Focus::Filename, true) => Focus::Checkbox,
            (Focus::Filename, false) => Focus::Export,
            (Focus::Checkbox, _) => Focus::Export,
            (Focus::Export, _) => Focus::Cancel,
            (Focus::Cancel, _) => Focus::Directory,
        };
        self.focus.set(next);
    }

    fn prev_focus(&self) {
        let has_checkbox = matches!(self.mode, ExportMode::Results);
        let prev = match (self.focus.get(), has_checkbox) {
            (Focus::Directory, _) => Focus::Cancel,
            (Focus::Filename, _) => Focus::Directory,
            (Focus::Checkbox, _) => Focus::Filename,
            (Focus::Export, true) => Focus::Checkbox,
            (Focus::Export, false) => Focus::Filename,
            (Focus::Cancel, _) => Focus::Export,
        };
        self.focus.set(prev);
    }

    fn toggle_fetch_all(&self) {
        if matches!(self.mode, ExportMode::Results) {
            self.fetch_all.set(!self.fetch_all.get());
        }
    }

    fn export_enabled(&self) -> bool {
        !self.file_input.borrow().value().trim().is_empty()
    }

    fn build_path(&self) -> PathBuf {
        let dir_value = self.dir_input.borrow().value().trim().to_string();
        let file_value = self.file_input.borrow().value().trim().to_string();
        let dir_path = if dir_value.is_empty() {
            PathBuf::from(".")
        } else {
            expand_home(&dir_value)
        };
        if file_value.is_empty() {
            dir_path
        } else {
            dir_path.join(file_value)
        }
    }

    fn render_input_row(
        &self,
        frame: &mut Frame,
        area: Rect,
        label: &str,
        input: &FormInput,
        focused: bool,
        theme: &Theme,
    ) {
        let label_area = Rect::new(area.x, area.y, Self::LABEL_WIDTH, 1);
        let input_area = Rect::new(
            area.x + Self::LABEL_WIDTH + 1,
            area.y,
            area.width.saturating_sub(Self::LABEL_WIDTH + 1),
            1,
        );

        let label_style = if focused {
            Style::default()
                .fg(theme.accent())
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(theme.text_muted())
        };
        let label_line = Line::from(Span::styled(label, label_style));
        frame.render_widget(Paragraph::new(label_line), label_area);

        let (visible, cursor_pos) = input.visible_text(input_area.width as usize);
        let mut text = visible;
        let text_width = text.chars().count();
        if text_width < input_area.width as usize {
            text.push_str(&" ".repeat(input_area.width as usize - text_width));
        }
        let input_style = if focused {
            Style::default()
                .fg(theme.text())
                .add_modifier(Modifier::UNDERLINED)
        } else {
            Style::default().fg(theme.text())
        };
        frame.render_widget(Paragraph::new(text).style(input_style), input_area);

        if focused {
            frame.set_cursor_position((input_area.x + cursor_pos as u16, input_area.y));
        }
    }

    fn render_checkbox_row(&self, frame: &mut Frame, area: Rect, focused: bool, theme: &Theme) {
        let label_area = Rect::new(area.x, area.y, Self::LABEL_WIDTH, 1);
        frame.render_widget(Paragraph::new(""), label_area);
        let input_area = Rect::new(
            area.x + Self::LABEL_WIDTH + 1,
            area.y,
            area.width.saturating_sub(Self::LABEL_WIDTH + 1),
            1,
        );
        let checked = if self.fetch_all.get() { "[x]" } else { "[ ]" };
        let text = format!("{checked} Fetch all results before exporting");
        let style = if focused {
            Style::default()
                .fg(theme.accent())
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(theme.text())
        };
        frame.render_widget(Paragraph::new(text).style(style), input_area);
    }

    fn render_buttons(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let export_focused = self.focus.get() == Focus::Export;
        let cancel_focused = self.focus.get() == Focus::Cancel;
        let export_enabled = self.export_enabled();
        let export_style = if export_enabled {
            if export_focused {
                Style::default()
                    .bg(theme.accent())
                    .fg(theme.panel_bg())
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(theme.accent())
            }
        } else {
            Style::default().fg(theme.text_muted())
        };
        let cancel_style = if cancel_focused {
            Style::default()
                .bg(theme.border())
                .fg(theme.panel_bg())
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(theme.text_muted())
        };
        let export_button = Span::styled("[ Export ]", export_style);
        let cancel_button = Span::styled("[ Cancel ]", cancel_style);
        let buttons = Line::from(vec![export_button, Span::raw("  "), cancel_button]).centered();
        frame.render_widget(
            Paragraph::new(Text::from(buttons)).alignment(Alignment::Center),
            area,
        );
    }
}

impl crate::widgets::Widget for ExportPopup {
    fn inner(&self) -> &WidgetInner {
        &self.inner
    }

    fn help(&self) -> Option<&[help::Entry<'_>]> {
        Some(self.help_entries.as_slice())
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        fill_bg(frame.buffer_mut(), area, theme.panel_bg());
        let title = Line::styled(
            pad("Export", 1),
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
        let inner = block.inner(area).inner(Margin::new(1, 1));

        let mut rows = vec![Constraint::Length(1), Constraint::Length(1)];
        let mut checkbox_row = None;
        if matches!(self.mode, ExportMode::Results) {
            rows.push(Constraint::Length(1));
            rows.push(Constraint::Length(1));
            checkbox_row = Some(rows.len() - 1);
        }
        rows.push(Constraint::Length(2));
        rows.push(Constraint::Length(1));
        let layout = Layout::vertical(rows).split(inner);

        let dir_input = self.dir_input.borrow();
        self.render_input_row(
            frame,
            layout[0],
            "Directory",
            &dir_input,
            self.focus.get() == Focus::Directory,
            theme,
        );

        let file_input = self.file_input.borrow();
        self.render_input_row(
            frame,
            layout[1],
            "Filename",
            &file_input,
            self.focus.get() == Focus::Filename,
            theme,
        );

        if let Some(row) = checkbox_row {
            self.render_checkbox_row(
                frame,
                layout[row],
                self.focus.get() == Focus::Checkbox,
                theme,
            );
        }
        let button_row = layout.len().saturating_sub(1);
        self.render_buttons(frame, layout[button_row], theme);
    }

    fn handle_event(&self, ctx: crate::env::WidgetCtx, event: &crossterm::event::Event) -> bool {
        let Some(key) = event.as_key_press_event() else {
            return true;
        };

        match key.code {
            KeyCode::Esc => {
                ctx.dismiss_popup();
                ctx.invalidate();
                return true;
            }
            KeyCode::Tab | KeyCode::Down => {
                self.next_focus();
                ctx.invalidate();
                return true;
            }
            KeyCode::BackTab | KeyCode::Up => {
                self.prev_focus();
                ctx.invalidate();
                return true;
            }
            _ => {}
        }

        match self.focus.get() {
            Focus::Directory => {
                let mut input = self.dir_input.borrow_mut();
                if input.handle_key(&key) {
                    ctx.invalidate();
                    return true;
                }
            }
            Focus::Filename => {
                let mut input = self.file_input.borrow_mut();
                if input.handle_key(&key) {
                    ctx.invalidate();
                    return true;
                }
            }
            Focus::Checkbox => {
                if matches!(
                    key.code,
                    KeyCode::Char(' ') | KeyCode::Char('f') | KeyCode::Enter
                ) {
                    self.toggle_fetch_all();
                    ctx.invalidate();
                    return true;
                }
                if matches!(key.code, KeyCode::Left | KeyCode::Right) {
                    self.next_focus();
                    ctx.invalidate();
                    return true;
                }
            }
            Focus::Export | Focus::Cancel => {
                if matches!(key.code, KeyCode::Left | KeyCode::Right) {
                    let next = if self.focus.get() == Focus::Export {
                        Focus::Cancel
                    } else {
                        Focus::Export
                    };
                    self.focus.set(next);
                    ctx.invalidate();
                    return true;
                }
                if matches!(key.code, KeyCode::Enter) {
                    if self.focus.get() == Focus::Export && self.export_enabled() {
                        let path = self.build_path();
                        (self.on_confirm)(path, self.fetch_all.get());
                        ctx.dismiss_popup();
                        ctx.invalidate();
                        return true;
                    }
                    if self.focus.get() == Focus::Cancel {
                        ctx.dismiss_popup();
                        ctx.invalidate();
                        return true;
                    }
                }
            }
        }
        true
    }
}

impl Popup for ExportPopup {
    fn rect(&self, area: Rect) -> Rect {
        let content_height = if matches!(self.mode, ExportMode::Results) {
            7
        } else {
            5
        };
        let min_height = content_height as u16 + 4;
        let height = min_height.min(area.height.saturating_sub(2));
        let min_width = 44;
        let max_width = 72;
        let mut width = (area.width as f32 * 0.55) as u16;
        width = width.clamp(min_width, max_width);
        let max_available = area.width.saturating_sub(4);
        if max_available > 0 {
            width = width.min(max_available);
            if width < min_width {
                width = max_available;
            }
        } else {
            width = area.width;
        }
        let height = height.max(min_height.min(area.height.saturating_sub(4)));
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

fn split_path(path: &Path) -> (String, String) {
    let file = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| "export.json".to_string());
    let dir = path
        .parent()
        .map(abbreviate_home)
        .unwrap_or_else(|| "~".to_string());
    (dir, file)
}

fn expand_home(value: &str) -> PathBuf {
    let trimmed = value.trim();
    if trimmed == "~"
        && let Some(base) = BaseDirs::new()
    {
        return base.home_dir().to_path_buf();
    }
    if let Some(rest) = trimmed.strip_prefix("~/")
        && let Some(base) = BaseDirs::new()
    {
        return base.home_dir().join(rest);
    }
    PathBuf::from(trimmed)
}
