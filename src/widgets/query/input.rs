use crossterm::event;
use ratatui::{
    Frame,
    layout::{Position, Rect},
    style::Style,
    widgets::{Block, Paragraph, Widget},
};

use crate::widgets::theme;

#[derive(Default)]
pub struct Input {
    input: String,
    character_index: usize,
    is_active: bool,
}

impl Input {
    pub fn value(&self) -> &str {
        &self.input
    }

    pub fn is_active(&self) -> bool {
        self.is_active
    }

    pub fn set_active(&mut self, active: bool) {
        self.is_active = active;
    }

    pub fn toggle_active(&mut self) {
        self.set_active(!self.is_active());
    }

    pub fn render(&self, frame: &mut Frame, area: Rect, theme: &theme::Theme) {
        let scroll = 0;
        let border = if self.is_active() {
            theme.accent()
        } else {
            theme.border()
        };
        let block = Block::bordered()
            .title("Query")
            .style(Style::default().bg(theme.panel_bg_alt()).fg(theme.text()))
            .border_style(Style::default().fg(border));
        let input_area = area;
        let input = Paragraph::new(self.input.as_str())
            .style(Style::default().fg(theme.text()))
            .scroll((0, scroll as u16))
            .block(block);
        input.render(area, frame.buffer_mut());

        if self.is_active() {
            frame.set_cursor_position(Position::new(
                // Draw the cursor at the current position in the input field.
                // This position can be controlled via the left and right arrow key
                input_area.x + self.character_index as u16 + 1,
                // Move one line down, from the border to the input line
                input_area.y + 1,
            ));
        }
    }

    pub fn handle_event(&mut self, evt: &event::Event) -> bool {
        if !self.is_active() {
            return false;
        }
        if let Some(key) = evt.as_key_press_event() {
            match key.code {
                event::KeyCode::Char('a')
                    if key.modifiers.contains(event::KeyModifiers::CONTROL) =>
                {
                    self.character_index = 0;
                }
                event::KeyCode::Char('e')
                    if key.modifiers.contains(event::KeyModifiers::CONTROL) =>
                {
                    self.character_index = self.input.len();
                }
                event::KeyCode::Char(c) => {
                    self.input.insert(self.character_index, c);
                    self.character_index += 1;
                }
                event::KeyCode::Backspace => {
                    if self.character_index > 0 && !self.input.is_empty() {
                        self.input.remove(self.character_index - 1);
                        self.character_index -= 1;
                    }
                }
                event::KeyCode::Delete => {
                    if self.character_index < self.input.len() && !self.input.is_empty() {
                        self.input.remove(self.character_index);
                    }
                }
                event::KeyCode::Left => {
                    if self.character_index > 0 {
                        self.character_index -= 1;
                    }
                }
                event::KeyCode::Right => {
                    if self.character_index < self.input.len() {
                        self.character_index += 1;
                    }
                }
                event::KeyCode::Home => {
                    self.character_index = 0;
                }
                event::KeyCode::End => {
                    self.character_index = self.input.len();
                }
                _ => {
                    return false;
                }
            }
            return true;
        }
        false
        //        self.input.handle_event(evt)
    }
}
