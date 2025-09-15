use ratatui::{
    buffer::Buffer, layout::Rect, style::Style, widgets::{Block, Paragraph, Widget}, Frame
};

use crate::widgets::theme;

#[derive(Default)]
pub struct Input {
    input: String,
    is_active: bool,
}

impl Input {
    pub fn new() -> Self {
        Self {
            input: String::new(),
            is_active: false,
        }
    }

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
        // keep 2 for borders and 1 for cursor
        let width = area.width.max(3) - 3;
        let scroll = 0;
        // do this more elegantly
        let style = if self.is_active() {
            Style::default().fg(theme.secondary())
        } else {
            Style::default()
        };
        let block = Block::bordered().title("Query").style(style);
        let input = Paragraph::new(self.input.as_str())
            .scroll((0, scroll as u16))
            .block(block);
        input.render(area, frame.buffer_mut());

        // if self.input_mode == InputMode::Editing {
        //     // Ratatui hides the cursor unless it's explicitly set. Position the  cursor past the
        //     // end of the input text and one line down from the border to the input line
        //     let x = self.input.visual_cursor().max(scroll) - scroll + 1;
        //     frame.set_cursor_position((area.x + x as u16, area.y + 1))
        // }
    }

    fn handle_event(&mut self, evt: &crossterm::event::Event) -> bool {
        false
        //        self.input.handle_event(evt)
    }
}
