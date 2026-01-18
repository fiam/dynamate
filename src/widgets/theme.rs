use ratatui::style::Color;

pub struct Theme {
    primary: Color,
    secondary: Color,
    neutral: Color,
    neutral_variant: Color,
}

pub const PRIMARY: Color = Color::Indexed(34); // Green3
pub const SECONDARY: Color = Color::Indexed(101); // OliveDrab3
pub const NEUTRAL: Color = Color::Indexed(234); // Grey46
pub const NEUTRAL_VARIANT: Color = Color::Indexed(236); // Grey35

impl Theme {
    pub fn default() -> Self {
        Self {
            primary: PRIMARY,
            secondary: SECONDARY,
            neutral: NEUTRAL,
            neutral_variant: NEUTRAL_VARIANT,
        }
    }

    pub fn primary(&self) -> Color {
        self.primary
    }

    pub fn secondary(&self) -> Color {
        self.secondary
    }

    pub fn neutral(&self) -> Color {
        self.neutral
    }

    pub fn neutral_variant(&self) -> Color {
        self.neutral_variant
    }
}
