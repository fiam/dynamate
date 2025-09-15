use ratatui::style::Color;

pub struct Theme {
    primary: Color,
    secondary: Color,
    tertiary: Color,
    neutral: Color,
    neutral_variant: Color,
    error: Color,
}

pub const PRIMARY: Color = Color::Indexed(34); // Green3
pub const SECONDARY: Color = Color::Indexed(101); // OliveDrab3
pub const TERTIARY: Color = Color::Indexed(37); // DarkCyan
pub const ERROR: Color = Color::Indexed(196); // Red1
pub const NEUTRAL: Color = Color::Indexed(234); // Grey46
pub const NEUTRAL_VARIANT: Color = Color::Indexed(236); // Grey35

impl Theme {
    pub fn default() -> Self {
        Self {
            primary: PRIMARY,
            secondary: SECONDARY,
            tertiary: TERTIARY,
            neutral: NEUTRAL,
            neutral_variant: NEUTRAL_VARIANT,
            error: ERROR,
        }
    }

    pub fn primary(&self) -> Color {
        self.primary
    }

    pub fn secondary(&self) -> Color {
        self.secondary
    }

    pub fn tertiary(&self) -> Color {
        self.tertiary
    }

    pub fn neutral(&self) -> Color {
        self.neutral
    }

    pub fn neutral_variant(&self) -> Color {
        self.neutral_variant
    }

    pub fn error(&self) -> Color {
        self.error
    }
}
