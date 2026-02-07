use std::{env, sync::OnceLock, time::Duration};

use ratatui::style::Color;

const LUMA_THRESHOLD: f32 = 0.6;
// Some terminals report noisy or transient luma right after startup; take a few
// samples and use the median to avoid a single bad read flipping the theme.
const LUMA_SAMPLES: usize = 5;
const LUMA_SAMPLE_DELAY: Duration = Duration::from_millis(20);

#[derive(Clone, Copy)]
pub struct Theme {
    bg: Color,
    panel_bg: Color,
    panel_bg_alt: Color,
    text: Color,
    text_muted: Color,
    accent: Color,
    accent_alt: Color,
    border: Color,
    selection_bg: Color,
    selection_fg: Color,
    success: Color,
    warning: Color,
    error: Color,
}

impl Theme {
    pub fn default() -> Self {
        static THEME: OnceLock<Theme> = OnceLock::new();
        *THEME.get_or_init(|| {
            if let Ok(value) = env::var("DYNAMATE_THEME") {
                if value.eq_ignore_ascii_case("light") {
                    return Self::light();
                }
                if value.eq_ignore_ascii_case("dark") {
                    return Self::dark();
                }
            }

            if let Some(luma) = detect_terminal_luma()
                && luma > LUMA_THRESHOLD
            {
                return Self::light();
            }

            Self::dark()
        })
    }

    pub fn dark() -> Self {
        Self {
            bg: Color::Rgb(12, 15, 20),
            panel_bg: Color::Rgb(17, 22, 29),
            panel_bg_alt: Color::Rgb(22, 27, 34),
            text: Color::Rgb(230, 237, 243),
            text_muted: Color::Rgb(154, 164, 178),
            accent: Color::Rgb(92, 207, 230),
            accent_alt: Color::Rgb(242, 177, 110),
            border: Color::Rgb(65, 76, 92),
            selection_bg: Color::Rgb(37, 50, 74),
            selection_fg: Color::Rgb(230, 237, 243),
            success: Color::Rgb(158, 206, 106),
            warning: Color::Rgb(224, 175, 104),
            error: Color::Rgb(247, 118, 142),
        }
    }

    pub fn light() -> Self {
        Self {
            bg: Color::Rgb(247, 247, 245),
            panel_bg: Color::Rgb(255, 255, 255),
            panel_bg_alt: Color::Rgb(240, 241, 243),
            text: Color::Rgb(31, 35, 40),
            text_muted: Color::Rgb(91, 97, 110),
            accent: Color::Rgb(31, 119, 180),
            accent_alt: Color::Rgb(180, 83, 9),
            border: Color::Rgb(156, 163, 175),
            selection_bg: Color::Rgb(219, 234, 254),
            selection_fg: Color::Rgb(15, 23, 42),
            success: Color::Rgb(47, 158, 68),
            warning: Color::Rgb(180, 83, 9),
            error: Color::Rgb(217, 72, 15),
        }
    }

    pub fn bg(&self) -> Color {
        self.bg
    }

    pub fn panel_bg(&self) -> Color {
        self.panel_bg
    }

    pub fn panel_bg_alt(&self) -> Color {
        self.panel_bg_alt
    }

    pub fn text(&self) -> Color {
        self.text
    }

    pub fn text_muted(&self) -> Color {
        self.text_muted
    }

    pub fn accent(&self) -> Color {
        self.accent
    }

    pub fn accent_alt(&self) -> Color {
        self.accent_alt
    }

    pub fn border(&self) -> Color {
        self.border
    }

    pub fn selection_bg(&self) -> Color {
        self.selection_bg
    }

    pub fn selection_fg(&self) -> Color {
        self.selection_fg
    }

    pub fn success(&self) -> Color {
        self.success
    }

    pub fn warning(&self) -> Color {
        self.warning
    }

    pub fn error(&self) -> Color {
        self.error
    }
}

fn detect_terminal_luma() -> Option<f32> {
    let mut samples = Vec::with_capacity(LUMA_SAMPLES);
    for attempt in 0..LUMA_SAMPLES {
        if let Ok(luma) = terminal_light::luma()
            && luma.is_finite()
        {
            samples.push(luma);
        }
        if attempt + 1 < LUMA_SAMPLES {
            std::thread::sleep(LUMA_SAMPLE_DELAY);
        }
    }

    if samples.is_empty() {
        return None;
    }

    Some(median_luma(&mut samples))
}

fn median_luma(samples: &mut [f32]) -> f32 {
    samples.sort_by(|a, b| a.total_cmp(b));
    let mid = samples.len() / 2;
    if samples.len().is_multiple_of(2) {
        (samples[mid - 1] + samples[mid]) / 2.0
    } else {
        samples[mid]
    }
}

#[cfg(test)]
mod tests {
    use super::median_luma;

    #[test]
    fn median_luma_odd() {
        let mut samples = [0.9_f32, 0.4_f32, 0.2_f32];
        let median = median_luma(&mut samples);
        assert!((median - 0.4).abs() < 1e-6);
    }

    #[test]
    fn median_luma_even() {
        let mut samples = [0.2_f32, 0.8_f32, 0.4_f32, 0.6_f32];
        let median = median_luma(&mut samples);
        assert!((median - 0.5).abs() < 1e-6);
    }
}
