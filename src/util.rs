use std::{env, path::Path};

use directories::BaseDirs;

use ratatui::{buffer::Buffer, layout::Rect, style::Color};

pub fn fill_bg(buf: &mut Buffer, area: Rect, color: Color) {
    for x in area.left()..area.right() {
        for y in area.top()..area.bottom() {
            buf[(x, y)].set_bg(color);
        }
    }
}

pub fn pad<S: AsRef<str>>(s: S, pad: usize) -> String {
    let s = s.as_ref();
    let mut out = String::with_capacity(s.len() + pad * 2);
    for _ in 0..pad {
        out.push(' ');
    }
    out.push_str(s);
    for _ in 0..pad {
        out.push(' ');
    }
    out
}

pub fn env_flag(name: &str) -> bool {
    match env::var(name) {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"),
        Err(_) => false,
    }
}

pub fn abbreviate_home(path: &Path) -> String {
    let Some(base_dirs) = BaseDirs::new() else {
        return path.display().to_string();
    };
    let home = base_dirs.home_dir();
    if let Ok(rest) = path.strip_prefix(home) {
        if rest.as_os_str().is_empty() {
            "~".to_string()
        } else {
            format!("~/{}", rest.display())
        }
    } else {
        path.display().to_string()
    }
}
