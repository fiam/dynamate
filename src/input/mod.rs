use crossterm::event::KeyModifiers;

pub fn poll_modifiers(fallback: KeyModifiers) -> KeyModifiers {
    platform::poll_modifiers(fallback)
}

#[cfg(target_os = "macos")]
mod platform {
    use crossterm::event::KeyModifiers;

    type CGEventSourceStateID = u32;
    type CGKeyCode = u16;

    const KCG_EVENT_SOURCE_STATE_COMBINED_SESSION_STATE: CGEventSourceStateID = 0;

    const KEY_CTRL_LEFT: CGKeyCode = 59;
    const KEY_CTRL_RIGHT: CGKeyCode = 62;
    const KEY_SHIFT_LEFT: CGKeyCode = 56;
    const KEY_SHIFT_RIGHT: CGKeyCode = 60;
    const KEY_ALT_LEFT: CGKeyCode = 58;
    const KEY_ALT_RIGHT: CGKeyCode = 61;

    #[link(name = "ApplicationServices", kind = "framework")]
    unsafe extern "C" {
        fn CGEventSourceKeyState(state_id: CGEventSourceStateID, key: CGKeyCode) -> bool;
    }

    pub fn poll_modifiers(_fallback: KeyModifiers) -> KeyModifiers {
        let ctrl = unsafe {
            CGEventSourceKeyState(KCG_EVENT_SOURCE_STATE_COMBINED_SESSION_STATE, KEY_CTRL_LEFT)
                || CGEventSourceKeyState(
                    KCG_EVENT_SOURCE_STATE_COMBINED_SESSION_STATE,
                    KEY_CTRL_RIGHT,
                )
        };
        let shift = unsafe {
            CGEventSourceKeyState(
                KCG_EVENT_SOURCE_STATE_COMBINED_SESSION_STATE,
                KEY_SHIFT_LEFT,
            ) || CGEventSourceKeyState(
                KCG_EVENT_SOURCE_STATE_COMBINED_SESSION_STATE,
                KEY_SHIFT_RIGHT,
            )
        };
        let alt = unsafe {
            CGEventSourceKeyState(KCG_EVENT_SOURCE_STATE_COMBINED_SESSION_STATE, KEY_ALT_LEFT)
                || CGEventSourceKeyState(
                    KCG_EVENT_SOURCE_STATE_COMBINED_SESSION_STATE,
                    KEY_ALT_RIGHT,
                )
        };
        let mut mods = KeyModifiers::empty();
        if ctrl {
            mods.insert(KeyModifiers::CONTROL);
        }
        if shift {
            mods.insert(KeyModifiers::SHIFT);
        }
        if alt {
            mods.insert(KeyModifiers::ALT);
        }
        mods
    }
}

#[cfg(target_os = "windows")]
mod platform {
    use crossterm::event::KeyModifiers;

    const VK_CONTROL: i32 = 0x11;
    const VK_SHIFT: i32 = 0x10;
    const VK_MENU: i32 = 0x12;

    #[link(name = "user32")]
    extern "system" {
        fn GetAsyncKeyState(vkey: i32) -> i16;
    }

    fn is_down(vkey: i32) -> bool {
        unsafe { (GetAsyncKeyState(vkey) as u16 & 0x8000) != 0 }
    }

    pub fn poll_modifiers(_fallback: KeyModifiers) -> KeyModifiers {
        let mut mods = KeyModifiers::empty();
        if is_down(VK_CONTROL) {
            mods.insert(KeyModifiers::CONTROL);
        }
        if is_down(VK_SHIFT) {
            mods.insert(KeyModifiers::SHIFT);
        }
        if is_down(VK_MENU) {
            mods.insert(KeyModifiers::ALT);
        }
        mods
    }
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
mod platform {
    use crossterm::event::KeyModifiers;

    pub fn poll_modifiers(fallback: KeyModifiers) -> KeyModifiers {
        fallback
    }
}
