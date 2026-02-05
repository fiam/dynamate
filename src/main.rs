//! # [Ratatui] Async example
//!
//! This example demonstrates how to use Ratatui with widgets that fetch data asynchronously. It
//! uses the `octocrab` crate to fetch a list of pull requests from the GitHub API.
//!
//! <https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-fine-grained-personal-access-token>
//! <https://github.com/settings/tokens/new> to create a new token (select classic, and no scopes)
//!
//! This example does not cover message passing between threads, it only demonstrates how to manage
//! shared state between the main thread and a background task, which acts mostly as a one-shot
//! fetcher. For more complex scenarios, you may need to use channels or other synchronization
//! primitives.
//!
//! A simple app might have multiple widgets that fetch data from different sources, and each widget
//! would have its own background task to fetch the data. The main thread would then render the
//! widgets with the latest data.
//!
//! The latest version of this example is available in the [examples] folder in the repository.
//!
//! Please note that the examples are designed to be run against the `main` branch of the Github
//! repository. This means that you may not be able to compile with the latest release version on
//! crates.io, or the one that you have installed locally.
//!
//! See the [examples readme] for more information on finding examples that match the version of the
//! library you are using.
//!
//! [Ratatui]: https://github.com/ratatui/ratatui
//! [examples]: https://github.com/ratatui/ratatui/blob/main/examples
//! [examples readme]: https://github.com/ratatui/ratatui/blob/main/examples/README.md
use std::backtrace::Backtrace;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use color_eyre::Result;
use crossterm::event::{
    Event, EventStream, KeyCode, KeyEventKind, ModifierKeyCode, poll, read,
};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style, Stylize};
use ratatui::text::Line;
use ratatui::widgets::Clear;
use ratatui::widgets::{Block, BorderType};
use ratatui::{DefaultTerminal, Frame};
use tokio_stream::StreamExt;
use unicode_width::UnicodeWidthStr;

#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};

use dynamate::aws;

mod env;
mod help;
mod input;
mod logging;
mod subcommands;
mod util;
mod widgets;

use crate::env::{
    AppBus, AppBusRx, AppCommand, AppEvent, HelpStateEvent, Toast, ToastKind, WidgetEvent,
};
use crate::help::ModDisplay;
use crate::util::fill_bg;
use crate::widgets::theme::Theme;

#[derive(clap::Parser)]
#[command(
    name = "dynamate",
    version = "0.1.0",
    about = "DynamoDB swiss army knife",
    long_about = None
)]
struct Cli {
    /// Increase output verbosity (-v, -vv, etc.)
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Path to config file
    #[arg(long, global = true)]
    config: Option<String>,

    /// Endpoint URL for the DynamoDB service
    #[arg(long)]
    endpoint_url: Option<String>,

    /// Table name to open directly
    #[arg(short, long)]
    table: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(clap::Subcommand)]
enum Commands {
    ListTables {
        /// Output in JSON format
        #[arg(short, long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install aws-lc-rs provider");

    color_eyre::install()?;
    let cli = <Cli as clap::Parser>::parse();
    let client = Arc::new(aws::new_client(cli.endpoint_url.as_deref()).await?);
    aws::validate_connection(&client).await?;

    match cli.command {
        Some(Commands::ListTables { json }) => {
            let options = subcommands::list_tables::Options { json };
            subcommands::list_tables::command(&client, options).await?;
            Ok(())
        }
        None => {
            logging::initialize()?;
            App::default()
                .run_tui(client.clone(), cli.table.as_deref())
                .await?;
            Ok(())
        }
    }
}

struct App {
    bus: AppBus,
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<AppCommand>,
    event_rx: tokio::sync::mpsc::UnboundedReceiver<AppEvent>,
    should_quit: bool,
    should_redraw: bool,
    input_grace_until: Option<Instant>,
    widgets: Vec<Box<dyn crate::widgets::Widget>>,
    popup: Option<Box<dyn crate::widgets::Popup>>,
    toast: Option<ToastState>,
    modifiers: crossterm::event::KeyModifiers,
    help_mode: ModDisplay,
}

impl App {
    const FRAMES_PER_SECOND: f32 = 60.0;
    const HELP_WITHOUT_POPUP_BACK: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed(""),
            short: Cow::Borrowed(""),
            long: Cow::Borrowed(""),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^q")),
                short: Some(Cow::Borrowed("quit")),
                long: Some(Cow::Borrowed("Quit dynamate")),
            }),
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("h"),
            short: Cow::Borrowed("help"),
            long: Cow::Borrowed("Show help"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("back"),
            long: Cow::Borrowed("Back"),
            ctrl: None,
            shift: None,
            alt: None,
        },
    ];
    const HELP_WITHOUT_POPUP_NO_ESC: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed(""),
            short: Cow::Borrowed(""),
            long: Cow::Borrowed(""),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^q")),
                short: Some(Cow::Borrowed("quit")),
                long: Some(Cow::Borrowed("Quit dynamate")),
            }),
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("h"),
            short: Cow::Borrowed("help"),
            long: Cow::Borrowed("Show help"),
            ctrl: None,
            shift: None,
            alt: None,
        },
    ];
    const HELP_WITHOUT_POPUP_EXIT: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed(""),
            short: Cow::Borrowed(""),
            long: Cow::Borrowed(""),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^q")),
                short: Some(Cow::Borrowed("quit")),
                long: Some(Cow::Borrowed("Quit dynamate")),
            }),
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("h"),
            short: Cow::Borrowed("help"),
            long: Cow::Borrowed("Show help"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("exit"),
            long: Cow::Borrowed("Exit"),
            ctrl: None,
            shift: None,
            alt: None,
        },
    ];
    const HELP_WITH_POPUP: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed(""),
            short: Cow::Borrowed(""),
            long: Cow::Borrowed(""),
            ctrl: Some(help::Variant {
                keys: Some(Cow::Borrowed("^q")),
                short: Some(Cow::Borrowed("quit")),
                long: Some(Cow::Borrowed("Quit dynamate")),
            }),
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("h"),
            short: Cow::Borrowed("help"),
            long: Cow::Borrowed("Show help"),
            ctrl: None,
            shift: None,
            alt: None,
        },
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("close"),
            long: Cow::Borrowed("Close popup"),
            ctrl: None,
            shift: None,
            alt: None,
        },
    ];

    pub fn default() -> Self {
        let (bus, AppBusRx { cmd_rx, event_rx }) = AppBus::new();
        App {
            bus,
            cmd_rx,
            event_rx,
            should_quit: false,
            should_redraw: true,
            input_grace_until: None,
            widgets: Vec::new(),
            popup: None,
            toast: None,
            modifiers: crossterm::event::KeyModifiers::empty(),
            help_mode: ModDisplay::Both,
        }
    }

    pub async fn run_tui(
        self,
        client: Arc<aws_sdk_dynamodb::Client>,
        table_name: Option<&str>,
    ) -> Result<()> {
        let mut app = self;
        let terminal = ratatui::init();
        crossterm::execute!(std::io::stdout(), crossterm::event::EnableMouseCapture)?;

        app.help_mode = ModDisplay::Swap;
        drain_pending_input()?;
        app.input_grace_until = Some(Instant::now() + Duration::from_millis(250));

        let app_result = app.run(terminal, client, table_name).await;
        crossterm::execute!(std::io::stdout(), crossterm::event::DisableMouseCapture)?;
        ratatui::restore();
        app_result
    }

    pub async fn run(
        mut self,
        mut terminal: DefaultTerminal,
        client: Arc<aws_sdk_dynamodb::Client>,
        table_name: Option<&str>,
    ) -> Result<()> {
        let event_driven_render = env_flag("DYNAMATE_EVENT_DRIVEN_RENDER");
        let widget: Box<dyn crate::widgets::Widget> = match table_name {
            Some(name) => Box::new(widgets::QueryWidget::new(
                client.as_ref().clone(),
                name,
                env::WidgetId::app(),
            )),
            None => Box::new(widgets::TablePickerWidget::new(
                client.as_ref().clone(),
                env::WidgetId::app(),
            )),
        };
        let ctx = widget.inner().ctx(self.bus.clone());
        widget.start(ctx);

        self.widgets.push(widget);

        let period = Duration::from_secs_f32(1.0 / Self::FRAMES_PER_SECOND);
        let mut interval = tokio::time::interval(period);
        let mut events = EventStream::new();

        #[cfg(unix)]
        let mut sigint = signal(SignalKind::interrupt())?;
        #[cfg(unix)]
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigquit = signal(SignalKind::quit())?;

        #[cfg(unix)]
        {
            while !self.should_quit {
                tokio::select! {
                    _ = interval.tick() => {
                        self.prune_toast();
                        self.process_widget_self_events();
                        self.update_help_modifiers();
                        if !event_driven_render {
                            terminal.draw(|frame| self.render(frame))?;
                        } else if self.should_redraw {
                            terminal.draw(|frame| self.render(frame))?;
                            self.should_redraw = false;
                        }
                        // if self.should_redraw {
                        //     terminal.draw(|frame| self.render(frame))?;
                        //     self.should_redraw = false;
                        // }
                    },
                    Some(Ok(event)) = events.next() => {
                        if self.handle_event(&event) {
                            terminal.draw(|frame| self.render(frame))?;
                        }
                    },
                    Some(cmd) = self.cmd_rx.recv() => {
                        let force_redraw = matches!(cmd, AppCommand::ForceRedraw);
                        self.handle_cmd(cmd);
                        if self.should_redraw || force_redraw {
                            if force_redraw {
                                terminal.clear()?;
                            }
                            terminal.draw(|frame| self.render(frame))?;
                            self.should_redraw = false;
                        }
                    },
                    Some(event) = self.event_rx.recv() => {
                        self.dispatch_app_event(&event);
                    },
                    _ = sigint.recv() => {
                        self.should_quit = true;
                    },
                    _ = sigterm.recv() => {
                        self.should_quit = true;
                    },
                    _ = sigquit.recv() => {
                        eprintln!("SIGQUIT received; dumping backtrace (set RUST_BACKTRACE=full for more detail):");
                        eprintln!("{:?}", Backtrace::force_capture());
                    },
                }
            }
        }

        #[cfg(not(unix))]
        {
            while !self.should_quit {
                tokio::select! {
                    _ = interval.tick() => {
                        self.prune_toast();
                        self.process_widget_self_events();
                        self.update_help_modifiers();
                        if !event_driven_render {
                            terminal.draw(|frame| self.render(frame))?;
                        } else if self.should_redraw {
                            terminal.draw(|frame| self.render(frame))?;
                            self.should_redraw = false;
                        }
                        // if self.should_redraw {
                        //     terminal.draw(|frame| self.render(frame))?;
                        //     self.should_redraw = false;
                        // }
                    },
                    Some(Ok(event)) = events.next() => {
                        if self.handle_event(&event) {
                            terminal.draw(|frame| self.render(frame))?;
                        }
                    },
                    Some(cmd) = self.cmd_rx.recv() => {
                        let force_redraw = matches!(cmd, AppCommand::ForceRedraw);
                        self.handle_cmd(cmd);
                        if self.should_redraw || force_redraw {
                            if force_redraw {
                                terminal.clear()?;
                            }
                            terminal.draw(|frame| self.render(frame))?;
                            self.should_redraw = false;
                        }
                    },
                    Some(event) = self.event_rx.recv() => {
                        self.dispatch_app_event(&event);
                    },
                }
            }
        }
        Ok(())
    }

    fn make_help(&self) -> Vec<&help::Entry<'_>> {
        let help = if let Some(popup) = self.popup.as_ref() {
            popup.help()
        } else if let Some(widget) = self.widgets.last() {
            widget.help()
        } else {
            None
        };
        let popup_declares_esc = self.popup.as_ref().is_some_and(|popup| {
            popup
                .help()
                .is_some_and(|entries| entries.iter().any(entry_declares_esc))
        });
        let app_help = if self.popup.is_some() {
            if popup_declares_esc {
                App::HELP_WITHOUT_POPUP_NO_ESC
            } else {
                App::HELP_WITH_POPUP
            }
        } else if self
            .widgets
            .last()
            .is_some_and(|w| w.suppress_global_help())
        {
            &[]
        } else if self.widget_declares_esc() {
            App::HELP_WITHOUT_POPUP_NO_ESC
        } else if self.widgets.len() > 1 {
            App::HELP_WITHOUT_POPUP_BACK
        } else {
            App::HELP_WITHOUT_POPUP_EXIT
        };
        [help, Some(app_help)]
            .into_iter()
            .flatten()
            .flatten()
            .collect()
    }

    fn render(&self, frame: &mut Frame) {
        let start = Instant::now();
        let theme = Theme::default();
        let area = frame.area();
        let buf = frame.buffer_mut();
        fill_bg(buf, area, theme.bg());
        let all_help = self.make_help();
        let modifiers = self.modifiers;
        let help_mode = self.help_mode;
        let help_height = help::height(&all_help, frame.area(), modifiers, help_mode);
        let layout = Layout::vertical([
            Constraint::Length(1),
            Constraint::Fill(1),
            Constraint::Length(help_height),
        ]);
        let [title_area, body_area, footer_area] = frame.area().layout(&layout);
        let title = Line::styled(
            "dynamate",
            Style::default()
                .fg(theme.accent())
                .add_modifier(Modifier::BOLD),
        )
        .centered();
        frame.render_widget(title, title_area);
        if let Some(widget) = self.widgets.last() {
            let back_title = self
                .widgets
                .iter()
                .rev()
                .nth(1)
                .and_then(|w| w.navigation_title());
            let nav = widgets::NavContext { back_title };
            widget.render_with_nav(frame, body_area, &theme, &nav);
        }
        if let Some(popup) = self.popup.as_ref() {
            let popup_area = popup.rect(body_area);
            frame.render_widget(Clear, popup_area);
            popup.render_with_nav(frame, popup_area, &theme, &widgets::NavContext::default());
        }
        if self.popup.is_none()
            && let Some(toast) = self.toast.as_ref()
        {
            self.render_toast(frame, body_area, footer_area, &theme, toast);
        }
        help::render(&all_help, frame, footer_area, &theme, modifiers, help_mode);
        let duration = start.elapsed();
        // Render duration in red at the bottom right corner
        let duration_str = format!("{:.2?}", duration);
        let area = frame.area();
        let len = duration_str.len();
        let x = area.x + area.width.saturating_sub(len as u16 + 1);
        let y = area.y + area.height.saturating_sub(1);
        let duration_line = Line::from(duration_str).right_aligned().red();
        frame.render_widget(duration_line, Rect::new(x, y, len as u16, 1));
    }

    fn handle_event(&mut self, event: &Event) -> bool {
        if let Some(key) = event.as_key_press_event()
            && key
                .modifiers
                .contains(crossterm::event::KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('q'))
        {
            self.should_quit = true;
            return true;
        }
        if let Some(until) = self.input_grace_until {
            if Instant::now() < until {
                if event.as_key_event().is_some() {
                    return false;
                }
            } else {
                self.input_grace_until = None;
            }
        }

        if let Some(key) = event.as_key_event() {
            let mut updated = false;
            if let KeyCode::Modifier(modifier) = key.code {
                if let Some(flag) = modifier_flag(modifier) {
                    match key.kind {
                        KeyEventKind::Press | KeyEventKind::Repeat => {
                            if !self.modifiers.contains(flag) {
                                self.modifiers.insert(flag);
                                updated = true;
                            }
                        }
                        KeyEventKind::Release => {
                            if self.modifiers.contains(flag) {
                                self.modifiers.remove(flag);
                                updated = true;
                            }
                        }
                    }
                }
            } else if self.modifiers != key.modifiers {
                self.modifiers = key.modifiers;
                updated = true;
            }

            if updated {
                self.should_redraw = true;
                self.broadcast_help_state();
            }
        }

        if let Some(key) = event.as_key_press_event()
            && matches!(key.code, KeyCode::Esc)
            && self.popup.is_some()
        {
            self.popup = None;
            self.should_redraw = true;
            return true;
        }

        if let Some(popup) = self.popup.as_ref()
            && popup.handle_event(self.make_ctx(popup.as_ref()), event)
        {
            return true;
        }

        if let Some(widget) = self.widgets.last()
            && widget.handle_event(self.make_ctx(widget.as_ref()), event)
        {
            return true;
        }

        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Char('h') => {
                    if self
                        .widgets
                        .last()
                        .is_some_and(|w| w.suppress_global_help())
                    {
                        return true;
                    }
                    self.popup = Some(Box::new(help::Widget::new(
                        self.make_help(),
                        self.modifiers,
                        ModDisplay::Both,
                        self.widgets
                            .last()
                            .map(|w| w.id())
                            .unwrap_or_else(env::WidgetId::app),
                    )));
                }
                KeyCode::Esc => {
                    if self.popup.is_some() {
                        self.popup = None;
                        self.should_redraw = true;
                    } else if self.widget_declares_esc() {
                        return false;
                    } else if self.toast.is_some() {
                        self.toast = None;
                        self.should_redraw = true;
                    } else if self.widgets.len() > 1 {
                        self.widgets.pop();
                        self.should_redraw = true;
                    } else {
                        self.should_quit = true;
                    }
                }
                _ => return false,
            }
            return true;
        }
        false
    }

    fn widget_declares_esc(&self) -> bool {
        let Some(widget) = self.widgets.last() else {
            return false;
        };
        let Some(entries) = widget.help() else {
            return false;
        };
        entries.iter().any(entry_declares_esc)
    }

    fn make_ctx(&self, widget: &dyn crate::widgets::Widget) -> crate::env::WidgetCtx {
        widget.inner().ctx(self.bus.clone())
    }

    fn update_help_modifiers(&mut self) {
        let polled = input::poll_modifiers(self.modifiers);
        if polled != self.modifiers {
            self.modifiers = polled;
            self.should_redraw = true;
            self.broadcast_help_state();
        }
    }

    fn broadcast_help_state(&self) {
        let event = HelpStateEvent {
            modifiers: self.modifiers,
        };
        self.bus.broadcast(AppEvent::new(env::WidgetId::app(), event));
    }

    fn process_widget_self_events(&mut self) {
        for widget in &self.widgets {
            let ctx = self.make_ctx(widget.as_ref());
            for event in widget.inner().drain_self_events() {
                widget.on_self_event(ctx.clone(), &event);
            }
        }
        if let Some(popup) = self.popup.as_ref() {
            let ctx = self.make_ctx(popup.as_ref());
            for event in popup.inner().drain_self_events() {
                popup.on_self_event(ctx.clone(), &event);
            }
        }
    }

    fn dispatch_app_event(&mut self, event: &AppEvent) {
        if let Some(widget_event) = event.payload::<WidgetEvent>() {
            match widget_event {
                WidgetEvent::Created { id, parent } => {
                    tracing::debug!(
                        source = %event.source.as_str(),
                        widget_id = %id.as_str(),
                        parent = %parent.as_str(),
                        "widget_created"
                    );
                }
                WidgetEvent::Started { id } => {
                    tracing::debug!(
                        source = %event.source.as_str(),
                        widget_id = %id.as_str(),
                        "widget_started"
                    );
                }
                WidgetEvent::Closed { id } => {
                    tracing::debug!(
                        source = %event.source.as_str(),
                        widget_id = %id.as_str(),
                        "widget_closed"
                    );
                }
            }
        }
        for widget in &self.widgets {
            let ctx = self.make_ctx(widget.as_ref());
            widget.on_app_event(ctx, event);
        }
        if let Some(popup) = self.popup.as_ref() {
            let ctx = self.make_ctx(popup.as_ref());
            popup.on_app_event(ctx, event);
        }
    }

    fn handle_cmd(&mut self, cmd: AppCommand) {
        match cmd {
            AppCommand::PushWidget(widget) => {
                let ctx = self.make_ctx(widget.as_ref());
                ctx.emit_self(WidgetEvent::Started { id: widget.id() });
                ctx.broadcast_event(WidgetEvent::Created {
                    id: widget.id(),
                    parent: ctx.parent.clone(),
                });
                widget.start(ctx);
                self.widgets.push(widget);
                self.should_redraw = true;
            }
            AppCommand::PopWidget => {
                let popped = self.widgets.pop();
                if let Some(widget) = popped.as_ref() {
                    let ctx = self.make_ctx(widget.as_ref());
                    ctx.broadcast_event(WidgetEvent::Closed { id: widget.id() });
                }
                if self.widgets.is_empty() {
                    self.should_quit = true;
                } else {
                    self.should_redraw = true;
                }
            }
            AppCommand::SetPopup(popup) => {
                if self.popup.is_some() {
                    panic!("popup is already set");
                }
                self.popup = Some(popup);
                self.should_redraw = true;
            }
            AppCommand::DismissPopup => {
                if self.popup.is_none() {
                    panic!("popup is not set");
                }
                self.popup = None;
                self.should_redraw = true;
            }
            AppCommand::ShowToast(toast) => {
                self.toast = Some(ToastState::from(toast));
                self.should_redraw = true;
            }
            AppCommand::Invalidate => {
                self.should_redraw = true;
            }
            AppCommand::ForceRedraw => {
                self.should_redraw = true;
            }
        }
    }

    fn render_toast(
        &self,
        frame: &mut Frame,
        body_area: Rect,
        footer_area: Rect,
        theme: &Theme,
        toast: &ToastState,
    ) {
        let message = toast.message.as_str();
        let text_width = message.width() as u16;
        let width = (text_width + 6)
            .min(body_area.width.saturating_sub(2))
            .max(20);
        let height = 3u16;
        let x = body_area.x + body_area.width.saturating_sub(width + 1);
        let y = footer_area.y.saturating_sub(height + 1);
        let area = Rect::new(x, y, width, height);

        let color = match toast.kind {
            ToastKind::Info => theme.accent(),
            ToastKind::Warning => theme.warning(),
            ToastKind::Error => theme.error(),
        };
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(color))
            .style(Style::default().bg(theme.panel_bg()).fg(theme.text()));
        let text = Line::styled(message, Style::default().fg(theme.text()));
        frame.render_widget(Clear, area);
        frame.render_widget(block, area);
        let text_area = Rect::new(area.x + 2, area.y + 1, area.width - 4, 1);
        frame.render_widget(text, text_area);
    }

    fn prune_toast(&mut self) {
        if let Some(toast) = self.toast.as_ref()
            && toast.expires_at <= Instant::now()
        {
            self.toast = None;
            self.should_redraw = true;
        }
    }
}

fn drain_pending_input() -> Result<()> {
    let mut drained = 0;
    while poll(Duration::from_millis(0))? {
        let _ = read()?;
        drained += 1;
        if drained > 256 {
            break;
        }
    }
    Ok(())
}

fn env_flag(name: &str) -> bool {
    match std::env::var(name) {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"),
        Err(_) => false,
    }
}

#[derive(Debug, Clone)]
struct ToastState {
    message: String,
    kind: ToastKind,
    expires_at: Instant,
}

impl ToastState {
    fn from(toast: Toast) -> Self {
        Self {
            message: toast.message,
            kind: toast.kind,
            expires_at: Instant::now() + toast.duration,
        }
    }
}

fn modifier_flag(modifier: ModifierKeyCode) -> Option<crossterm::event::KeyModifiers> {
    use crossterm::event::KeyModifiers;
    match modifier {
        ModifierKeyCode::LeftControl | ModifierKeyCode::RightControl => Some(KeyModifiers::CONTROL),
        ModifierKeyCode::LeftShift
        | ModifierKeyCode::RightShift
        | ModifierKeyCode::IsoLevel3Shift
        | ModifierKeyCode::IsoLevel5Shift => Some(KeyModifiers::SHIFT),
        ModifierKeyCode::LeftAlt | ModifierKeyCode::RightAlt => Some(KeyModifiers::ALT),
        _ => None,
    }
}

fn entry_declares_esc(entry: &help::Entry<'_>) -> bool {
    if entry.keys.to_ascii_lowercase().contains("esc") {
        return true;
    }
    for variant in [
        entry.ctrl.as_ref(),
        entry.shift.as_ref(),
        entry.alt.as_ref(),
    ] {
        if let Some(variant) = variant
            && let Some(keys) = variant.keys.as_ref()
            && keys.to_ascii_lowercase().contains("esc")
        {
            return true;
        }
    }
    false
}
