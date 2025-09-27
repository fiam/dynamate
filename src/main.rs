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
use std::borrow::Cow;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use color_eyre::Result;
use crossterm::event::{Event, EventStream, KeyCode};
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Paragraph, Wrap};
use ratatui::{DefaultTerminal, Frame};
use tokio_stream::StreamExt;

use dynamate::aws;

mod env;
mod help;
mod logging;
mod subcommands;
mod util;
mod widgets;

use widgets::Widget;

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
    match cli.command {
        Some(Commands::ListTables { json }) => {
            let options = subcommands::list_tables::Options { json };
            subcommands::list_tables::command(&client, options).await?;
            Ok(())
        }
        None => {
            logging::initialize()?;
            App::default().run_tui(client.clone()).await?;
            Ok(())
        }
    }
}

struct App {
    env: env::Env,
    should_quit: bool,
    should_redraw: bool,
    widgets: Vec<Arc<dyn crate::widgets::Widget>>,
    popup: Option<Arc<dyn crate::widgets::Popup>>,
}

impl App {
    const FRAMES_PER_SECOND: f32 = 60.0;
    const HELP_WITHOUT_POPUP: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("h"),
            short: Cow::Borrowed("help"),
            long: Cow::Borrowed("Show help"),
        },
        help::Entry {
            keys: Cow::Borrowed("q/esc"),
            short: Cow::Borrowed("quit"),
            long: Cow::Borrowed("Quit dynamate"),
        },
    ];
    const HELP_WITH_POPUP: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("close"),
            long: Cow::Borrowed("Close popup"),
        },
        help::Entry {
            keys: Cow::Borrowed("q"),
            short: Cow::Borrowed("quit"),
            long: Cow::Borrowed("Quit dynamate"),
        },
    ];

    pub fn default() -> Self {
        App {
            env: env::Env::new(),
            should_quit: false,
            should_redraw: true,
            widgets: Vec::new(),
            popup: None,
        }
    }

    pub async fn run_tui(self, client: Arc<aws_sdk_dynamodb::Client>) -> Result<()> {
        let terminal = ratatui::init();
//        crossterm::execute!(std::io::stdout(), crossterm::event::EnableMouseCapture)?;
        let app_result = self.run(terminal, client).await;
//        crossterm::execute!(std::io::stdout(), crossterm::event::DisableMouseCapture)?;
        ratatui::restore();
        app_result
    }

    pub async fn run(
        mut self,
        mut terminal: DefaultTerminal,
        client: Arc<aws_sdk_dynamodb::Client>,
    ) -> Result<()> {
        let widget = Arc::new(widgets::QueryWidget::new(client, "test1"));
        widget.start(self.env.tx());

        self.widgets.push(widget);

        let period = Duration::from_secs_f32(1.0 / Self::FRAMES_PER_SECOND);
        let mut interval = tokio::time::interval(period);
        let mut events = EventStream::new();

        while !self.should_quit {
            tokio::select! {
                _ = interval.tick() => {
                    terminal.draw(|frame| self.render(frame))?;
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
                Some(msg) = self.env.rx().recv() => {
                    self.handle_msg(msg);
                    if self.should_redraw {
                        terminal.draw(|frame| self.render(frame))?;
                        self.should_redraw = false;
                    }
                },
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
        let app_help = if self.popup.is_some() {
            App::HELP_WITH_POPUP
        } else {
            App::HELP_WITHOUT_POPUP
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
        let all_help = self.make_help();
        let help_height = help::height(&all_help, frame.area());
        let layout = Layout::vertical([
            Constraint::Length(1),
            Constraint::Fill(1),
            Constraint::Length(help_height),
        ]);
        let [title_area, body_area, footer_area] = frame.area().layout(&layout);
        let title = Line::from("dynamate").centered().bold();
        frame.render_widget(title, title_area);
        if let Some(widget) = self.widgets.last() {
            widget.render(frame, body_area, &theme);
        }
        if let Some(popup) = self.popup.as_ref() {
            let popup_area = popup.rect(body_area);
            frame.render_widget(Clear, popup_area);
            popup.render(frame, popup_area, &theme);
        }
        help::render(&all_help, frame, footer_area, &theme);
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
        if let Some(popup) = self.popup.as_ref()
            && popup.handle_event(self.env.tx(), event)
        {
            return true;
        }

        if let Some(widget) = self.widgets.last()
            && widget.handle_event(self.env.tx(), event)
        {
            return true;
        }

        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Char('h') => {
                    self.popup = Some(Arc::new(help::Widget::new(self.make_help())));
                }
                KeyCode::Esc => {
                    if self.popup.is_some() {
                        self.popup = None;
                        self.should_redraw = true;
                    } else {
                        self.should_quit = true;
                    }
                }
                KeyCode::Char('q') => {
                    self.should_quit = true;
                }
                _ => return false,
            }
            return true;
        }
        return false;
    }

    fn handle_msg(&mut self, msg: env::Message) {
        match msg {
            env::Message::PushWidget(widget) => {
                self.widgets.push(widget);
            }
            env::Message::PopWidget => {
                self.widgets.pop();
            }
            env::Message::SetPopup(popup) => {
                if self.popup.is_some() {
                    panic!("popup is already set");
                }
                self.popup = Some(popup);
                self.should_redraw = true;
            }
            env::Message::DismissPopup => {
                if self.popup.is_none() {
                    panic!("popup is not set");
                }
                self.popup = None;
                self.should_redraw = true;
            }
            env::Message::Invalidate => {
                // Redraw the screen
                self.should_redraw = true;
            }
        }
    }
}
