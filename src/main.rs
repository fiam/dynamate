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
use std::sync::Arc;
use std::time::Duration;

use color_eyre::Result;
use crossterm::event::{Event, EventStream, KeyCode};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::Stylize;
use ratatui::text::Line;
use ratatui::{DefaultTerminal, Frame};
use tokio_stream::StreamExt;

use dynamate::aws;

mod subcommands;
mod widgets;

use widgets::Widget;

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
        None => App::default().run_tui(client.clone()).await,
    }
}

#[derive(Default)]
struct App {
    should_quit: bool,
    widgets: Vec<Arc<dyn crate::widgets::Widget>>,
}

impl App {
    const FRAMES_PER_SECOND: f32 = 60.0;

    pub async fn run_tui(self, client: Arc<aws_sdk_dynamodb::Client>) -> Result<()> {
        let terminal = ratatui::init();
        let app_result = self.run(terminal, client).await;
        ratatui::restore();
        app_result
    }

    pub async fn run(
        mut self,
        mut terminal: DefaultTerminal,
        client: Arc<aws_sdk_dynamodb::Client>,
    ) -> Result<()> {
        let widget = Arc::new(widgets::QueryWidget::new(client, "test1"));
        widget.start();
        self.widgets.push(widget);

        let period = Duration::from_secs_f32(1.0 / Self::FRAMES_PER_SECOND);
        let mut interval = tokio::time::interval(period);
        let mut events = EventStream::new();

        while !self.should_quit {
            tokio::select! {
                _ = interval.tick() => { terminal.draw(|frame| self.render(frame))?; },
                Some(Ok(event)) = events.next() => self.handle_event(&event),
            }
        }
        Ok(())
    }

    fn render(&self, frame: &mut Frame) {
        let layout = Layout::vertical([Constraint::Length(1), Constraint::Fill(1)]);
        let [title_area, body_area] = frame.area().layout(&layout);
        let title = Line::from("Ratatui async example").centered().bold();
        frame.render_widget(title, title_area);
        if let Some(widget) = self.widgets.last() {
            widget.render(frame, body_area);
        }
    }

    fn handle_event(&mut self, event: &Event) {
        if let Some(widget) = self.widgets.last() {
            if widget.handle_event(event) {
                return;
            }
        }
        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
                _ => {}
            }
        }
    }
}
