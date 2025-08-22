use std::sync::{Arc, RwLock};

use crossterm::event::{Event, KeyCode};
use octocrab::{
    Page,
    params::{Direction, pulls::Sort},
};
use ratatui::{
    Frame,
    buffer::Buffer,
    layout::{Constraint, Rect},
    style::Style,
    text::Line,
    widgets::{Block, HighlightSpacing, Row, StatefulWidget, Table, TableState},
};

/// A widget that displays a list of pull requests.
///
/// This is an async widget that fetches the list of pull requests from the GitHub API. It contains
/// an inner `Arc<RwLock<PullRequestListState>>` that holds the state of the widget. Cloning the
/// widget will clone the Arc, so you can pass it around to other threads, and this is used to spawn
/// a background task to fetch the pull requests.
#[derive(Debug, Clone, Default)]
pub struct PullRequestListWidget {
    state: Arc<RwLock<PullRequestListState>>,
}

#[derive(Debug, Default)]
struct PullRequestListState {
    pull_requests: Vec<PullRequest>,
    loading_state: LoadingState,
    table_state: TableState,
}

#[derive(Debug, Clone)]
struct PullRequest {
    id: String,
    title: String,
    url: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum LoadingState {
    #[default]
    Idle,
    Loading,
    Loaded,
    Error(String),
}

impl crate::widgets::Widget for PullRequestListWidget {
    fn start(&self) {
        self.run();
    }

    fn render(&self, frame: &mut Frame, area: Rect) {
        frame.render_widget(self, area);
    }

    fn handle_event(&self, event: &Event) -> bool {
        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Char('j') | KeyCode::Down => self.scroll_down(),
                KeyCode::Char('k') | KeyCode::Up => self.scroll_up(),
                _ => {
                    return false; // not handled
                }
            }
            return true;
        }
        false
    }
}

impl PullRequestListWidget {
    /// Start fetching the pull requests in the background.
    ///
    /// This method spawns a background task that fetches the pull requests from the GitHub API.
    /// The result of the fetch is then passed to the `on_load` or `on_err` methods.
    fn run(&self) {
        let this: PullRequestListWidget = self.clone(); // clone the widget to pass to the background task
        tokio::spawn(this.fetch_pulls());
    }

    async fn fetch_pulls(self) {
        // this runs once, but you could also run this in a loop, using a channel that accepts
        // messages to refresh on demand, or with an interval timer to refresh every N seconds
        self.set_loading_state(LoadingState::Loading);
        match octocrab::instance()
            .pulls("ratatui", "ratatui")
            .list()
            .sort(Sort::Updated)
            .direction(Direction::Descending)
            .send()
            .await
        {
            Ok(page) => self.on_load(&page),
            Err(err) => self.on_err(&err),
        }
    }
    fn on_load(&self, page: &Page<OctoPullRequest>) {
        let prs = page.items.iter().map(Into::into);
        let mut state = self.state.write().unwrap();
        state.loading_state = LoadingState::Loaded;
        state.pull_requests.extend(prs);
        if !state.pull_requests.is_empty() {
            state.table_state.select(Some(0));
        }
    }

    fn on_err(&self, err: &octocrab::Error) {
        self.set_loading_state(LoadingState::Error(err.to_string()));
    }

    fn set_loading_state(&self, state: LoadingState) {
        self.state.write().unwrap().loading_state = state;
    }

    fn scroll_down(&self) {
        self.state.write().unwrap().table_state.scroll_down_by(1);
    }

    fn scroll_up(&self) {
        self.state.write().unwrap().table_state.scroll_up_by(1);
    }
}

type OctoPullRequest = octocrab::models::pulls::PullRequest;

impl From<&OctoPullRequest> for PullRequest {
    fn from(pr: &OctoPullRequest) -> Self {
        Self {
            id: pr.number.to_string(),
            title: pr.title.as_ref().unwrap().to_string(),
            url: pr
                .html_url
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
        }
    }
}

impl ratatui::widgets::Widget for &PullRequestListWidget {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let mut state = self.state.write().unwrap();

        // a block with a right aligned title with the loading state on the right
        let loading_state = Line::from(format!("{:?}", state.loading_state)).right_aligned();
        let block = Block::bordered()
            .title("Pull Requests")
            .title(loading_state)
            .title_bottom("j/k to scroll, q to quit");

        // a table with the list of pull requests
        let rows = state.pull_requests.iter();
        let widths = [
            Constraint::Length(5),
            Constraint::Fill(1),
            Constraint::Max(49),
        ];
        let table = Table::new(rows, widths)
            .block(block)
            .highlight_spacing(HighlightSpacing::Always)
            .highlight_symbol(">>")
            .row_highlight_style(Style::new().on_blue());

        StatefulWidget::render(table, area, buf, &mut state.table_state);
    }
}

impl From<&PullRequest> for Row<'_> {
    fn from(pr: &PullRequest) -> Self {
        let pr = pr.clone();
        Row::new(vec![pr.id, pr.title, pr.url])
    }
}
