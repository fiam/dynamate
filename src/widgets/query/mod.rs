use std::{
    borrow::Cow,
    cmp::{max, min},
    collections::{HashMap, HashSet},
    sync::Arc,
};

use aws_sdk_dynamodb::types::{AttributeValue, TableDescription};
use crossterm::event::{Event, KeyCode};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::Style,
    text::Line,
    widgets::{Block, HighlightSpacing, Row, StatefulWidget, Table, TableState},
};
use tokio::sync::OnceCell;

use item_keys::ItemKeys;
use keys_widget::KeysWidget;

use crate::{
    help,
    util::pad,
    widgets::{EnvHandle, theme::Theme},
};
use dynamate::{
    dynamodb::{DynamoDbRequest, Kind, Output, ScanBuilder, execute},
    expr::parse_dynamo_expression,
};

mod input;
mod item_keys;
mod keys_widget;

#[derive(Clone)]
pub struct QueryWidget {
    client: Arc<aws_sdk_dynamodb::Client>,
    table_name: String,
    sync_state: Arc<std::sync::RwLock<QuerySyncState>>,
    table_desc: Arc<OnceCell<Arc<TableDescription>>>,
}

#[derive(Default)]
struct QuerySyncState {
    input: input::Input,
    loading_state: LoadingState,
    query_output: Option<Output>,
    items: Vec<Item>,
    item_keys: Arc<item_keys::ItemKeys>,
    table_state: TableState,
}

#[derive(Debug, Clone)]
struct Item(HashMap<String, AttributeValue>, Arc<ItemKeys>);

impl Item {
    fn value(&self, key: &str) -> String {
        self.0
            .get(key)
            .map(|val| {
                if let Ok(v) = val.as_s() {
                    v.clone()
                } else if let Ok(v) = val.as_n() {
                    v.clone()
                } else if let Ok(v) = val.as_bool() {
                    v.to_string()
                } else {
                    format!("{val:?}")
                }
            })
            .unwrap_or_default()
    }

    fn value_size(&self, key: &str) -> usize {
        self.value(key).len()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum LoadingState {
    #[default]
    Idle,
    Loading,
    Loaded,
    Error(String),
}

impl crate::widgets::Widget for QueryWidget {
    fn start(&self, env: EnvHandle) {
        let this: QueryWidget = self.clone();
        tokio::spawn(this.load(env));
    }

    fn render(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let mut state = self.sync_state.write().unwrap();

        let layout = Layout::vertical([Constraint::Length(3), Constraint::Fill(1)]);
        let [query_area, results_area] = area.layout(&layout);

        state.input.render(frame, query_area, theme);

        let keys_view = state.item_keys.sorted();
        let header = Row::new(
            keys_view
                .as_slice()
                .iter()
                .map(|key| Line::from(key.clone())),
        )
        .style(Style::new().bold());

        let items = &state.items;
        let widths: Vec<Constraint> = keys_view
            .as_slice()
            .iter()
            .map(|key| {
                let max_value = items
                    .iter()
                    .map(|item| item.value_size(key))
                    .max()
                    .unwrap_or(0);
                let key_size = key.len() + 2;
                Constraint::Min(max(max_value, key_size) as u16)
            })
            .collect();

        drop(keys_view);

        // maximum rows is the area height, minus 2 for the the top and bottom borders,
        // minus 1 for the header
        let max_rows = (results_area.height - 2 - 1) as usize;
        let total = state.items.len();
        let first_item = state.table_state.offset() + 1;
        let last_item = min(first_item + max_rows, total);

        // a block with a right aligned title with the loading state on the right
        let (title, title_bottom) = match &state.loading_state {
            LoadingState::Idle | LoadingState::Loaded => (
                format!("Results{}", output_info(state.query_output.as_ref())),
                pad(
                    format!("{} results, showing {}-{}", total, first_item, last_item),
                    2,
                ),
            ),
            LoadingState::Loading => ("Loading".to_string(), "".to_string()),
            LoadingState::Error(err) => (format!("Error: {err}"), "".to_string()),
        };

        let block = Block::bordered()
            .title_top(title)
            .title_bottom(Line::styled(
                title_bottom,
                Style::default().fg(theme.neutral_variant()),
            ));

        if state.table_state.selected().is_none() && !state.items.is_empty() {
            state.table_state.select(Some(0));
        }

        let table = Table::new(&state.items, widths)
            .block(block)
            .header(header)
            .highlight_spacing(HighlightSpacing::Always)
            .highlight_symbol(">>")
            .row_highlight_style(Style::default().bg(theme.secondary()));

        StatefulWidget::render(
            table,
            results_area,
            frame.buffer_mut(),
            &mut state.table_state,
        );
    }

    fn handle_event(&self, env: EnvHandle, event: &Event) -> bool {
        let input_is_active = self.sync_state.read().unwrap().input.is_active();
        if input_is_active && self.sync_state.write().unwrap().input.handle_event(event) {
            return true;
        }
        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Tab | KeyCode::BackTab => {
                    self.sync_state.write().unwrap().input.toggle_active()
                }
                KeyCode::Esc if input_is_active => {
                    self.sync_state.write().unwrap().input.toggle_active()
                }
                KeyCode::Enter if input_is_active => {
                    let mut state = self.sync_state.write().unwrap();
                    self.start_query(Some(state.input.value()), env.clone());
                    state.input.toggle_active();
                }
                KeyCode::Char('j') | KeyCode::Down => self.scroll_down(),
                KeyCode::Char('k') | KeyCode::Up => self.scroll_up(),
                KeyCode::Char('f') => {
                    let state = self.sync_state.read().unwrap();
                    let keys = state
                        .item_keys
                        .sorted()
                        .iter()
                        .map(|k| keys_widget::Key {
                            name: k.clone(),
                            hidden: state.item_keys.is_hidden(k),
                        })
                        .collect::<Vec<_>>();
                    let item_keys = state.item_keys.clone();
                    let popup = Arc::new(KeysWidget::new(&keys, move |ev| match ev {
                        keys_widget::Event::KeyHidden(name) => {
                            item_keys.hide(&name);
                        }
                        keys_widget::Event::KeyUnhidden(name) => {
                            item_keys.unhide(&name);
                        }
                    }));
                    env.set_popup(popup);
                }
                _ => {
                    return false; // not handled
                }
            }
            return true;
        }

        if let Some(mouse) = event.as_mouse_event() {
            match mouse.kind {
                crossterm::event::MouseEventKind::ScrollUp => self.scroll_up(),
                crossterm::event::MouseEventKind::ScrollDown => self.scroll_down(),
                _ => return false, // not handled
            }
        }

        false
    }

    fn help(&self) -> Option<&[help::Entry<'_>]> {
        Some(Self::HELP)
    }
}

impl QueryWidget {
    const HELP: &'static [help::Entry<'static>] = &[help::Entry {
        keys: Cow::Borrowed("f"),
        short: Cow::Borrowed("fields"),
        long: Cow::Borrowed("Enable/disable fields"),
    }];
    pub fn new(client: Arc<aws_sdk_dynamodb::Client>, table_name: &str) -> Self {
        Self {
            client,
            table_name: table_name.to_string(),
            sync_state: Arc::new(std::sync::RwLock::new(QuerySyncState::default())),
            table_desc: Arc::new(OnceCell::new()),
        }
    }

    async fn load(self, env: EnvHandle) {
        self.start_query(None, env);
    }

    fn set_loading_state(&self, state: LoadingState) {
        self.sync_state.write().unwrap().loading_state = state;
    }

    async fn table_description(&self) -> Result<Arc<TableDescription>, String> {
        let arc_ref = self
            .table_desc
            .get_or_try_init(|| async {
                let out = self
                    .client
                    .describe_table()
                    .table_name(self.table_name.clone())
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;

                let table = out
                    .table()
                    .ok_or_else(|| "DescribeTable: missing table".to_string())?;
                Ok::<Arc<TableDescription>, String>(Arc::new(table.clone()))
            })
            .await?;

        Ok(arc_ref.clone())
    }

    fn scroll_down(&self) {
        self.sync_state
            .write()
            .unwrap()
            .table_state
            .scroll_down_by(1);
    }

    fn scroll_up(&self) {
        self.sync_state.write().unwrap().table_state.scroll_up_by(1);
    }

    async fn create_request(&self, query: Option<&str>) -> Result<DynamoDbRequest, String> {
        let query = query.unwrap_or("").trim();
        if query.is_empty() {
            return Ok(DynamoDbRequest::Scan(ScanBuilder::new()));
        }
        let expr = parse_dynamo_expression(query).map_err(|e| e.to_string())?;
        let table_desc = self.table_description().await.map_err(|e| e.to_string())?;
        Ok(DynamoDbRequest::from_expression_and_table(
            &expr,
            &table_desc,
        ))
    }

    fn start_query(&self, query: Option<&str>, env: EnvHandle) {
        let this: QueryWidget = self.clone();
        let query = query.map(|q| q.to_string());
        tokio::spawn(async move {
            this.set_loading_state(LoadingState::Loading);
            env.invalidate();
            match this.create_request(query.as_deref()).await {
                Ok(request) => {
                    match execute(this.client.clone(), &this.table_name, &request).await {
                        Ok(query_output) => {
                            this.process_query_output(query_output).await;
                            this.set_loading_state(LoadingState::Loaded);
                        }
                        Err(e) => {
                            this.set_loading_state(LoadingState::Error(e.to_string()));
                        }
                    };
                }
                Err(e) => {
                    this.set_loading_state(LoadingState::Error(e));
                }
            }
            env.invalidate();
        });
    }

    async fn process_query_output(&self, output: Output) {
        let mut item_keys = HashSet::new();
        let shared_item_keys = self.sync_state.read().unwrap().item_keys.clone();

        let items = output.items();
        let new_items: Vec<Item> = items
            .iter()
            .map(|item| {
                item_keys.extend(item.keys().cloned());
                Item(item.clone(), shared_item_keys.clone())
            })
            .collect();

        // Get table description before acquiring the write lock
        let table_desc = self.table_description().await.ok();

        let mut state = self.sync_state.write().unwrap();
        // Clear previous results
        state.items.clear();
        state.items.extend(new_items);

        // Update item keys with table description
        if let Some(table_desc) = table_desc {
            state.item_keys.extend(item_keys, &table_desc);
        }

        state.query_output = Some(output);
    }
}

impl<'a> From<&Item> for Row<'a> {
    fn from(item: &Item) -> Self {
        let mut parts = Vec::new();
        let view = item.1.sorted();
        for key in view.as_slice() {
            parts.push(item.value(key));
        }
        Row::new(parts)
    }
}

fn output_info(output: Option<&Output>) -> String {
    match output.map(|o| o.kind()) {
        Some(Kind::Scan) => " (Scan)".to_string(),
        Some(Kind::Query) => " (Query)".to_string(),
        Some(Kind::QueryGSI(index_name)) => {
            format!(" (Query GSI: {})", index_name)
        }
        Some(Kind::QueryLSI(index_name)) => {
            format!(" (Query LSI: {})", index_name)
        }
        None => "".to_string(),
    }
}
