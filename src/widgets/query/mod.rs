use std::{
    borrow::Cow,
    cmp::{max, min},
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use aws_sdk_dynamodb::{
    operation::{query::Query, scan::ScanOutput},
    types::{AttributeValue, TableDescription},
};
use crossterm::event::{Event, KeyCode};
use ratatui::{text::Text, widgets::Widget};
use ratatui::{
    Frame,
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    style::Style,
    text::{self, Line},
    widgets::{Block, Clear, HighlightSpacing, Row, StatefulWidget, Table, TableState},
};
use tokio::{sync::OnceCell, try_join};

use item_keys::ItemKeys;
use keys_widget::KeysWidget;

use crate::{
    help, util::pad, widgets::{
        theme::Theme, EnvHandle
    }
};
use dynamate::{
    expr::parse_dynamo_expression,
    dynamodb::{DynamoDbRequest, QueryBuilder, ScanBuilder},
};

mod input;
mod item_keys;
mod keys_widget;

#[derive(Clone)]
pub struct QueryWidget {
    client: Arc<aws_sdk_dynamodb::Client>,
    table_name: String,
    sync_state: Arc<std::sync::RwLock<QuerySyncState>>,
    async_state: Arc<tokio::sync::RwLock<QueryAsyncState>>,
}

#[derive(Default)]
struct QuerySyncState {
    input: input::Input,
    loading_state: LoadingState,
    query_output: Option<QueryOutput>,
    items: Vec<Item>,
    item_keys: Arc<item_keys::ItemKeys>,
    table_state: TableState,
}

#[derive(Debug, Default)]
struct QueryAsyncState {
    table_desc: OnceCell<Arc<TableDescription>>,
}

#[derive(Debug, Clone)]
enum QueryOutput {
    Scan(ScanOutput),
    Query(ScanOutput),
    QueryGSI(ScanOutput, String), // output, index_name
    QueryLSI(ScanOutput, String), // output, index_name
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
            .unwrap_or_else(|| "".to_string())
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum EditingState {
    #[default]
    None,
    Editing,
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
                .into_iter()
                .map(|key| Line::from(key.clone())),
        )
        .style(Style::new().bold());

        let items = &state.items;
        let widths: Vec<Constraint> = keys_view
            .as_slice()
            .into_iter()
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
            LoadingState::Idle | LoadingState::Loaded => {
                let operation_info = match &state.query_output {
                    Some(QueryOutput::Scan(_)) => " (Scan)".to_string(),
                    Some(QueryOutput::Query(_)) => " (Query)".to_string(),
                    Some(QueryOutput::QueryGSI(_, index_name)) => format!(" (Query GSI: {})", index_name),
                    Some(QueryOutput::QueryLSI(_, index_name)) => format!(" (Query LSI: {})", index_name),
                    None => "".to_string(),
                };
                (
                    format!("Results{}", operation_info),
                    pad(format!("{} results, showing {}-{}", total, first_item, last_item), 2),
                )
            },
            LoadingState::Loading => ("Loading".to_string(), "".to_string()),
            LoadingState::Error(err) => (format!("Error: {err}"), "".to_string()),
        };

        let block = Block::bordered()
            .title_top(title)
            .title_bottom(Line::styled(title_bottom, Style::default().fg(theme.neutral_variant())));

        if state.table_state.selected().is_none() && !state.items.is_empty() {
            state.table_state.select(Some(0));
        }

        let table = Table::new(&state.items, widths)
            .block(block)
            .header(header)
            .highlight_spacing(HighlightSpacing::Always)
            .highlight_symbol(">>")
            .row_highlight_style(Style::default().bg(theme.secondary()));

        StatefulWidget::render(table, results_area, frame.buffer_mut(), &mut state.table_state);
    }

    fn handle_event(&self, env: EnvHandle, event: &Event) -> bool {
        let input_is_active = self.sync_state.read().unwrap().input.is_active();
        if input_is_active {
            if self.sync_state.write().unwrap().input.handle_event(event) {                
                return true;
            }
        }    
        if let Some(key) = event.as_key_press_event() {
            match key.code {
                KeyCode::Tab | KeyCode::BackTab => self.sync_state.write().unwrap().input.toggle_active(),
                KeyCode::Esc if input_is_active => self.sync_state.write().unwrap().input.toggle_active(),
                KeyCode::Enter if input_is_active => {
                    let mut state = self.sync_state.write().unwrap();
                    self.start_query(state.input.value(), env.clone());
                    state.input.toggle_active();
                },
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
    const HELP_EDITING: &'static [help::Entry<'static>] = &[
        help::Entry {
            keys: Cow::Borrowed("esc"),
            short: Cow::Borrowed("cancel"),
            long: Cow::Borrowed("Cancel the current operation"),
        },
        help::Entry {
            keys: Cow::Borrowed("enter"),
            short: Cow::Borrowed("query"),
            long: Cow::Borrowed("Execute query"),
        },
    ];
    pub fn new(client: Arc<aws_sdk_dynamodb::Client>, table_name: &str) -> Self {
        Self {
            client: client,
            table_name: table_name.to_string(),
            sync_state: Arc::new(std::sync::RwLock::new(QuerySyncState::default())),
            async_state: Arc::new(tokio::sync::RwLock::new(QueryAsyncState::default())),
        }
    }

    async fn load(self, env: EnvHandle) {
        self.set_loading_state(LoadingState::Loading);
        env.invalidate();

        let this = &self;

        let result = try_join!(
            this.table_description(), // returns Result<Arc<TableDescription>, String>
            this.query(),             // returns Result<_, String>
        );

        match result {
            Ok((table, query)) => {
                self.set_loading_state(LoadingState::Loaded);
                let mut item_keys = HashSet::new();
                let shared_item_keys = self.sync_state.read().unwrap().item_keys.clone();
                let mut state = self.sync_state.write().unwrap();
                let items = query.items().into_iter().map(|item| {
                    item_keys.extend(item.keys().cloned());
                    Item(item.clone(), shared_item_keys.clone())
                });
                state.items.extend(items);
                state.item_keys.extend(item_keys, &table);
                state.query_output = Some(QueryOutput::Scan(query));
                drop(state);
            }
            Err(e) => {
                self.set_loading_state(LoadingState::Error(e));
            }
        }
        env.invalidate();
    }

    async fn query(&self) -> Result<ScanOutput, String> {
        self.scan_table(None).await
    }

    fn set_loading_state(&self, state: LoadingState) {
        self.sync_state.write().unwrap().loading_state = state;
    }

    async fn table_description(&self) -> Result<Arc<TableDescription>, String> {
        let state = self.async_state.write().await;
        let arc_ref = state
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

    fn start_query(&self, query: &str, env: EnvHandle) {
        if query.trim().is_empty() {
            let this: QueryWidget = self.clone();
            tokio::spawn(async move {
                this.set_loading_state(LoadingState::Loading);
                env.invalidate();
                match this.scan_table(None).await {
                    Ok(output) => {
                        this.process_query_output(QueryOutput::Scan(output)).await;
                        this.set_loading_state(LoadingState::Loaded);
                    },
                    Err(e) => this.set_loading_state(LoadingState::Error(e)),
                }
                env.invalidate();
            });
        } else {
            match parse_dynamo_expression(query) {
                Ok(expr) => {
                    let this: QueryWidget = self.clone();
                    tokio::spawn(async move {
                        this.set_loading_state(LoadingState::Loading);
                        env.invalidate();

                        let table_desc = match this.table_description().await {
                            Ok(desc) => desc,
                            Err(e) => {
                                this.set_loading_state(LoadingState::Error(e));
                                env.invalidate();
                                return;
                            }
                        };

                        let db_request = DynamoDbRequest::from_expression_and_table(&expr, &table_desc);
                        let operation_type = db_request.operation_type();

                        tracing::info!("Using operation: {}", operation_type);

                        match this.execute_db_request(&db_request).await {
                            Ok(query_output) => {
                                this.process_query_output(query_output).await;
                                this.set_loading_state(LoadingState::Loaded);
                            },
                            Err(e) => this.set_loading_state(LoadingState::Error(e)),
                        }
                        env.invalidate();
                    });
                },
                Err(e) => {
                    let this = self.clone();
                    tokio::spawn(async move {
                        this.set_loading_state(LoadingState::Error(format!("Parse error: {}", e)));
                        env.invalidate();
                    });
                }
            }
        }
    }

    async fn execute_db_request(&self, db_request: &DynamoDbRequest) -> Result<QueryOutput, String> {
        match db_request {
            DynamoDbRequest::Query(query_builder) => {
                let output = self.execute_query(query_builder).await?;
                let query_output = match query_builder.query_type() {
                    dynamate::dynamodb::QueryType::TableQuery { .. } =>
                        QueryOutput::Query(output),
                    dynamate::dynamodb::QueryType::GlobalSecondaryIndexQuery { index_name, .. } =>
                        QueryOutput::QueryGSI(output, index_name.clone()),
                    dynamate::dynamodb::QueryType::LocalSecondaryIndexQuery { index_name, .. } =>
                        QueryOutput::QueryLSI(output, index_name.clone()),
                    dynamate::dynamodb::QueryType::TableScan =>
                        QueryOutput::Scan(output), // This shouldn't happen for Query
                };
                Ok(query_output)
            }
            DynamoDbRequest::Scan(scan_builder) => {
                let output = self.execute_scan(scan_builder).await?;
                Ok(QueryOutput::Scan(output))
            }
        }
    }


    async fn execute_query(&self, query_builder: &QueryBuilder) -> Result<ScanOutput, String> {
        use aws_sdk_dynamodb::operation::query::QueryOutput;

        let mut query_request = self.client
            .query()
            .table_name(self.table_name.clone());

        // Set index name if this is an index query
        if let Some(index_name) = query_builder.index_name() {
            query_request = query_request.index_name(index_name.clone());
        }

        // Set key condition expression
        if let Some(key_condition) = query_builder.key_condition_expression() {
            query_request = query_request.key_condition_expression(key_condition);
        }

        // Set filter expression if present
        if let Some(filter_expr) = query_builder.filter_expression() {
            query_request = query_request.filter_expression(filter_expr);
        }

        // Set expression attribute names and values
        for (key, value) in query_builder.expression_attribute_names() {
            query_request = query_request.expression_attribute_names(key.clone(), value.clone());
        }

        for (key, value) in query_builder.expression_attribute_values() {
            query_request = query_request.expression_attribute_values(key.clone(), value.clone());
        }

        tracing::info!("Key condition expression: {:?}", query_builder.key_condition_expression());
        tracing::info!("Filter expression: {:?}", query_builder.filter_expression());
        tracing::info!("Attribute names: {:?}", query_builder.expression_attribute_names());
        tracing::info!("Attribute values: {:?}", query_builder.expression_attribute_values());

        let query_result: QueryOutput = query_request
            .send()
            .await
            .map_err(|e| e.to_string())?;

        // Convert QueryOutput to ScanOutput format for compatibility
        let scan_output = ScanOutput::builder()
            .set_items(Some(query_result.items().to_vec()))
            .set_count(Some(query_result.count()))
            .set_scanned_count(Some(query_result.scanned_count()))
            .build();

        Ok(scan_output)
    }

    async fn execute_scan(&self, scan_builder: &ScanBuilder) -> Result<ScanOutput, String> {
        let mut scan_request = self.client
            .scan()
            .table_name(self.table_name.clone());

        if let Some(filter_expr) = scan_builder.filter_expression() {
            scan_request = scan_request.filter_expression(filter_expr);

            tracing::info!("Filter expression: {}", filter_expr);
            tracing::info!("Attribute names: {:?}", scan_builder.expression_attribute_names());
            tracing::info!("Attribute values: {:?}", scan_builder.expression_attribute_values());

            for (key, value) in scan_builder.expression_attribute_names() {
                scan_request = scan_request.expression_attribute_names(key.clone(), value.clone());
            }

            for (key, value) in scan_builder.expression_attribute_values() {
                scan_request = scan_request.expression_attribute_values(key.clone(), value.clone());
            }
        }

        scan_request
            .send()
            .await
            .map_err(|e| e.to_string())
    }

    async fn scan_table(&self, filter_expr: Option<&str>) -> Result<ScanOutput, String> {
        let mut scan_builder = self.client
            .scan()
            .table_name(self.table_name.clone());

        if let Some(filter) = filter_expr {
            scan_builder = scan_builder.filter_expression(filter);
        }

        scan_builder
            .send()
            .await
            .map_err(|e| e.to_string())
    }


    async fn process_query_output(&self, query_output: QueryOutput) {
        let mut item_keys = HashSet::new();
        let shared_item_keys = self.sync_state.read().unwrap().item_keys.clone();

        // Extract the ScanOutput from the QueryOutput enum
        let scan_output = match &query_output {
            QueryOutput::Scan(output) |
            QueryOutput::Query(output) |
            QueryOutput::QueryGSI(output, _) |
            QueryOutput::QueryLSI(output, _) => output,
        };

        let items = scan_output.items();
        let new_items: Vec<Item> = items.iter().map(|item| {
            item_keys.extend(item.keys().cloned());
            Item(item.clone(), shared_item_keys.clone())
        }).collect();

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

        state.query_output = Some(query_output);
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
