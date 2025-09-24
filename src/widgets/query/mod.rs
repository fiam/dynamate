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
use dynamate::expr::{parse_dynamo_expression, DynamoExpression, Operand, Comparator, FunctionName};

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
            LoadingState::Idle | LoadingState::Loaded => (
                "Results".to_string(),
                pad(format!("{} results, showing {}-{}", total, first_item, last_item), 2),
            ),
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
                        this.process_scan_results(output).await;
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
                        let (filter_expr, attr_names, attr_values) = this.convert_expression_to_filter(&expr);
                        match this.scan_with_filter(&filter_expr, attr_names, attr_values).await {
                            Ok(output) => {
                                this.process_scan_results(output).await;
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

    fn convert_expression_to_filter(&self, expr: &DynamoExpression) -> (String, HashMap<String, String>, HashMap<String, AttributeValue>) {
        let mut attr_names = HashMap::new();
        let mut attr_values = HashMap::new();
        let mut name_counter = 0;
        let mut value_counter = 0;

        let filter_expr = self.build_filter_expression(expr, &mut attr_names, &mut attr_values, &mut name_counter, &mut value_counter);
        (filter_expr, attr_names, attr_values)
    }

    fn build_filter_expression(
        &self,
        expr: &DynamoExpression,
        attr_names: &mut HashMap<String, String>,
        attr_values: &mut HashMap<String, AttributeValue>,
        name_counter: &mut u32,
        value_counter: &mut u32
    ) -> String {
        match expr {
            DynamoExpression::Comparison { left, operator, right } => {
                let left_str = self.operand_to_string(left, attr_names, attr_values, name_counter, value_counter);
                let op_str = match operator {
                    Comparator::Equal => "=",
                    Comparator::NotEqual => "<>",
                    Comparator::Less => "<",
                    Comparator::LessOrEqual => "<=",
                    Comparator::Greater => ">",
                    Comparator::GreaterOrEqual => ">=",
                };
                let right_str = self.operand_to_string(right, attr_names, attr_values, name_counter, value_counter);
                format!("{} {} {}", left_str, op_str, right_str)
            },
            DynamoExpression::Between { operand, lower, upper } => {
                let operand_str = self.operand_to_string(operand, attr_names, attr_values, name_counter, value_counter);
                let lower_str = self.operand_to_string(lower, attr_names, attr_values, name_counter, value_counter);
                let upper_str = self.operand_to_string(upper, attr_names, attr_values, name_counter, value_counter);
                format!("{} BETWEEN {} AND {}", operand_str, lower_str, upper_str)
            },
            DynamoExpression::In { operand, values } => {
                let operand_str = self.operand_to_string(operand, attr_names, attr_values, name_counter, value_counter);
                let value_strs: Vec<String> = values.iter()
                    .map(|v| self.operand_to_string(v, attr_names, attr_values, name_counter, value_counter))
                    .collect();
                format!("{} IN ({})", operand_str, value_strs.join(", "))
            },
            DynamoExpression::Function { name, args } => {
                let func_name = match name {
                    FunctionName::AttributeExists => "attribute_exists",
                    FunctionName::AttributeNotExists => "attribute_not_exists",
                    FunctionName::AttributeType => "attribute_type",
                    FunctionName::BeginsWith => "begins_with",
                    FunctionName::Contains => "contains",
                    FunctionName::Size => "size",
                };
                let arg_strs: Vec<String> = args.iter()
                    .map(|arg| self.operand_to_string(arg, attr_names, attr_values, name_counter, value_counter))
                    .collect();
                format!("{}({})", func_name, arg_strs.join(", "))
            },
            DynamoExpression::And(left, right) => {
                let left_str = self.build_filter_expression(left, attr_names, attr_values, name_counter, value_counter);
                let right_str = self.build_filter_expression(right, attr_names, attr_values, name_counter, value_counter);
                format!("({}) AND ({})", left_str, right_str)
            },
            DynamoExpression::Or(left, right) => {
                let left_str = self.build_filter_expression(left, attr_names, attr_values, name_counter, value_counter);
                let right_str = self.build_filter_expression(right, attr_names, attr_values, name_counter, value_counter);
                format!("({}) OR ({})", left_str, right_str)
            },
            DynamoExpression::Not(inner) => {
                let inner_str = self.build_filter_expression(inner, attr_names, attr_values, name_counter, value_counter);
                format!("NOT ({})", inner_str)
            },
            DynamoExpression::Parentheses(inner) => {
                let inner_str = self.build_filter_expression(inner, attr_names, attr_values, name_counter, value_counter);
                format!("({})", inner_str)
            },
        }
    }

    fn operand_to_string(
        &self,
        operand: &Operand,
        attr_names: &mut HashMap<String, String>,
        attr_values: &mut HashMap<String, AttributeValue>,
        name_counter: &mut u32,
        value_counter: &mut u32
    ) -> String {
        match operand {
            Operand::Path(path) => {
                let name_placeholder = format!("#name{}", name_counter);
                *name_counter += 1;
                attr_names.insert(name_placeholder.clone(), path.clone());
                name_placeholder
            },
            Operand::Value(val) => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                attr_values.insert(value_placeholder.clone(), AttributeValue::S(val.clone()));
                value_placeholder
            },
            Operand::Number(num) => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                attr_values.insert(value_placeholder.clone(), AttributeValue::N(num.to_string()));
                value_placeholder
            },
            Operand::Boolean(b) => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                attr_values.insert(value_placeholder.clone(), AttributeValue::Bool(*b));
                value_placeholder
            },
            Operand::Null => {
                let value_placeholder = format!(":val{}", value_counter);
                *value_counter += 1;
                attr_values.insert(value_placeholder.clone(), AttributeValue::Null(true));
                value_placeholder
            },
        }
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

    async fn scan_with_filter(
        &self,
        filter_expr: &str,
        attr_names: HashMap<String, String>,
        attr_values: HashMap<String, AttributeValue>
    ) -> Result<ScanOutput, String> {
        let mut scan_builder = self.client
            .scan()
            .table_name(self.table_name.clone())
            .filter_expression(filter_expr);

        for (key, value) in attr_names {
            scan_builder = scan_builder.expression_attribute_names(key, value);
        }

        for (key, value) in attr_values {
            scan_builder = scan_builder.expression_attribute_values(key, value);
        }

        scan_builder
            .send()
            .await
            .map_err(|e| e.to_string())
    }

    async fn process_scan_results(&self, query_output: ScanOutput) {
        let mut item_keys = HashSet::new();
        let shared_item_keys = self.sync_state.read().unwrap().item_keys.clone();

        let items = query_output.items();
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

        state.query_output = Some(QueryOutput::Scan(query_output));
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
