//! Render a parsed query expression back into a compact, human-readable string
//! for footers and summaries. Pure functions over the `expr` AST with no
//! dependency on widget state.

pub(super) fn format_query_summary(expr: &dynamate::expr::DynamoExpression) -> String {
    if !contains_or_or_not(expr) {
        let mut parts = Vec::new();
        collect_and_parts(expr, &mut parts);
        return parts
            .into_iter()
            .map(format_expr_compact)
            .collect::<Vec<_>>()
            .join(" ");
    }
    format_expr(expr, 0)
}

fn contains_or_or_not(expr: &dynamate::expr::DynamoExpression) -> bool {
    use dynamate::expr::DynamoExpression::{
        And, Between, Comparison, Function, In, Not, Or, Parentheses,
    };
    match expr {
        Or(_, _) | Not(_) => true,
        And(left, right) => contains_or_or_not(left) || contains_or_or_not(right),
        Parentheses(inner) => contains_or_or_not(inner),
        Comparison { .. } | Between { .. } | In { .. } | Function { .. } => false,
    }
}

fn collect_and_parts<'a>(
    expr: &'a dynamate::expr::DynamoExpression,
    parts: &mut Vec<&'a dynamate::expr::DynamoExpression>,
) {
    use dynamate::expr::DynamoExpression::{And, Parentheses};
    match expr {
        And(left, right) => {
            collect_and_parts(left, parts);
            collect_and_parts(right, parts);
        }
        Parentheses(inner) => collect_and_parts(inner, parts),
        _ => parts.push(expr),
    }
}

fn format_expr(expr: &dynamate::expr::DynamoExpression, parent_prec: u8) -> String {
    use dynamate::expr::DynamoExpression::{
        And, Between, Comparison, Function, In, Not, Or, Parentheses,
    };
    let my_prec = match expr {
        Or(_, _) => 1,
        And(_, _) => 2,
        Not(_) => 3,
        _ => 4,
    };
    let rendered = match expr {
        Comparison {
            left,
            operator,
            right,
        } => {
            format!(
                "{}{}{}",
                format_operand(left),
                format_comparator(operator),
                format_operand(right)
            )
        }
        Between {
            operand,
            lower,
            upper,
        } => {
            format!(
                "{} BETWEEN {} AND {}",
                format_operand(operand),
                format_operand(lower),
                format_operand(upper)
            )
        }
        In { operand, values } => {
            let values = values
                .iter()
                .map(format_operand)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{} IN ({values})", format_operand(operand))
        }
        Function { name, args } => {
            let args = args
                .iter()
                .map(format_operand)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({})", format_function_name(name), args)
        }
        And(left, right) => {
            format!(
                "{} AND {}",
                format_expr(left, my_prec),
                format_expr(right, my_prec)
            )
        }
        Or(left, right) => {
            format!(
                "{} OR {}",
                format_expr(left, my_prec),
                format_expr(right, my_prec)
            )
        }
        Not(inner) => format!("NOT {}", format_expr(inner, my_prec)),
        Parentheses(inner) => format!("({})", format_expr(inner, 0)),
    };
    if my_prec < parent_prec {
        format!("({rendered})")
    } else {
        rendered
    }
}

fn format_expr_compact(expr: &dynamate::expr::DynamoExpression) -> String {
    use dynamate::expr::DynamoExpression::{
        And, Between, Comparison, Function, In, Not, Or, Parentheses,
    };
    match expr {
        Comparison {
            left,
            operator,
            right,
        } => {
            format!(
                "{}{}{}",
                format_operand(left),
                format_comparator(operator),
                format_operand(right)
            )
        }
        Between {
            operand,
            lower,
            upper,
        } => {
            format!(
                "{} BETWEEN {} AND {}",
                format_operand(operand),
                format_operand(lower),
                format_operand(upper)
            )
        }
        In { operand, values } => {
            let values = values
                .iter()
                .map(format_operand)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{} IN ({values})", format_operand(operand))
        }
        Function { name, args } => {
            let args = args
                .iter()
                .map(format_operand)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({})", format_function_name(name), args)
        }
        Parentheses(inner) => format!("({})", format_expr(inner, 0)),
        And(_, _) | Or(_, _) | Not(_) => format_expr(expr, 0),
    }
}

fn format_operand(operand: &dynamate::expr::Operand) -> String {
    use dynamate::expr::Operand;
    match operand {
        Operand::Path(path) => format_path(path),
        Operand::Value(value) => format_string(value),
        Operand::Number(num) => format_number(*num),
        Operand::Boolean(value) => value.to_string(),
        Operand::Null => "null".to_string(),
    }
}

fn format_comparator(comp: &dynamate::expr::Comparator) -> &'static str {
    use dynamate::expr::Comparator::{Equal, Greater, GreaterOrEqual, Less, LessOrEqual, NotEqual};
    match comp {
        Equal => "=",
        NotEqual => "!=",
        Less => "<",
        LessOrEqual => "<=",
        Greater => ">",
        GreaterOrEqual => ">=",
    }
}

fn format_function_name(name: &dynamate::expr::FunctionName) -> &'static str {
    name.as_str()
}

fn format_path(path: &str) -> String {
    if path.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        path.to_string()
    } else {
        format!("`{path}`")
    }
}

fn format_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""))
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}
