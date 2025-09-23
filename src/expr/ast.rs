#[derive(Debug, Clone, PartialEq)]
pub enum DynamoExpression {
    Comparison {
        left: Operand,
        operator: Comparator,
        right: Operand,
    },
    Between {
        operand: Operand,
        lower: Operand,
        upper: Operand,
    },
    In {
        operand: Operand,
        values: Vec<Operand>,
    },
    Function {
        name: FunctionName,
        args: Vec<Operand>,
    },
    And(Box<DynamoExpression>, Box<DynamoExpression>),
    Or(Box<DynamoExpression>, Box<DynamoExpression>),
    Not(Box<DynamoExpression>),
    Parentheses(Box<DynamoExpression>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Path(String),
    Value(String),
    Number(f64),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Comparator {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionName {
    AttributeExists,
    AttributeNotExists,
    AttributeType,
    BeginsWith,
    Contains,
    Size,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Number(f64),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyValue {
    pub key: String,
    pub value: Value,
}