use std::collections::{HashMap, HashSet};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::RequestId;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType,
    LocalSecondaryIndex, Projection, ProjectionType, ScalarAttributeType,
};

use super::send_dynamo_request;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttributeType {
    String,
    Number,
    Binary,
}

impl AttributeType {
    pub fn parse(value: &str) -> Result<Self, String> {
        let value = value.trim();
        if value.is_empty() {
            return Err("Attribute type is required".to_string());
        }
        match value.to_ascii_lowercase().as_str() {
            "s" | "string" => Ok(AttributeType::String),
            "n" | "number" => Ok(AttributeType::Number),
            "b" | "binary" => Ok(AttributeType::Binary),
            _ => Err(format!("Unknown attribute type: {value}")),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            AttributeType::String => "S",
            AttributeType::Number => "N",
            AttributeType::Binary => "B",
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            AttributeType::String => "string",
            AttributeType::Number => "number",
            AttributeType::Binary => "binary",
        }
    }

    pub fn to_scalar(self) -> ScalarAttributeType {
        match self {
            AttributeType::String => ScalarAttributeType::S,
            AttributeType::Number => ScalarAttributeType::N,
            AttributeType::Binary => ScalarAttributeType::B,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexProjection {
    All,
    KeysOnly,
    Include(Vec<String>),
}

impl IndexProjection {
    pub fn parse_token(raw: &str) -> Result<Self, String> {
        let token = raw.trim();
        if token.is_empty() {
            return Err("Projection is empty".to_string());
        }
        let lower = token.to_ascii_lowercase();
        match lower.as_str() {
            "all" => return Ok(IndexProjection::All),
            "keys_only" | "keys-only" | "keys" => return Ok(IndexProjection::KeysOnly),
            _ => {}
        }

        if lower.starts_with("include=") {
            let attrs = &token["include=".len()..];
            return Self::parse_include(attrs);
        }
        if lower.starts_with("include:") {
            let attrs = &token["include:".len()..];
            return Self::parse_include(attrs);
        }
        if lower.starts_with("include(") && lower.ends_with(')') {
            let attrs = &token["include(".len()..token.len().saturating_sub(1)];
            return Self::parse_include(attrs);
        }

        Err(format!("Unknown projection: {token}"))
    }

    pub fn validate(&self) -> Result<(), String> {
        if let IndexProjection::Include(attrs) = self
            && attrs.is_empty()
        {
            return Err("Include projection requires attributes".to_string());
        }
        Ok(())
    }

    pub fn build_projection(&self) -> Result<Projection, String> {
        match self {
            IndexProjection::All => Ok(Projection::builder()
                .projection_type(ProjectionType::All)
                .build()),
            IndexProjection::KeysOnly => Ok(Projection::builder()
                .projection_type(ProjectionType::KeysOnly)
                .build()),
            IndexProjection::Include(attrs) => {
                if attrs.is_empty() {
                    return Err("Include projection requires attributes".to_string());
                }
                Ok(Projection::builder()
                    .projection_type(ProjectionType::Include)
                    .set_non_key_attributes(Some(attrs.clone()))
                    .build())
            }
        }
    }

    fn parse_include(raw: &str) -> Result<Self, String> {
        let attrs = parse_attribute_list(raw);
        if attrs.is_empty() {
            return Err("Include projection requires attributes".to_string());
        }
        Ok(IndexProjection::Include(attrs))
    }
}

#[derive(Debug, Clone)]
pub struct KeySpec {
    pub name: String,
    pub attr_type: AttributeType,
}

#[derive(Debug, Clone)]
pub struct GsiSpec {
    pub name: String,
    pub hash_key: KeySpec,
    pub sort_key: Option<KeySpec>,
    pub projection: IndexProjection,
}

#[derive(Debug, Clone)]
pub struct LsiSpec {
    pub name: String,
    pub sort_key: KeySpec,
    pub projection: IndexProjection,
}

#[derive(Debug, Clone)]
pub struct CreateTableSpec {
    pub table_name: String,
    pub hash_key: KeySpec,
    pub sort_key: Option<KeySpec>,
    pub gsis: Vec<GsiSpec>,
    pub lsis: Vec<LsiSpec>,
}

impl CreateTableSpec {
    pub fn validate(&self) -> Result<(), String> {
        if self.table_name.trim().is_empty() {
            return Err("Table name is required".to_string());
        }
        if self.hash_key.name.trim().is_empty() {
            return Err("Partition key is required".to_string());
        }
        if let Some(sort_key) = self.sort_key.as_ref()
            && sort_key.name.trim().is_empty()
        {
            return Err("Sort key name is required".to_string());
        }

        if !self.lsis.is_empty() && self.sort_key.is_none() {
            return Err("LSI requires a table sort key".to_string());
        }

        let mut index_names = HashSet::new();
        for gsi in &self.gsis {
            if gsi.name.trim().is_empty() {
                return Err("GSI name is required".to_string());
            }
            if !index_names.insert(gsi.name.clone()) {
                return Err(format!("Duplicate index name: {}", gsi.name));
            }
            if gsi.hash_key.name.trim().is_empty() {
                return Err("GSI partition key is required".to_string());
            }
            if let Some(sort_key) = gsi.sort_key.as_ref()
                && sort_key.name.trim().is_empty()
            {
                return Err("GSI sort key name is required".to_string());
            }
            gsi.projection.validate()?;
        }

        for lsi in &self.lsis {
            if lsi.name.trim().is_empty() {
                return Err("LSI name is required".to_string());
            }
            if !index_names.insert(lsi.name.clone()) {
                return Err(format!("Duplicate index name: {}", lsi.name));
            }
            if lsi.sort_key.name.trim().is_empty() {
                return Err("LSI sort key is required".to_string());
            }
            lsi.projection.validate()?;
        }

        self.attribute_map()?;
        Ok(())
    }

    fn attribute_map(&self) -> Result<HashMap<String, AttributeType>, String> {
        let mut map = HashMap::new();
        register_attribute(&mut map, &self.hash_key.name, self.hash_key.attr_type)?;
        if let Some(sort_key) = self.sort_key.as_ref() {
            register_attribute(&mut map, &sort_key.name, sort_key.attr_type)?;
        }
        for gsi in &self.gsis {
            register_attribute(&mut map, &gsi.hash_key.name, gsi.hash_key.attr_type)?;
            if let Some(sort_key) = gsi.sort_key.as_ref() {
                register_attribute(&mut map, &sort_key.name, sort_key.attr_type)?;
            }
        }
        for lsi in &self.lsis {
            register_attribute(&mut map, &lsi.sort_key.name, lsi.sort_key.attr_type)?;
        }
        Ok(map)
    }
}

fn register_attribute(
    map: &mut HashMap<String, AttributeType>,
    name: &str,
    attr_type: AttributeType,
) -> Result<(), String> {
    if let Some(existing) = map.get(name) {
        if *existing != attr_type {
            return Err(format!(
                "Attribute {name} has conflicting types ({} vs {})",
                existing.label(),
                attr_type.label()
            ));
        }
        return Ok(());
    }
    map.insert(name.to_string(), attr_type);
    Ok(())
}

pub async fn create_table(client: Client, spec: CreateTableSpec) -> Result<(), String> {
    spec.validate()?;

    let attribute_map = spec.attribute_map()?;
    let mut attribute_definitions = Vec::with_capacity(attribute_map.len());
    for (name, attr_type) in attribute_map {
        let def = AttributeDefinition::builder()
            .attribute_name(name)
            .attribute_type(attr_type.to_scalar())
            .build()
            .map_err(|err| err.to_string())?;
        attribute_definitions.push(def);
    }

    let mut key_schema = Vec::new();
    key_schema.push(
        KeySchemaElement::builder()
            .attribute_name(spec.hash_key.name.clone())
            .key_type(KeyType::Hash)
            .build()
            .map_err(|err| err.to_string())?,
    );
    if let Some(sort_key) = spec.sort_key.as_ref() {
        key_schema.push(
            KeySchemaElement::builder()
                .attribute_name(sort_key.name.clone())
                .key_type(KeyType::Range)
                .build()
                .map_err(|err| err.to_string())?,
        );
    }

    let mut gsi_defs = Vec::new();
    for gsi in &spec.gsis {
        let projection = gsi.projection.build_projection()?;
        let mut gsi_key_schema = Vec::new();
        gsi_key_schema.push(
            KeySchemaElement::builder()
                .attribute_name(gsi.hash_key.name.clone())
                .key_type(KeyType::Hash)
                .build()
                .map_err(|err| err.to_string())?,
        );
        if let Some(sort_key) = gsi.sort_key.as_ref() {
            gsi_key_schema.push(
                KeySchemaElement::builder()
                    .attribute_name(sort_key.name.clone())
                    .key_type(KeyType::Range)
                    .build()
                    .map_err(|err| err.to_string())?,
            );
        }
        let gsi_def = GlobalSecondaryIndex::builder()
            .index_name(gsi.name.clone())
            .set_key_schema(Some(gsi_key_schema))
            .projection(projection)
            .build()
            .map_err(|err| err.to_string())?;
        gsi_defs.push(gsi_def);
    }

    let mut lsi_defs = Vec::new();
    for lsi in &spec.lsis {
        let projection = lsi.projection.build_projection()?;
        let lsi_key_schema = vec![
            KeySchemaElement::builder()
                .attribute_name(spec.hash_key.name.clone())
                .key_type(KeyType::Hash)
                .build()
                .map_err(|err| err.to_string())?,
            KeySchemaElement::builder()
                .attribute_name(lsi.sort_key.name.clone())
                .key_type(KeyType::Range)
                .build()
                .map_err(|err| err.to_string())?,
        ];
        let lsi_def = LocalSecondaryIndex::builder()
            .index_name(lsi.name.clone())
            .set_key_schema(Some(lsi_key_schema))
            .projection(projection)
            .build()
            .map_err(|err| err.to_string())?;
        lsi_defs.push(lsi_def);
    }

    let mut request = client
        .create_table()
        .table_name(spec.table_name.clone())
        .billing_mode(BillingMode::PayPerRequest);

    for def in attribute_definitions {
        request = request.attribute_definitions(def);
    }
    for key in key_schema {
        request = request.key_schema(key);
    }
    if !gsi_defs.is_empty() {
        for gsi in gsi_defs {
            request = request.global_secondary_indexes(gsi);
        }
    }
    if !lsi_defs.is_empty() {
        for lsi in lsi_defs {
            request = request.local_secondary_indexes(lsi);
        }
    }

    let span = tracing::trace_span!(
        "CreateTable",
        table = %spec.table_name,
        gsi_count = spec.gsis.len(),
        lsi_count = spec.lsis.len()
    );
    let result = send_dynamo_request(span, || request.send(), format_sdk_error).await;
    result.map(|_| ()).map_err(|err| format_sdk_error(&err))
}

fn format_sdk_error<E>(err: &SdkError<E>) -> String
where
    E: ProvideErrorMetadata + RequestId + std::error::Error + 'static,
{
    if let Some(service_err) = err.as_service_error() {
        let code = service_err.code().unwrap_or("ServiceError");
        let message = service_err.message().unwrap_or("").trim();
        let mut summary = if message.is_empty() {
            code.to_string()
        } else {
            format!("{code}: {message}")
        };
        if let Some(request_id) = service_err.request_id() {
            summary.push_str(&format!(" (request id: {request_id})"));
        }
        return summary;
    }
    DisplayErrorContext(err).to_string()
}

fn parse_attribute_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        AttributeType, CreateTableSpec, GsiSpec, IndexProjection, KeySpec, LsiSpec,
    };

    #[test]
    fn attribute_type_parse_accepts_shortcodes() {
        assert_eq!(AttributeType::parse("S").unwrap(), AttributeType::String);
        assert_eq!(AttributeType::parse("n").unwrap(), AttributeType::Number);
        assert_eq!(AttributeType::parse("binary").unwrap(), AttributeType::Binary);
    }

    #[test]
    fn projection_include_requires_attributes() {
        let projection = IndexProjection::Include(Vec::new());
        assert!(projection.validate().is_err());
    }

    #[test]
    fn lsi_requires_table_sort_key() {
        let spec = CreateTableSpec {
            table_name: "demo".to_string(),
            hash_key: KeySpec {
                name: "PK".to_string(),
                attr_type: AttributeType::String,
            },
            sort_key: None,
            gsis: Vec::new(),
            lsis: vec![LsiSpec {
                name: "LSI1".to_string(),
                sort_key: KeySpec {
                    name: "LSI1SK".to_string(),
                    attr_type: AttributeType::String,
                },
                projection: IndexProjection::All,
            }],
        };
        let err = spec.validate().unwrap_err();
        assert!(err.contains("LSI requires a table sort key"));
    }

    #[test]
    fn conflicting_attribute_types_fail() {
        let spec = CreateTableSpec {
            table_name: "demo".to_string(),
            hash_key: KeySpec {
                name: "PK".to_string(),
                attr_type: AttributeType::String,
            },
            sort_key: Some(KeySpec {
                name: "SK".to_string(),
                attr_type: AttributeType::String,
            }),
            gsis: vec![GsiSpec {
                name: "GSI1".to_string(),
                hash_key: KeySpec {
                    name: "PK".to_string(),
                    attr_type: AttributeType::Number,
                },
                sort_key: None,
                projection: IndexProjection::All,
            }],
            lsis: Vec::new(),
        };
        let err = spec.validate().unwrap_err();
        assert!(err.contains("conflicting types"));
    }
}
