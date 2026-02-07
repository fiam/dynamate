use color_eyre::eyre::{Result, eyre};

use dynamate::dynamodb::{
    AttributeType, CreateTableSpec, GsiSpec, IndexProjection, KeySpec, LsiSpec, create_table,
};

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Table name to create
    #[arg(long, value_name = "TABLE")]
    pub table: String,

    /// Partition key, format: NAME:TYPE (TYPE is S|N|B)
    #[arg(long, value_name = "NAME:TYPE")]
    pub pk: String,

    /// Sort key, format: NAME:TYPE (TYPE is S|N|B)
    #[arg(long, value_name = "NAME:TYPE")]
    pub sk: Option<String>,

    /// Add a GSI. Format: NAME:PK:PK_TYPE[:SK:SK_TYPE][:PROJECTION]
    /// PROJECTION can be: all | keys_only | include=attr1,attr2
    #[arg(long, value_name = "GSI", action = clap::ArgAction::Append)]
    pub gsi: Vec<String>,

    /// Add a LSI. Format: NAME:SK:SK_TYPE[:PROJECTION]
    /// PROJECTION can be: all | keys_only | include=attr1,attr2
    #[arg(long, value_name = "LSI", action = clap::ArgAction::Append)]
    pub lsi: Vec<String>,
}

pub async fn command(client: &aws_sdk_dynamodb::Client, args: Args) -> Result<()> {
    let table_name = args.table.trim().to_string();
    let hash_key = parse_key_spec(&args.pk)
        .map_err(|err| eyre!("Invalid --pk value: {err}"))?;
    let sort_key = match args.sk.as_deref() {
        Some(raw) => Some(parse_key_spec(raw).map_err(|err| eyre!("Invalid --sk value: {err}"))?),
        None => None,
    };

    let mut gsis = Vec::new();
    for raw in &args.gsi {
        let spec = parse_gsi(raw)
            .map_err(|err| eyre!("Invalid --gsi value ({raw}): {err}"))?;
        gsis.push(spec);
    }

    let mut lsis = Vec::new();
    for raw in &args.lsi {
        let spec = parse_lsi(raw)
            .map_err(|err| eyre!("Invalid --lsi value ({raw}): {err}"))?;
        lsis.push(spec);
    }

    let spec = CreateTableSpec {
        table_name,
        hash_key,
        sort_key,
        gsis,
        lsis,
    };

    create_table(client.clone(), spec.clone())
        .await
        .map_err(|err| eyre!(err))?;

    println!("Created table {}", spec.table_name);
    Ok(())
}

fn parse_key_spec(raw: &str) -> Result<KeySpec, String> {
    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() != 2 {
        return Err("expected NAME:TYPE".to_string());
    }
    let name = parts[0].trim();
    if name.is_empty() {
        return Err("name is required".to_string());
    }
    let attr_type = AttributeType::parse(parts[1])?;
    Ok(KeySpec {
        name: name.to_string(),
        attr_type,
    })
}

fn parse_gsi(raw: &str) -> Result<GsiSpec, String> {
    let mut parts: Vec<&str> = raw.split(':').collect();
    let projection = parse_optional_projection(&mut parts)?;

    match parts.len() {
        3 => {
            let name = parse_name(parts[0], "GSI name")?;
            let pk_name = parse_name(parts[1], "GSI partition key")?;
            let pk_type = AttributeType::parse(parts[2])?;
            Ok(GsiSpec {
                name,
                hash_key: KeySpec {
                    name: pk_name,
                    attr_type: pk_type,
                },
                sort_key: None,
                projection,
            })
        }
        5 => {
            let name = parse_name(parts[0], "GSI name")?;
            let pk_name = parse_name(parts[1], "GSI partition key")?;
            let pk_type = AttributeType::parse(parts[2])?;
            let sk_name = parse_name(parts[3], "GSI sort key")?;
            let sk_type = AttributeType::parse(parts[4])?;
            Ok(GsiSpec {
                name,
                hash_key: KeySpec {
                    name: pk_name,
                    attr_type: pk_type,
                },
                sort_key: Some(KeySpec {
                    name: sk_name,
                    attr_type: sk_type,
                }),
                projection,
            })
        }
        _ => Err(
            "expected NAME:PK:PK_TYPE[:SK:SK_TYPE][:PROJECTION]".to_string(),
        ),
    }
}

fn parse_lsi(raw: &str) -> Result<LsiSpec, String> {
    let mut parts: Vec<&str> = raw.split(':').collect();
    let projection = parse_optional_projection(&mut parts)?;

    if parts.len() != 3 {
        return Err("expected NAME:SK:SK_TYPE[:PROJECTION]".to_string());
    }

    let name = parse_name(parts[0], "LSI name")?;
    let sk_name = parse_name(parts[1], "LSI sort key")?;
    let sk_type = AttributeType::parse(parts[2])?;

    Ok(LsiSpec {
        name,
        sort_key: KeySpec {
            name: sk_name,
            attr_type: sk_type,
        },
        projection,
    })
}

fn parse_optional_projection(parts: &mut Vec<&str>) -> Result<IndexProjection, String> {
    let Some(last) = parts.last().copied() else {
        return Ok(IndexProjection::All);
    };
    let candidate = last.trim();
    if candidate.is_empty() {
        return Ok(IndexProjection::All);
    }
    if let Ok(projection) = IndexProjection::parse_token(candidate) {
        parts.pop();
        return Ok(projection);
    }
    Ok(IndexProjection::All)
}

fn parse_name(value: &str, label: &str) -> Result<String, String> {
    let name = value.trim();
    if name.is_empty() {
        return Err(format!("{label} is required"));
    }
    Ok(name.to_string())
}

#[cfg(test)]
mod tests {
    use super::{Args, parse_gsi, parse_key_spec, parse_lsi};
    use clap::Parser;
    use dynamate::dynamodb::{AttributeType, IndexProjection};

    #[derive(Parser, Debug)]
    struct Cli {
        #[command(flatten)]
        args: Args,
    }

    #[test]
    fn parse_gsi_without_sort_key() {
        let spec = parse_gsi("GSI1:PK:S").unwrap();
        assert!(spec.sort_key.is_none());
    }

    #[test]
    fn parse_gsi_with_projection() {
        let spec = parse_gsi("GSI1:PK:S:all").unwrap();
        assert!(matches!(spec.projection, IndexProjection::All));
    }

    #[test]
    fn parse_lsi_with_projection() {
        let spec = parse_lsi("LSI1:SK:S:keys_only").unwrap();
        assert!(matches!(spec.projection, IndexProjection::KeysOnly));
    }

    #[test]
    fn parse_args_minimal() {
        let cli = Cli::try_parse_from(["dynamate", "--table", "demo", "--pk", "PK:S"])
            .unwrap();
        assert_eq!(cli.args.table, "demo");
        assert_eq!(cli.args.pk, "PK:S");
        assert!(cli.args.sk.is_none());
        assert!(cli.args.gsi.is_empty());
        assert!(cli.args.lsi.is_empty());
    }

    #[test]
    fn parse_args_with_indexes() {
        let cli = Cli::try_parse_from([
            "dynamate",
            "--table",
            "demo",
            "--pk",
            "PK:S",
            "--sk",
            "SK:S",
            "--gsi",
            "GSI1:GSI1PK:S:all",
            "--gsi",
            "GSI2:GSI2PK:N:GSI2SK:S:include=owner,status",
            "--lsi",
            "LSI1:LSI1SK:S:keys_only",
        ])
        .unwrap();
        assert_eq!(cli.args.gsi.len(), 2);
        assert_eq!(cli.args.lsi.len(), 1);
    }

    #[test]
    fn parse_args_rejects_bad_key_format() {
        let err = parse_key_spec("PK").unwrap_err();
        assert!(err.contains("expected NAME:TYPE"));
    }

    #[test]
    fn parse_args_rejects_bad_type() {
        let err = parse_key_spec("PK:Z").unwrap_err();
        assert!(err.contains("Unknown attribute type"));
    }

    #[test]
    fn parse_args_allows_gsi_without_sort_key() {
        let cli = Cli::try_parse_from([
            "dynamate",
            "--table",
            "demo",
            "--pk",
            "PK:S",
            "--gsi",
            "GSI1:GSI1PK:S",
        ])
        .unwrap();
        let spec = parse_gsi(&cli.args.gsi[0]).unwrap();
        assert!(spec.sort_key.is_none());
        assert!(matches!(spec.hash_key.attr_type, AttributeType::String));
    }
}
