//! SQL connection setup: build a connection pool from the URL, and (when
//! read-only) make every connection a read-only session.

use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;

use super::backend::{SqlBackend, SqlPool};
use super::dialect::SqlDialectKind;

pub async fn connect(
    url: &str,
    dialect: SqlDialectKind,
    read_only: bool,
) -> Result<SqlBackend, String> {
    let pool = match dialect {
        SqlDialectKind::Postgres => {
            let mut options = PgPoolOptions::new().max_connections(5);
            if read_only {
                options = options.after_connect(|conn, _meta| {
                    Box::pin(async move {
                        sqlx::query("SET default_transaction_read_only = on")
                            .execute(&mut *conn)
                            .await?;
                        Ok(())
                    })
                });
            }
            SqlPool::Pg(options.connect(url).await.map_err(|e| e.to_string())?)
        }
        SqlDialectKind::Mysql => {
            let mut options = MySqlPoolOptions::new().max_connections(5);
            if read_only {
                options = options.after_connect(|conn, _meta| {
                    Box::pin(async move {
                        sqlx::query("SET SESSION transaction_read_only = ON")
                            .execute(&mut *conn)
                            .await?;
                        Ok(())
                    })
                });
            }
            SqlPool::MySql(options.connect(url).await.map_err(|e| e.to_string())?)
        }
    };
    Ok(SqlBackend::new(
        pool,
        dialect,
        database_name(url),
        read_only,
    ))
}

/// Best-effort database name from a connection URL (`scheme://…/db?params`).
fn database_name(url: &str) -> String {
    let after_scheme = url.split("://").nth(1).unwrap_or(url);
    let path = after_scheme.split_once('/').map_or("", |(_, rest)| rest);
    path.split(['?', '/']).next().unwrap_or("").to_string()
}
