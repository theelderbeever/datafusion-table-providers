use datafusion::execution::SendableRecordBatchStream;
use datafusion_table_providers_common::sql::db_connection_pool::indexes::{IndexDiscovery, IndexInfo};
use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::GenericError;
use datafusion_table_providers_common::sql::db_connection_pool::query::{NativeQueryExecutor, QueryOptions, QueryParams};
use std::{collections::HashMap, sync::Arc};

use clickhouse::{Client, Compression};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::TableReference;
use secrecy::{ExposeSecret, SecretString};
use snafu::{ResultExt, Snafu};

use crate::conn::ClickHouseConnection;
use datafusion_table_providers_common::sql::db_connection_pool::{
    dbconnection::{AsyncDbConnection, DbConnection, TableSchemas},
    DbConnectionPool, JoinPushDown,
};
type PoolResult<T, E = Box<dyn std::error::Error + Send + Sync>> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ClickHouse connection failed. {source}"))]
    ConnectionError { source: clickhouse::error::Error },

    #[snafu(display("Invalid connection string for ClickHouse. {source}"))]
    InvalidConnectionString { source: url::ParseError },

    #[snafu(display("Invalid value for parameter {parameter_name}. Ensure the value is valid for parameter {parameter_name}"))]
    InvalidParameterError { parameter_name: String },
}

#[derive(Clone)]
pub struct ClickHouseConnectionPool {
    pub client: Client,
    pub join_push_down: JoinPushDown,
    pub schemas: Arc<TableSchemas>,
}

impl std::fmt::Debug for ClickHouseConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseConnectionPool")
            .field("join_push_down", &self.join_push_down)
            .finish()
    }
}

impl ClickHouseConnectionPool {
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self, Error> {
        let mut client = Client::default();
        let mut url = None;
        let mut database = None;

        for (key, value) in &params {
            let value = value.expose_secret();
            match key.as_str() {
                "url" => {
                    client = client.with_url(value);
                    url = Some(value)
                }
                "database" => {
                    client = client.with_database(value);
                    database = Some(value)
                }
                "user" => {
                    client = client.with_user(value);
                }
                "password" => {
                    client = client.with_password(value);
                }
                "access_token" => {
                    client = client.with_access_token(value);
                }
                "compression" => {
                    client = match value.to_lowercase().as_str() {
                        "lz4" => client.with_compression(Compression::Lz4),
                        "none" => client.with_compression(Compression::None),
                        other => {
                            return Err(Error::InvalidParameterError {
                                parameter_name: format!("compression = {}", other),
                            });
                        }
                    };
                }
                key if key.starts_with("option_") => {
                    let opt = &key["option_".len()..];
                    client = client.with_setting(opt, value);
                }
                key if key.starts_with("header_") => {
                    let header = &key["header_".len()..];
                    client = client.with_header(header, value);
                }
                key if key.starts_with("product_") => {
                    let name = &key["product_".len()..];
                    client = client.with_product_info(name, value);
                }
                _ => {
                    // Unknown keys are ignored silently or optionally warn
                }
            }
        }

        client
            .query("SELECT 1")
            .fetch_all::<u8>()
            .await
            .context(ConnectionSnafu)?;

        let join_push_down = {
            let mut ctx = format!("url={}", url.unwrap_or("default"));
            if let Some(db) = database {
                ctx.push_str(&format!(",db={}", db));
            }
            JoinPushDown::AllowedFor(ctx)
        };

        Ok(Self {
            client,
            join_push_down,
            schemas: Arc::default(),
        })
    }

    /// Creates a pool from a preconfigured client, e.g. one with a custom TLS setup, client
    /// identity, or settings such as `output_format_arrow_string_as_string = 0`.
    ///
    /// `compute_context` identifies the server for join pushdown: tables of pools with the
    /// same context may be joined in a single query. No connectivity check is made.
    #[must_use]
    pub fn from_client(client: Client, compute_context: impl Into<String>) -> Self {
        Self {
            client,
            join_push_down: JoinPushDown::AllowedFor(compute_context.into()),
            schemas: Arc::default(),
        }
    }

    /// Use the provided `schemas` instead of inferring them from the database, replacing any
    /// set before. See [`Self::with_schema`].
    #[must_use]
    pub fn with_schemas(mut self, schemas: Arc<TableSchemas>) -> Self {
        self.schemas = schemas;
        self
    }

    /// Use the provided `schema` for `table_reference` instead of inferring it from the database.
    ///
    /// Call it once per table to provide the schemas of several tables. Table references are
    /// matched exactly, e.g. `users` and `public.users` are different tables.
    #[must_use]
    pub fn with_schema(
        mut self,
        table_reference: impl Into<TableReference>,
        schema: SchemaRef,
    ) -> Self {
        Arc::make_mut(&mut self.schemas).insert(table_reference.into(), schema);
        self
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// Returns a direct connection to the underlying database.
    pub fn connect_direct(&self) -> PoolResult<ClickHouseConnection> {
        let connection = <ClickHouseConnection as AsyncDbConnection<Client, ()>>::new_with_schemas(
            self.client(),
            Arc::clone(&self.schemas),
        );
        Ok(connection)
    }
}

#[async_trait::async_trait]
impl DbConnectionPool<Client, ()> for ClickHouseConnectionPool {
    async fn connect(&self) -> PoolResult<Box<dyn DbConnection<Client, ()>>> {
        self.connect_direct()
            .map(|c| Box::new(c) as Box<dyn DbConnection<Client, ()>>)
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

#[async_trait::async_trait]
impl NativeQueryExecutor for ClickHouseConnectionPool {
    async fn query(
        &self,
        sql: &str,
        params: &QueryParams,
        options: &QueryOptions,
    ) -> std::result::Result<SendableRecordBatchStream, GenericError> {
        self.connect_direct()?
            .query_arrow_with_params(sql, params, options, None)
            .await
    }
}

/// Lists a table's primary key (from `system.tables.primary_key`, as an index named
/// `PRIMARY` of kind `primary_key`) and its data-skipping indices (from
/// `system.data_skipping_indices`, whose `columns` hold the index expression). The database
/// defaults to the client's current database.
#[async_trait::async_trait]
impl IndexDiscovery for ClickHouseConnectionPool {
    async fn indexes(
        &self,
        table: &TableReference,
    ) -> std::result::Result<Vec<IndexInfo>, GenericError> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct KeyRow {
            primary_key: String,
        }
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct SkipIndexRow {
            name: String,
            #[serde(rename = "type")]
            type_: String,
            expr: String,
            granularity: u64,
        }

        let database_filter = "database = if(? = '', currentDatabase(), ?)";
        let database = table.schema().unwrap_or_default();

        let keys: Vec<KeyRow> = self
            .client
            .query(&format!(
                "SELECT primary_key FROM system.tables WHERE {database_filter} AND name = ?"
            ))
            .bind(database)
            .bind(database)
            .bind(table.table())
            .fetch_all()
            .await?;

        let mut indexes = Vec::new();
        if let Some(key) = keys.into_iter().next() {
            if !key.primary_key.is_empty() {
                indexes.push(IndexInfo {
                    name: "PRIMARY".to_string(),
                    columns: key
                        .primary_key
                        .split(',')
                        .map(|c| c.trim().to_string())
                        .collect(),
                    unique: false,
                    primary: true,
                    kind: Some("primary_key".to_string()),
                    definition: Some(key.primary_key),
                });
            }
        }

        let skip_indexes: Vec<SkipIndexRow> = self
            .client
            .query(&format!(
                "SELECT name, type, expr, granularity FROM system.data_skipping_indices \
                 WHERE {database_filter} AND table = ? ORDER BY name"
            ))
            .bind(database)
            .bind(database)
            .bind(table.table())
            .fetch_all()
            .await?;
        indexes.extend(skip_indexes.into_iter().map(|i| IndexInfo {
            definition: Some(format!(
                "INDEX {} {} TYPE {} GRANULARITY {}",
                i.name, i.expr, i.type_, i.granularity
            )),
            name: i.name,
            columns: vec![i.expr],
            unique: false,
            primary: false,
            kind: Some(i.type_),
        }));
        Ok(indexes)
    }
}
