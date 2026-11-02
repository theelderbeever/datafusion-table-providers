use crate::conn::{PostgresConnection, PostgresPooledConnection};
use datafusion::execution::SendableRecordBatchStream;
use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::GenericError;
use datafusion_table_providers_common::sql::db_connection_pool::query::{NativeQueryExecutor, QueryOptions, QueryParams};
use arrow::datatypes::SchemaRef;
use datafusion::common::TableReference;
use std::{collections::HashMap, path::PathBuf, str::FromStr, sync::Arc};

use async_trait::async_trait;
use bb8::ErrorSink;
use bb8_postgres::tokio_postgres::{config::Host, types::ToSql, Config};
use datafusion_table_providers_common::{
    util::{self, ns_lookup::verify_ns_lookup_and_tcp_connect},
    UnsupportedTypeAction,
};
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use secrecy::{ExposeSecret, SecretBox, SecretString};
use snafu::{prelude::*, ResultExt};
use tokio::runtime::Handle;
use tokio_postgres;

use datafusion_table_providers_common::sql::db_connection_pool::{
    dbconnection::{AsyncDbConnection, DbConnection, TableSchemas},
    runtime::run_async_with_tokio,
    DbConnectionPool, JoinPushDown, PasswordProvider, StaticPasswordProvider,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("PostgreSQL connection failed.\n{source}\nFor details, refer to the PostgreSQL documentation: https://www.postgresql.org/docs/17/index.html"))]
    ConnectionPoolError {
        source: bb8_postgres::tokio_postgres::Error,
    },

    #[snafu(display("PostgreSQL connection failed.\n{source}\nAdjust the connection pool parameters for sufficient capacity."))]
    ConnectionPoolRunError {
        source: bb8::RunError<bb8_postgres::tokio_postgres::Error>,
    },

    #[snafu(display(
        "Invalid parameter: {parameter_name}. Ensure the parameter name is correct."
    ))]
    InvalidParameterError { parameter_name: String },

    #[snafu(display("Could not parse {parameter_name} into a valid integer. Ensure it is configured with a valid value."))]
    InvalidIntegerParameterError {
        parameter_name: String,
        source: std::num::ParseIntError,
    },

    #[snafu(display("Cannot connect to PostgreSQL on {host}:{port}. Ensure the host and port are correct and reachable."))]
    InvalidHostOrPortError {
        source: datafusion_table_providers_common::util::ns_lookup::Error,
        host: String,
        port: u16,
    },

    #[snafu(display(
        "Invalid root certificate path: {path}. Ensure it points to a valid root certificate."
    ))]
    InvalidRootCertPathError { path: String },

    #[snafu(display(
        "Failed to read certificate.\n{source}\nEnsure the root certificate path points to a valid certificate."
    ))]
    FailedToReadCertError { source: std::io::Error },

    #[snafu(display(
        "Certificate loading failed.\n{source}\nEnsure the root certificate path points to a valid certificate."
    ))]
    FailedToLoadCertError { source: native_tls::Error },

    #[snafu(display(
        "Invalid client certificate or key path: {path}. Ensure 'sslcert' and 'sslkey' point to PEM files."
    ))]
    InvalidClientCertPathError { path: String },

    #[snafu(display(
        "'sslcert' and 'sslkey' must be provided together for client certificate authentication."
    ))]
    IncompleteClientIdentityError,

    #[snafu(display(
        "Failed to read the client certificate or key.\n{source}"
    ))]
    FailedToReadClientIdentityError { source: std::io::Error },

    #[snafu(display(
        "Failed to load the client certificate and key.\n{source}\nEnsure 'sslcert' is a PEM certificate (chain) and 'sslkey' a PEM PKCS#8 private key ('BEGIN PRIVATE KEY')."
    ))]
    FailedToLoadClientIdentityError { source: native_tls::Error },

    #[snafu(display("TLS connector initialization failed.\n{source}\nVerify SSL mode and root certificate validity"))]
    FailedToBuildTlsConnectorError { source: native_tls::Error },

    #[snafu(display("PostgreSQL connection failed.\n{source}\nFor details, refer to the PostgreSQL documentation: https://www.postgresql.org/docs/17/index.html"))]
    PostgresConnectionError { source: tokio_postgres::Error },

    #[snafu(display("Authentication failed. Verify username and password."))]
    InvalidUsernameOrPassword { source: tokio_postgres::Error },

    #[snafu(display("Password provider error.\n{source}"))]
    PasswordProviderError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Task failed to execute on IO runtime.\n{source}"))]
    IoRuntimeError { source: tokio::task::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error type for the connection manager, covering both Postgres and password provider errors.
#[derive(Debug)]
pub enum ConnectionManagerError {
    /// An error from the underlying Postgres connection.
    Postgres(tokio_postgres::Error),
    /// An error from the password provider.
    PasswordProvider(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for ConnectionManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres(e) => write!(f, "{e}"),
            Self::PasswordProvider(e) => write!(f, "password provider error: {e}"),
        }
    }
}

impl std::error::Error for ConnectionManagerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Postgres(e) => Some(e),
            Self::PasswordProvider(e) => Some(e.as_ref()),
        }
    }
}

impl From<tokio_postgres::Error> for ConnectionManagerError {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::Postgres(e)
    }
}

/// A bb8 connection manager that supports dynamic password providers.
///
/// When a [`PasswordProvider`] is set, the manager calls it to get a fresh password
/// each time a new connection is created. This enables rotating credentials,
/// JWT-based auth, and cloud IAM authentication.
///
/// When no provider is set (passwordless auth), the manager connects using the
/// stored [`Config`] as-is.
pub struct ConnectionManager {
    config: Config,
    tls: MakeTlsConnector,
    password_provider: Option<Arc<dyn PasswordProvider>>,
}

impl ConnectionManager {
    fn new(config: Config, tls: MakeTlsConnector) -> Self {
        Self {
            config,
            tls,
            password_provider: None,
        }
    }

    fn with_password_provider(mut self, provider: Arc<dyn PasswordProvider>) -> Self {
        self.password_provider = Some(provider);
        self
    }
}

/// Applies per-connection session configuration after a connection is established.
///
/// Redshift surfaces Spectrum complex external columns (`ARRAY`/`STRUCT`/`MAP`) and
/// `SUPER` values as JSON text only when `json_serialization_enable` is on; otherwise
/// selecting such a column errors server-side. `json_serialization_parse_nested_strings`
/// additionally renders nested string fields that hold valid JSON inline (unescaped)
/// rather than as escaped string literals, so the decoded values match their schema.
///
/// These parameters only exist on Redshift, so rather than spend a `SELECT version()`
/// round-trip detecting the variant we apply them optimistically and treat vanilla
/// PostgreSQL's "unrecognized configuration parameter" error (`SQLSTATE 42704`) as a
/// no-op. The `SET`s run outside a transaction, so a failure leaves the connection
/// usable.
///
/// See <https://docs.aws.amazon.com/redshift/latest/dg/r_json_serialization_enable.html>
/// and <https://docs.aws.amazon.com/redshift/latest/dg/r_json_serialization_parse_nested_strings.html>.
async fn configure_session(
    client: &tokio_postgres::Client,
) -> std::result::Result<(), ConnectionManagerError> {
    if let Err(e) = client
        .batch_execute(
            "SET json_serialization_enable TO true; \
             SET json_serialization_parse_nested_strings TO true;",
        )
        .await
    {
        // Vanilla PostgreSQL rejects these unknown parameters with `undefined_object`
        // (42704); that just means "not Redshift", so ignore it. Anything else is real.
        if e.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_OBJECT) {
            return Ok(());
        }
        return Err(e.into());
    }

    Ok(())
}

impl bb8::ManageConnection for ConnectionManager {
    type Connection = tokio_postgres::Client;
    type Error = ConnectionManagerError;

    async fn connect(&self) -> std::result::Result<tokio_postgres::Client, ConnectionManagerError> {
        let (client, connection) = if let Some(provider) = &self.password_provider {
            let password = provider
                .get_password()
                .await
                .map_err(ConnectionManagerError::PasswordProvider)?;
            let mut config = self.config.clone();
            config.password(password.expose_secret());
            config.connect(self.tls.clone()).await?
        } else {
            self.config.connect(self.tls.clone()).await?
        };
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::debug!("postgres connection error: {e}");
            }
        });

        configure_session(&client).await?;

        Ok(client)
    }

    async fn is_valid(
        &self,
        conn: &mut tokio_postgres::Client,
    ) -> std::result::Result<(), ConnectionManagerError> {
        conn.simple_query("").await.map(|_| ())?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut tokio_postgres::Client) -> bool {
        conn.is_closed()
    }
}

#[derive(Debug)]
pub struct PostgresConnectionPool {
    pool: Arc<bb8::Pool<ConnectionManager>>,
    join_push_down: JoinPushDown,
    unsupported_type_action: UnsupportedTypeAction,
    io_handle: Option<Handle>,
    schemas: Arc<TableSchemas>,
}

impl PostgresConnectionPool {
    /// Creates a new instance of `PostgresConnectionPool`.
    ///
    /// If a `pass` parameter is present, it is wrapped in a [`StaticPasswordProvider`]
    /// internally. For dynamic credentials, use [`new_with_password_provider`](Self::new_with_password_provider).
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        Self::new_inner(params, None).await
    }

    /// Creates a new instance of `PostgresConnectionPool` with a dynamic password provider.
    ///
    /// The password provider is called each time a new connection is created in the pool,
    /// enabling support for rotating credentials, JWT tokens, and cloud IAM authentication.
    ///
    /// Any `pass` parameter in `params` is ignored; the provider is used instead.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn new_with_password_provider(
        params: HashMap<String, SecretString>,
        password_provider: Arc<dyn PasswordProvider>,
    ) -> Result<Self> {
        Self::new_inner(params, Some(password_provider)).await
    }

    async fn new_inner(
        params: HashMap<String, SecretString>,
        password_provider: Option<Arc<dyn PasswordProvider>>,
    ) -> Result<Self> {
        // Remove the "pg_" prefix from the keys to keep backward compatibility
        let params = util::remove_prefix_from_hashmap_keys(params, "pg_");

        let mut connection_string = String::new();
        let mut ssl_mode = "verify-full".to_string();
        let mut ssl_rootcert_path: Option<PathBuf> = None;
        let mut static_password: Option<SecretString> = None;

        if let Some(pg_connection_string) = params
            .get("connection_string")
            .map(SecretBox::expose_secret)
        {
            let (str, mode, cert_path, password) = parse_connection_string(pg_connection_string);
            connection_string = str;
            ssl_mode = mode;
            if password_provider.is_none() {
                static_password = password.map(SecretString::from);
            }
            if let Some(cert_path) = cert_path {
                let sslrootcert = cert_path.as_str();
                ensure!(
                    std::path::Path::new(sslrootcert).exists(),
                    InvalidRootCertPathSnafu { path: cert_path }
                );
                ssl_rootcert_path = Some(PathBuf::from(sslrootcert));
            }
        } else {
            if let Some(pg_host) = params.get("host").map(SecretBox::expose_secret) {
                connection_string.push_str(format!("host={pg_host} ").as_str());
            }
            if let Some(pg_user) = params.get("user").map(SecretBox::expose_secret) {
                connection_string.push_str(format!("user={pg_user} ").as_str());
            }
            if let Some(pg_db) = params.get("db").map(SecretBox::expose_secret) {
                connection_string.push_str(format!("dbname={pg_db} ").as_str());
            }
            if password_provider.is_none() {
                if let Some(pg_pass) = params.get("pass") {
                    static_password = Some(pg_pass.clone());
                }
            }
            if let Some(pg_port) = params.get("port").map(SecretBox::expose_secret) {
                connection_string.push_str(format!("port={pg_port} ").as_str());
            }
        }

        if let Some(pg_sslmode) = params.get("sslmode").map(SecretBox::expose_secret) {
            match pg_sslmode.to_lowercase().as_str() {
                "disable" | "require" | "prefer" | "verify-ca" | "verify-full" => {
                    ssl_mode = pg_sslmode.to_string();
                }
                _ => {
                    InvalidParameterSnafu {
                        parameter_name: "sslmode".to_string(),
                    }
                    .fail()?;
                }
            }
        }
        if let Some(pg_sslrootcert) = params.get("sslrootcert").map(SecretBox::expose_secret) {
            ensure!(
                std::path::Path::new(pg_sslrootcert).exists(),
                InvalidRootCertPathSnafu {
                    path: pg_sslrootcert,
                }
            );

            ssl_rootcert_path = Some(PathBuf::from(pg_sslrootcert));
        }

        // Client certificate authentication (mTLS): `sslcert`/`sslkey` from the parameters
        // take precedence over the connection string.
        let (conn_str_cert, conn_str_key) = params
            .get("connection_string")
            .map(SecretBox::expose_secret)
            .map(|s| extract_client_cert_params(s))
            .unwrap_or_default();
        let ssl_cert_path = params
            .get("sslcert")
            .map(|v| v.expose_secret().to_string())
            .or(conn_str_cert);
        let ssl_key_path = params
            .get("sslkey")
            .map(|v| v.expose_secret().to_string())
            .or(conn_str_key);
        let identity = load_client_identity(ssl_cert_path, ssl_key_path).await?;

        let mode = match ssl_mode.as_str() {
            "disable" => "disable",
            "prefer" => "prefer",
            // tokio_postgres supports only disable, require and prefer
            _ => "require",
        };

        // Password is never included in the connection string — it flows
        // through the PasswordProvider on each connection instead.
        connection_string.push_str(format!("sslmode={mode} ").as_str());
        let mut config =
            Config::from_str(connection_string.as_str()).context(ConnectionPoolSnafu)?;

        apply_optional_session_params(&mut config, &params);

        verify_postgres_config(&config).await?;

        let mut certs: Option<Vec<Certificate>> = None;

        if let Some(path) = ssl_rootcert_path {
            let buf = tokio::fs::read(path).await.context(FailedToReadCertSnafu)?;
            certs = Some(parse_certs(&buf)?);
        }

        let tls_connector = get_tls_connector(ssl_mode.as_str(), certs, identity)?;
        let connector = MakeTlsConnector::new(tls_connector);

        // Resolve the password provider: use the caller's, wrap the static password,
        // or leave as None for passwordless auth (trust, cert, etc.).
        let password_provider = password_provider.or_else(|| {
            static_password
                .map(|pw| Arc::new(StaticPasswordProvider::new(pw)) as Arc<dyn PasswordProvider>)
        });

        // Test the connection
        if let Some(ref provider) = password_provider {
            let password = provider
                .get_password()
                .await
                .map_err(|source| Error::PasswordProviderError { source })?;
            let mut test_config = config.clone();
            test_config.password(password.expose_secret());
            test_connection(&test_config, connector.clone()).await?;
        } else {
            test_connection(&config, connector.clone()).await?;
        }

        let join_push_down = get_join_context(&config);

        let mut manager = ConnectionManager::new(config, connector);
        if let Some(provider) = password_provider {
            manager = manager.with_password_provider(provider);
        }
        let error_sink = PostgresErrorSink::new();

        let mut connection_pool_size = 10; // The BB8 default is 10
        if let Some(pg_pool_size) = params
            .get("connection_pool_size")
            .map(SecretBox::expose_secret)
        {
            connection_pool_size = pg_pool_size.parse().context(InvalidIntegerParameterSnafu {
                parameter_name: "pool_size".to_string(),
            })?;
        }

        let pool = bb8::Pool::builder()
            .max_size(connection_pool_size)
            .error_sink(Box::new(error_sink))
            .build(manager)
            .await
            .map_err(map_pool_build_error)?;

        // Verify the pool by executing a simple query
        {
            let conn = pool.get().await.map_err(map_pool_run_error)?;
            conn.execute("SELECT 1", &[])
                .await
                .context(ConnectionPoolSnafu)?;
        }

        Ok(PostgresConnectionPool {
            pool: Arc::new(pool),
            join_push_down,
            unsupported_type_action: UnsupportedTypeAction::default(),
            io_handle: None,
            schemas: Arc::default(),
        })
    }

    /// Specify the action to take when an invalid type is encountered.
    #[must_use]
    pub fn with_unsupported_type_action(mut self, action: UnsupportedTypeAction) -> Self {
        self.unsupported_type_action = action;
        self
    }

    /// Route all Postgres connection background tasks to a dedicated IO runtime.
    #[must_use]
    pub fn with_io_runtime(mut self, handle: Handle) -> Self {
        self.io_handle = Some(handle);
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

    fn new_connection(&self, conn: PostgresPooledConnection) -> PostgresConnection {
        PostgresConnection::new_with_schemas(conn, Arc::clone(&self.schemas))
    }

    /// Returns a direct connection to the underlying database.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn connect_direct(&self) -> Result<PostgresConnection> {
        let pool = Arc::clone(&self.pool);
        let conn = if let Some(handle) = &self.io_handle {
            handle
                .spawn(async move { pool.get_owned().await.map_err(map_pool_run_error) })
                .await
                .context(IoRuntimeSnafu)??
        } else {
            pool.get_owned().await.map_err(map_pool_run_error)?
        };
        Ok(self.new_connection(conn))
    }
}

/// Parses a connection string into components, extracting `sslmode`, `sslrootcert`,
/// and `password` separately so they can be handled by the caller.
fn parse_connection_string(
    pg_connection_string: &str,
) -> (String, String, Option<String>, Option<String>) {
    let mut connection_string = String::new();
    let mut ssl_mode = "verify-full".to_string();
    let mut ssl_rootcert_path: Option<String> = None;
    let mut password: Option<String> = None;

    let str_params: Vec<&str> = pg_connection_string.split_whitespace().collect();
    for param in str_params {
        let param = param.split('=').collect::<Vec<&str>>();
        if let (Some(&name), Some(&value)) = (param.first(), param.get(1)) {
            match name {
                "sslmode" => {
                    ssl_mode = value.to_string();
                }
                "sslrootcert" => {
                    ssl_rootcert_path = Some(value.to_string());
                }
                // Handled by `extract_client_cert_params`.
                "sslcert" | "sslkey" => {}
                "password" => {
                    password = Some(value.to_string());
                }
                _ => {
                    connection_string.push_str(format!("{name}={value} ").as_str());
                }
            }
        }
    }

    (connection_string, ssl_mode, ssl_rootcert_path, password)
}

/// Apply the optional string session params libpq forwards verbatim to the
/// backend — `application_name` and `options` — onto a parsed [`Config`]. Split
/// out of `new_inner` so it is unit-testable without a live server.
///
/// `options` is the libpq `options` connection parameter: arbitrary command-line
/// switches sent at connection time (e.g. `-c statement_timeout=5000`), applied via
/// [`Config::options`]. It lets a caller pin session-level GUCs
/// (`statement_timeout`, `default_transaction_read_only`, …) that `Config` has no
/// dedicated setter for.
fn apply_optional_session_params(config: &mut Config, params: &HashMap<String, SecretString>) {
    if let Some(application_name) = params.get("application_name").map(SecretBox::expose_secret) {
        config.application_name(application_name);
    }
    if let Some(options) = params.get("options").map(SecretBox::expose_secret) {
        config.options(options);
    }
}

fn get_join_context(config: &Config) -> JoinPushDown {
    let mut join_push_context_str = String::new();
    for host in config.get_hosts() {
        join_push_context_str.push_str(&format!("host={host:?},"));
    }
    if !config.get_ports().is_empty() {
        join_push_context_str.push_str(&format!("port={port},", port = config.get_ports()[0]));
    }
    if let Some(dbname) = config.get_dbname() {
        join_push_context_str.push_str(&format!("db={dbname},"));
    }
    if let Some(user) = config.get_user() {
        join_push_context_str.push_str(&format!("user={user},"));
    }

    JoinPushDown::AllowedFor(join_push_context_str)
}

/// Classifies a connection error, returning a more specific error variant for
/// authentication failures.
fn classify_connection_error(err: tokio_postgres::Error) -> Error {
    if let Some(code) = err.code() {
        if *code == tokio_postgres::error::SqlState::INVALID_PASSWORD {
            return Error::InvalidUsernameOrPassword { source: err };
        }
    }
    Error::PostgresConnectionError { source: err }
}

async fn test_connection(config: &Config, connector: MakeTlsConnector) -> Result<()> {
    config
        .connect(connector)
        .await
        .map(|_| ())
        .map_err(classify_connection_error)
}

fn map_pool_build_error(e: ConnectionManagerError) -> Error {
    match e {
        ConnectionManagerError::Postgres(e) => Error::ConnectionPoolError { source: e },
        ConnectionManagerError::PasswordProvider(e) => Error::PasswordProviderError { source: e },
    }
}

fn map_pool_run_error(e: bb8::RunError<ConnectionManagerError>) -> Error {
    match e {
        bb8::RunError::User(ConnectionManagerError::Postgres(e)) => Error::ConnectionPoolRunError {
            source: bb8::RunError::User(e),
        },
        bb8::RunError::User(ConnectionManagerError::PasswordProvider(e)) => {
            Error::PasswordProviderError { source: e }
        }
        bb8::RunError::TimedOut => Error::ConnectionPoolRunError {
            source: bb8::RunError::TimedOut,
        },
    }
}

async fn verify_postgres_config(config: &Config) -> Result<()> {
    for host in config.get_hosts() {
        for port in config.get_ports() {
            if let Host::Tcp(host) = host {
                verify_ns_lookup_and_tcp_connect(host, *port)
                    .await
                    .context(InvalidHostOrPortSnafu { host, port: *port })?;
            }
        }
    }

    Ok(())
}

/// Extracts `sslcert` and `sslkey` from a libpq-style connection string.
fn extract_client_cert_params(pg_connection_string: &str) -> (Option<String>, Option<String>) {
    let mut cert = None;
    let mut key = None;
    for param in pg_connection_string.split_whitespace() {
        match param.split_once('=') {
            Some(("sslcert", v)) => cert = Some(v.to_string()),
            Some(("sslkey", v)) => key = Some(v.to_string()),
            _ => {}
        }
    }
    (cert, key)
}

/// Loads a client identity from PEM files: `cert_path` holds the certificate (optionally
/// followed by its chain) and `key_path` a PKCS#8 private key. Both or neither must be set.
async fn load_client_identity(
    cert_path: Option<String>,
    key_path: Option<String>,
) -> Result<Option<Identity>> {
    let (cert_path, key_path) = match (cert_path, key_path) {
        (None, None) => return Ok(None),
        (Some(cert), Some(key)) => (cert, key),
        _ => return IncompleteClientIdentitySnafu.fail(),
    };
    for path in [&cert_path, &key_path] {
        ensure!(
            std::path::Path::new(path).exists(),
            InvalidClientCertPathSnafu { path: path.clone() }
        );
    }
    let cert = tokio::fs::read(&cert_path)
        .await
        .context(FailedToReadClientIdentitySnafu)?;
    let key = tokio::fs::read(&key_path)
        .await
        .context(FailedToReadClientIdentitySnafu)?;
    Identity::from_pkcs8(&cert, &key)
        .map(Some)
        .context(FailedToLoadClientIdentitySnafu)
}

fn get_tls_connector(
    ssl_mode: &str,
    rootcerts: Option<Vec<Certificate>>,
    identity: Option<Identity>,
) -> Result<TlsConnector> {
    let mut builder = TlsConnector::builder();

    if ssl_mode == "disable" {
        return builder.build().context(FailedToBuildTlsConnectorSnafu);
    }

    if let Some(identity) = identity {
        builder.identity(identity);
    }

    if let Some(certs) = rootcerts {
        for cert in certs {
            builder.add_root_certificate(cert);
        }
    }

    builder
        .danger_accept_invalid_hostnames(ssl_mode != "verify-full")
        .danger_accept_invalid_certs(ssl_mode != "verify-full" && ssl_mode != "verify-ca")
        .build()
        .context(FailedToBuildTlsConnectorSnafu)
}

fn parse_certs(buf: &[u8]) -> Result<Vec<Certificate>> {
    Certificate::from_der(buf)
        .map(|x| vec![x])
        .or_else(|_| {
            pem::parse_many(buf)
                .unwrap_or_default()
                .iter()
                .map(pem::encode)
                .map(|s| Certificate::from_pem(s.as_bytes()))
                .collect()
        })
        .context(FailedToLoadCertSnafu)
}

#[derive(Debug, Clone, Copy)]
struct PostgresErrorSink {}

impl PostgresErrorSink {
    pub fn new() -> Self {
        PostgresErrorSink {}
    }
}

impl<E> ErrorSink<E> for PostgresErrorSink
where
    E: std::fmt::Debug,
    E: std::fmt::Display,
{
    fn sink(&self, error: E) {
        tracing::debug!("Postgres Pool Error: {}", error);
    }

    fn boxed_clone(&self) -> Box<dyn ErrorSink<E>> {
        Box::new(*self)
    }
}

#[async_trait]
impl
    DbConnectionPool<bb8::PooledConnection<'static, ConnectionManager>, &'static (dyn ToSql + Sync)>
    for PostgresConnectionPool
{
    async fn connect(
        &self,
    ) -> std::result::Result<
        Box<
            dyn DbConnection<
                bb8::PooledConnection<'static, ConnectionManager>,
                &'static (dyn ToSql + Sync),
            >,
        >,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn = if let Some(handle) = &self.io_handle {
            handle
                .spawn(async move { pool.get_owned().await.map_err(map_pool_run_error) })
                .await
                .context(IoRuntimeSnafu)??
        } else {
            let get_conn = async || pool.get_owned().await.map_err(map_pool_run_error);
            run_async_with_tokio(get_conn).await?
        };
        Ok(Box::new(
            self.new_connection(conn)
                .with_unsupported_type_action(self.unsupported_type_action),
        ))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_client_cert_params() {
        let (cert, key) =
            extract_client_cert_params("host=h sslcert=/c.pem sslkey=/k.pem sslmode=verify-full");
        assert_eq!(cert.as_deref(), Some("/c.pem"));
        assert_eq!(key.as_deref(), Some("/k.pem"));
        let (conn_str, ..) = parse_connection_string("host=h sslcert=/c.pem sslkey=/k.pem");
        assert_eq!(conn_str.trim(), "host=h");
    }

    #[tokio::test]
    async fn client_identity_requires_cert_and_key() {
        assert!(load_client_identity(None, None).await.unwrap().is_none());
        assert!(matches!(
            load_client_identity(Some("/c.pem".into()), None).await,
            Err(Error::IncompleteClientIdentityError)
        ));
        assert!(matches!(
            load_client_identity(Some("/nope/c.pem".into()), Some("/nope/k.pem".into())).await,
            Err(Error::InvalidClientCertPathError { .. })
        ));
    }
    use secrecy::ExposeSecret;

    #[tokio::test]
    async fn static_password_provider_returns_password() {
        let provider = StaticPasswordProvider::new(SecretString::from("hunter2".to_string()));
        let password = provider.get_password().await.unwrap();
        assert_eq!(password.expose_secret(), "hunter2");
    }

    #[test]
    fn connection_manager_error_display() {
        let err = ConnectionManagerError::PasswordProvider("token expired".into());
        assert_eq!(err.to_string(), "password provider error: token expired");
    }

    #[test]
    fn connection_manager_error_implements_std_error() {
        let err: Box<dyn std::error::Error> =
            Box::new(ConnectionManagerError::PasswordProvider("fail".into()));
        assert!(err.source().is_some());
    }

    #[test]
    fn parse_connection_string_extracts_password() {
        let (conn_str, ssl_mode, cert_path, password) = parse_connection_string(
            "host=localhost user=postgres password=secret dbname=mydb sslmode=disable",
        );
        assert_eq!(conn_str.trim(), "host=localhost user=postgres dbname=mydb");
        assert_eq!(ssl_mode, "disable");
        assert!(cert_path.is_none());
        assert_eq!(password.as_deref(), Some("secret"));
    }

    #[test]
    fn parse_connection_string_without_password() {
        let (conn_str, _ssl_mode, _cert_path, password) =
            parse_connection_string("host=localhost user=postgres dbname=mydb");
        assert_eq!(conn_str.trim(), "host=localhost user=postgres dbname=mydb");
        assert!(password.is_none());
    }

    #[test]
    fn options_param_reaches_the_config() {
        let mut params: HashMap<String, SecretString> = HashMap::new();
        params.insert(
            "options".to_string(),
            SecretString::from("-c statement_timeout=5000".to_string()),
        );
        params.insert(
            "application_name".to_string(),
            SecretString::from("semvia".to_string()),
        );

        let mut config = Config::new();
        apply_optional_session_params(&mut config, &params);

        assert_eq!(config.get_options(), Some("-c statement_timeout=5000"));
        assert_eq!(config.get_application_name(), Some("semvia"));
    }

    #[test]
    fn absent_options_param_leaves_the_config_untouched() {
        let params: HashMap<String, SecretString> = HashMap::new();

        let mut config = Config::new();
        apply_optional_session_params(&mut config, &params);

        assert_eq!(config.get_options(), None);
        assert_eq!(config.get_application_name(), None);
    }
}

#[async_trait]
impl NativeQueryExecutor for PostgresConnectionPool {
    async fn query(
        &self,
        sql: &str,
        params: &QueryParams,
        options: &QueryOptions,
    ) -> std::result::Result<SendableRecordBatchStream, GenericError> {
        let conn = self.connect_direct().await?;
        conn.query_arrow_with_params(sql, params, options, None)
            .await
    }
}
