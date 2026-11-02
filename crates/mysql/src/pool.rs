use datafusion::execution::SendableRecordBatchStream;
use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::GenericError;
use datafusion_table_providers_common::sql::db_connection_pool::query::{NativeQueryExecutor, QueryOptions, QueryParams};
use crate::conn::MySQLConnection;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::TableReference;
use std::{collections::HashMap, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use mysql_async::{
    prelude::{Queryable, ToValue},
    ClientIdentity, DriverError, Metrics, Opts, Params, PoolConstraints, PoolOpts, Row, SslOpts,
    DEFAULT_POOL_CONSTRAINTS,
};
use secrecy::{ExposeSecret, SecretBox, SecretString};
use snafu::{ResultExt, Snafu};

use datafusion_table_providers_common::{
    sql::db_connection_pool::{
        dbconnection::{AsyncDbConnection, DbConnection, TableSchemas},
        JoinPushDown,
    },
    util::{self, ns_lookup::verify_ns_lookup_and_tcp_connect},
};

use datafusion_table_providers_common::sql::db_connection_pool::DbConnectionPool;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("MySQL connection failed.\n{source}\nFor further information, refer to the MySQL manual: https://dev.mysql.com/doc/mysql-errors/9.1/en/error-reference-introduction.html"))]
    MySQLConnectionError { source: mysql_async::Error },

    #[snafu(display(
        "Invalid MySQL connection string.\n{source}\nEnsure the MySQL connection string is valid"
    ))]
    InvalidConnectionString { source: mysql_async::UrlError },

    #[snafu(display("Invalid value for parameter {parameter_name}\nEnsure the value is valid for parameter {parameter_name}"))]
    InvalidParameterError { parameter_name: String },

    #[snafu(display("Invalid root cert path: {path}\nEnsure the root cert path is valid"))]
    InvalidRootCertPathError { path: String },

    #[snafu(display("Invalid client certificate, key or identity path: {path}"))]
    InvalidClientCertPathError { path: String },

    #[snafu(display(
        "'sslcert' and 'sslkey' must be provided together for client certificate authentication"
    ))]
    IncompleteClientIdentityError,

    #[snafu(display("Failed to read the client certificate or key.\n{source}"))]
    FailedToReadClientIdentityError { source: std::io::Error },

    #[snafu(display(
        "Failed to load the client certificate and key: {message}\nEnsure 'sslcert' is a PEM certificate (chain) and 'sslkey' a PEM private key"
    ))]
    FailedToLoadClientIdentityError { message: String },

    #[snafu(display("Cannot connect to MySQL on {host}:{port}. Ensure the host and port are correct and reachable."))]
    InvalidHostOrPortError {
        source: datafusion_table_providers_common::util::ns_lookup::Error,
        host: String,
        port: u16,
    },

    #[snafu(display("Authentication failed. Verify username and password."))]
    InvalidUsernameOrPassword,

    #[snafu(display("{message}\nEnsure the given MySQL database exists"))]
    UnknownMySQLDatabase { message: String },
}

#[derive(Debug, Clone)]
pub struct MySQLConnectionPool {
    pool: Arc<mysql_async::Pool>,
    join_push_down: JoinPushDown,
    schemas: Arc<TableSchemas>,
}

const SETUP_QUERIES: [&str; 4] = [
    "SET time_zone = '+00:00'",
    "SET character_set_results = 'utf8mb4'",
    "SET character_set_client = 'utf8mb4'",
    "SET character_set_connection = 'utf8mb4'",
];

impl MySQLConnectionPool {
    /// Creates a new instance of `MySQLConnectionPool`.
    ///
    /// # Arguments
    ///
    /// * `params` - A map of parameters to create the connection pool.
    ///   * `connection_string` - The connection string to use to connect to the MySQL database, or can be specified with the below individual parameters.
    ///   * `host` - The host of the MySQL database.
    ///   * `user` - The user to use when connecting to the MySQL database.
    ///   * `db` - The database to connect to.
    ///   * `pass` - The password to use when connecting to the MySQL database.
    ///   * `tcp_port` - The TCP port to use when connecting to the MySQL database.
    ///   * `sslmode` - The SSL mode to use when connecting to the MySQL database:
    ///     * `disabled` - no TLS.
    ///     * `required` (default) - TLS with full verification of the server certificate and
    ///       host name (same as `verify_identity`).
    ///     * `verify_ca` - TLS, the server certificate is verified against the root
    ///       certificates but the host name is not checked.
    ///     * `verify_identity` - TLS with full verification.
    ///     * `preferred` - TLS without any verification (kept for compatibility; insecure).
    ///   * `sslrootcert` - The path to the root certificate to use when connecting to the MySQL database.
    ///   * `sslcert`, `sslkey` - Paths to a PEM client certificate (chain) and private key for
    ///     client certificate authentication. Both are required together. Supported where
    ///     native-tls uses OpenSSL (not macOS/Windows/iOS); elsewhere use `sslidentity`.
    ///   * `sslidentity` - Path to a PKCS#12 archive with the client certificate and key, with
    ///     an optional `sslidentitypass`. An alternative to `sslcert`/`sslkey`.
    ///   * `pool_min` - The minimum number of connections to keep open in the pool, lazily created when requested.
    ///   * `pool_max` - The maximum number of connections to allow in the pool.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    #[allow(clippy::unused_async)]
    pub async fn new(params: HashMap<String, SecretString>) -> Result<Self> {
        // Remove the "mysql_" prefix from the keys to keep backward compatibility
        let params = util::remove_prefix_from_hashmap_keys(params, "mysql_");

        let mut connection_string = mysql_async::OptsBuilder::default();
        let mut ssl_mode = "required";
        let mut ssl_rootcert_path: Option<PathBuf> = None;

        if let Some(mysql_connection_string) = params
            .get("connection_string")
            .map(SecretBox::expose_secret)
        {
            connection_string = mysql_async::OptsBuilder::from_opts(
                mysql_async::Opts::from_url(mysql_connection_string)
                    .context(InvalidConnectionStringSnafu)?,
            );
        } else {
            if let Some(mysql_host) = params.get("host").map(SecretBox::expose_secret) {
                connection_string = connection_string.ip_or_hostname(mysql_host);
            }
            if let Some(mysql_user) = params.get("user").map(SecretBox::expose_secret) {
                connection_string = connection_string.user(Some(mysql_user));
            }
            if let Some(mysql_db) = params.get("db").map(SecretBox::expose_secret) {
                connection_string = connection_string.db_name(Some(mysql_db));
            }
            if let Some(mysql_pass) = params.get("pass").map(SecretBox::expose_secret) {
                connection_string = connection_string.pass(Some(mysql_pass));
            }
            if let Some(mysql_tcp_port) = params.get("tcp_port").map(SecretBox::expose_secret) {
                connection_string =
                    connection_string.tcp_port(mysql_tcp_port.parse::<u16>().unwrap_or(3306));
            }

            let pool_min = params
                .get("pool_min")
                .map(SecretBox::expose_secret)
                .unwrap_or_default()
                .parse::<usize>()
                .unwrap_or(DEFAULT_POOL_CONSTRAINTS.min());

            let pool_max = params
                .get("pool_max")
                .map(SecretBox::expose_secret)
                .unwrap_or_default()
                .parse::<usize>()
                .unwrap_or(DEFAULT_POOL_CONSTRAINTS.max());

            connection_string = connection_string
                .pool_opts(PoolOpts::default().with_constraints(
                    PoolConstraints::new(pool_min, pool_max).unwrap_or_default(),
                ));
        }

        if let Some(mysql_sslmode) = params.get("sslmode").map(SecretBox::expose_secret) {
            match mysql_sslmode.to_lowercase().as_str() {
                "disabled" | "required" | "preferred" | "verify_ca" | "verify_identity" => {
                    ssl_mode = mysql_sslmode;
                }
                _ => {
                    InvalidParameterSnafu {
                        parameter_name: "sslmode".to_string(),
                    }
                    .fail()?;
                }
            }
        }
        if let Some(mysql_sslrootcert) = params.get("sslrootcert").map(SecretBox::expose_secret) {
            if !std::path::Path::new(mysql_sslrootcert).exists() {
                InvalidRootCertPathSnafu {
                    path: mysql_sslrootcert,
                }
                .fail()?;
            }

            ssl_rootcert_path = Some(PathBuf::from(mysql_sslrootcert));
        }

        let ssl_mode = ssl_mode.to_lowercase();
        let identity = load_client_identity(
            params.get("sslcert").map(|v| v.expose_secret().to_string()),
            params.get("sslkey").map(|v| v.expose_secret().to_string()),
            params.get("sslidentity").map(|v| v.expose_secret().to_string()),
            params
                .get("sslidentitypass")
                .map(|v| v.expose_secret().to_string()),
        )?;
        let ssl_opts =
            get_ssl_opts(&ssl_mode, ssl_rootcert_path).map(|o| o.with_client_identity(identity));

        connection_string = connection_string.ssl_opts(ssl_opts);

        connection_string = connection_string.setup(SETUP_QUERIES.to_vec());

        let opts = mysql_async::Opts::from(connection_string);

        verify_mysql_opts(&opts).await?;

        let join_push_down = get_join_context(&opts);

        let pool = mysql_async::Pool::new(opts);

        // Test the connection
        let mut conn = pool.get_conn().await.map_err(|err| match err {
            // In case of an incorrect user name, the error `Unknown authentication plugin 'sha256_password'` is returned.
            // We override it with a more user-friendly error message.
            mysql_async::Error::Driver(DriverError::UnknownAuthPlugin { .. }) => {
                Error::InvalidUsernameOrPassword
            }
            mysql_async::Error::Server(server_error) => {
                match server_error.code {
                    // Code 1049: Server error: `ERROR 42000 (1049): Unknown database <database>
                    1049 => Error::UnknownMySQLDatabase {
                        message: server_error.message,
                    },
                    // Code 1045: Server error: ERROR 1045 (28000): Access denied for user <user> (using password: YES / NO)
                    1045 => Error::InvalidUsernameOrPassword,
                    _ => Error::MySQLConnectionError {
                        source: mysql_async::Error::Server(server_error),
                    },
                }
            }
            _ => Error::MySQLConnectionError { source: err },
        })?;

        let _rows: Vec<Row> = conn
            .exec("SELECT 1", Params::Empty)
            .await
            .context(MySQLConnectionSnafu)?;

        Ok(Self {
            pool: Arc::new(pool),
            join_push_down,
            schemas: Arc::default(),
        })
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

    fn new_connection(&self, conn: mysql_async::Conn) -> MySQLConnection {
        MySQLConnection::new_with_schemas(conn, Arc::clone(&self.schemas))
    }

    /// Returns a direct connection to the underlying database.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem creating the connection pool.
    pub async fn connect_direct(&self) -> Result<MySQLConnection> {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get_conn().await.context(MySQLConnectionSnafu)?;

        Ok(self.new_connection(conn))
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.pool.metrics()
    }
}

async fn verify_mysql_opts(opts: &Opts) -> Result<()> {
    // Verify the host and port are correct
    let host = opts.ip_or_hostname();
    let port = opts.tcp_port();

    verify_ns_lookup_and_tcp_connect(host, port)
        .await
        .context(InvalidHostOrPortSnafu { host, port })?;

    Ok(())
}

fn get_join_context(opts: &mysql_async::Opts) -> JoinPushDown {
    let mut join_context = format!("host={},port={}", opts.ip_or_hostname(), opts.tcp_port());
    if let Some(db_name) = opts.db_name() {
        join_context.push_str(&format!(",db={db_name}"));
    }
    if let Some(user) = opts.user() {
        join_context.push_str(&format!(",user={user}"));
    }

    JoinPushDown::AllowedFor(join_context)
}

/// Loads the client identity from either PEM `sslcert`/`sslkey` files or a PKCS#12
/// `sslidentity` archive.
fn load_client_identity(
    cert_path: Option<String>,
    key_path: Option<String>,
    identity_path: Option<String>,
    identity_pass: Option<String>,
) -> Result<Option<ClientIdentity>> {
    let check = |path: &str| -> Result<()> {
        if std::path::Path::new(path).exists() {
            Ok(())
        } else {
            InvalidClientCertPathSnafu { path }.fail()
        }
    };
    match (cert_path, key_path) {
        (Some(cert), Some(key)) => {
            check(&cert)?;
            check(&key)?;
            let cert = std::fs::read(&cert).context(FailedToReadClientIdentitySnafu)?;
            let key = std::fs::read(&key).context(FailedToReadClientIdentitySnafu)?;
            let der = pem_to_pkcs12(&cert, &key)?;
            Ok(Some(ClientIdentity::new(der.into()).with_password("")))
        }
        (None, None) => match identity_path {
            Some(path) => {
                check(&path)?;
                let identity = ClientIdentity::new(PathBuf::from(path).into());
                Ok(Some(match identity_pass {
                    Some(pass) => identity.with_password(pass),
                    None => identity,
                }))
            }
            None => Ok(None),
        },
        _ => IncompleteClientIdentitySnafu.fail(),
    }
}

/// Packs a PEM certificate (chain) and private key into a PKCS#12 archive with an empty
/// password, the identity format native-tls accepts.
#[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "ios")))]
fn pem_to_pkcs12(cert_pem: &[u8], key_pem: &[u8]) -> Result<Vec<u8>> {
    use openssl::{pkcs12::Pkcs12, pkey::PKey, stack::Stack, x509::X509};
    let err = |e: openssl::error::ErrorStack| Error::FailedToLoadClientIdentityError {
        message: e.to_string(),
    };
    let key = PKey::private_key_from_pem(key_pem).map_err(err)?;
    let mut certs = X509::stack_from_pem(cert_pem).map_err(err)?.into_iter();
    let Some(leaf) = certs.next() else {
        return FailedToLoadClientIdentitySnafu {
            message: "no certificate found in 'sslcert'",
        }
        .fail();
    };
    let mut builder = Pkcs12::builder();
    builder.name("client").pkey(&key).cert(&leaf);
    let mut chain = Stack::new().map_err(err)?;
    for cert in certs {
        chain.push(cert).map_err(err)?;
    }
    if !chain.is_empty() {
        builder.ca(chain);
    }
    builder.build2("").map_err(err)?.to_der().map_err(err)
}

#[cfg(any(target_os = "macos", target_os = "windows", target_os = "ios"))]
fn pem_to_pkcs12(_cert_pem: &[u8], _key_pem: &[u8]) -> Result<Vec<u8>> {
    FailedToLoadClientIdentitySnafu {
        message: "PEM 'sslcert'/'sslkey' are not supported on this platform; use a PKCS#12 'sslidentity'",
    }
    .fail()
}

fn get_ssl_opts(ssl_mode: &str, rootcert_path: Option<PathBuf>) -> Option<SslOpts> {
    if ssl_mode == "disabled" {
        return None;
    }

    let mut opts = SslOpts::default();

    if let Some(rootcert_path) = rootcert_path {
        let path = rootcert_path;
        opts = opts.with_root_certs(vec![path.into()]);
    }

    // mysql_async does not have a "ssl_mode" https://github.com/blackbeam/mysql_async/issues/225#issuecomment-1409922237
    match ssl_mode {
        // Accept invalid certs and skip domain validation.
        "preferred" => {
            opts = opts
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true);
        }
        // Verify the certificate chain but not the host name.
        "verify_ca" => {
            opts = opts.with_danger_skip_domain_validation(true);
        }
        // "required" and "verify_identity": full verification.
        _ => {}
    }

    Some(opts)
}

#[async_trait]
impl DbConnectionPool<mysql_async::Conn, &'static (dyn ToValue + Sync)> for MySQLConnectionPool {
    async fn connect(
        &self,
    ) -> std::result::Result<
        Box<dyn DbConnection<mysql_async::Conn, &'static (dyn ToValue + Sync)>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn = pool
            .get_conn()
            .await
            .context(MySQLConnectionSnafu)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(Box::new(self.new_connection(conn)))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

#[async_trait]
impl NativeQueryExecutor for MySQLConnectionPool {
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

#[cfg(test)]
mod tls_tests {
    use super::*;

    #[test]
    fn client_identity_requires_cert_and_key() {
        assert!(load_client_identity(None, None, None, None).unwrap().is_none());
        assert!(matches!(
            load_client_identity(Some("/c.pem".into()), None, None, None),
            Err(Error::IncompleteClientIdentityError)
        ));
        assert!(matches!(
            load_client_identity(Some("/nope/c".into()), Some("/nope/k".into()), None, None),
            Err(Error::InvalidClientCertPathError { .. })
        ));
    }

    #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "ios")))]
    #[test]
    fn loads_pem_client_identity() {
        use openssl::{asn1::Asn1Time, hash::MessageDigest, pkey::PKey, rsa::Rsa, x509};
        let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        let mut name = x509::X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", "client").unwrap();
        let name = name.build();
        let mut builder = x509::X509Builder::new().unwrap();
        builder.set_version(2).unwrap();
        builder.set_subject_name(&name).unwrap();
        builder.set_issuer_name(&name).unwrap();
        builder.set_pubkey(&key).unwrap();
        builder
            .set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        builder
            .set_not_after(&Asn1Time::days_from_now(1).unwrap())
            .unwrap();
        builder.sign(&key, MessageDigest::sha256()).unwrap();
        let cert = builder.build();

        let dir = std::env::temp_dir().join(format!("dtp_mysql_tls_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert_path = dir.join("client.crt");
        let key_path = dir.join("client.key");
        std::fs::write(&cert_path, cert.to_pem().unwrap()).unwrap();
        std::fs::write(&key_path, key.private_key_to_pem_pkcs8().unwrap()).unwrap();

        let der = pem_to_pkcs12(
            &std::fs::read(&cert_path).unwrap(),
            &std::fs::read(&key_path).unwrap(),
        )
        .unwrap();
        native_tls::Identity::from_pkcs12(&der, "").expect("native-tls accepts the archive");
        assert!(load_client_identity(
            Some(cert_path.to_string_lossy().into()),
            Some(key_path.to_string_lossy().into()),
            None,
            None,
        )
        .unwrap()
        .is_some());
        std::fs::remove_dir_all(&dir).ok();
    }
}
