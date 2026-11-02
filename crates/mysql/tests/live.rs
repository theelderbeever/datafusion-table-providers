//! Tests against a live MySQL server. Set `MYSQL_TEST_PORT` (user `root`, password from
//! `MYSQL_TEST_PASSWORD`, default `pw`, database `d` on localhost) to run them.

use std::collections::HashMap;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::common::TableReference;
use datafusion::scalar::ScalarValue;
use datafusion_table_providers_common::sql::db_connection_pool::indexes::IndexDiscovery;
use datafusion_table_providers_common::sql::db_connection_pool::query::{
    NativeQueryExecutor, QueryOptions, QueryParams,
};
use datafusion_table_providers_common::SOURCE_TYPE_METADATA_KEY;
use datafusion_table_providers_mysql::pool::MySQLConnectionPool;
use futures::TryStreamExt;
use secrecy::SecretString;

async fn pool() -> Option<MySQLConnectionPool> {
    let port = std::env::var("MYSQL_TEST_PORT").ok()?;
    let pass = std::env::var("MYSQL_TEST_PASSWORD").unwrap_or_else(|_| "pw".into());
    let params: HashMap<String, SecretString> = [
        ("host", "127.0.0.1".to_string()),
        ("tcp_port", port),
        ("user", "root".to_string()),
        ("pass", pass),
        ("db", "d".to_string()),
        ("sslmode", "disabled".to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), SecretString::from(v)))
    .collect();
    Some(MySQLConnectionPool::new(params).await.expect("pool"))
}

async fn run(
    pool: &MySQLConnectionPool,
    sql: &str,
    params: &QueryParams,
) -> (
    datafusion::arrow::datatypes::SchemaRef,
    Vec<datafusion::arrow::array::RecordBatch>,
) {
    let stream = pool
        .query(sql, params, &QueryOptions::default())
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let schema = stream.schema();
    (schema, stream.try_collect().await.unwrap())
}

#[tokio::test]
async fn native_query_executor_end_to_end() {
    let Some(pool) = pool().await else {
        eprintln!("MYSQL_TEST_PORT not set; skipping");
        return;
    };
    let none = QueryParams::default();

    run(&pool, "DROP TABLE IF EXISTS dtp_live", &none).await;
    run(
        &pool,
        "CREATE TABLE dtp_live (id BIGINT PRIMARY KEY, price DECIMAL(10,2), name VARCHAR(20), \
         UNIQUE KEY u_name (name))",
        &none,
    )
    .await;
    run(
        &pool,
        "INSERT INTO dtp_live VALUES (1, 1.50, 'a'), (2, 2.25, 'b')",
        &none,
    )
    .await;

    // Empty results keep their schema, with precision from the column metadata.
    let (schema, batches) =
        run(&pool, "SELECT id, price, name FROM dtp_live WHERE false", &none).await;
    assert!(batches.iter().all(|b| b.num_rows() == 0));
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Decimal128(10, 2));
    assert_eq!(
        schema.field(1).metadata().get(SOURCE_TYPE_METADATA_KEY).map(String::as_str),
        Some("decimal(10,2)")
    );

    // Positional and named parameters.
    let (_, batches) = run(
        &pool,
        "SELECT id FROM dtp_live WHERE id = ?",
        &QueryParams::positional([ScalarValue::Int32(Some(2))]),
    )
    .await;
    assert_eq!(batches[0].column(0).as_primitive::<Int64Type>().value(0), 2);
    let (_, batches) = run(
        &pool,
        "SELECT id FROM dtp_live WHERE name = :name",
        &QueryParams::named([("name", ScalarValue::Utf8(Some("a".into())))]),
    )
    .await;
    assert_eq!(batches[0].column(0).as_primitive::<Int64Type>().value(0), 1);

    // Index discovery.
    let indexes = pool
        .indexes(&TableReference::bare("dtp_live"))
        .await
        .unwrap();
    assert_eq!(indexes.len(), 2, "{indexes:?}");
    assert!(indexes[0].primary && indexes[0].columns == vec!["id".to_string()]);
    assert!(indexes[1].unique && indexes[1].columns == vec!["name".to_string()]);
}
