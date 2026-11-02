//! Tests against a live Postgres server. Set `POSTGRES_TEST_PORT` (user `postgres`, password
//! from `POSTGRES_TEST_PASSWORD`, default `pw`, on localhost) to run them.

use std::collections::HashMap;

use datafusion::arrow::array::{Array, AsArray, ListArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Int32Type};
use datafusion::common::TableReference;
use datafusion::scalar::ScalarValue;
use datafusion_table_providers_common::sql::db_connection_pool::indexes::IndexDiscovery;
use datafusion_table_providers_common::sql::db_connection_pool::query::{
    NativeQueryExecutor, QueryOptions, QueryParams,
};
use datafusion_table_providers_common::{SOURCE_ENCODING_METADATA_KEY, SOURCE_TYPE_METADATA_KEY};
use datafusion_table_providers_postgres::pool::PostgresConnectionPool;
use futures::TryStreamExt;
use secrecy::SecretString;

async fn pool() -> Option<PostgresConnectionPool> {
    let port = std::env::var("POSTGRES_TEST_PORT").ok()?;
    let pass = std::env::var("POSTGRES_TEST_PASSWORD").unwrap_or_else(|_| "pw".into());
    let params: HashMap<String, SecretString> = [
        ("host", "localhost".to_string()),
        ("port", port),
        ("user", "postgres".to_string()),
        ("pass", pass),
        ("db", "postgres".to_string()),
        ("sslmode", "disable".to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), SecretString::from(v)))
    .collect();
    Some(PostgresConnectionPool::new(params).await.expect("pool"))
}

async fn run(
    pool: &PostgresConnectionPool,
    sql: &str,
    params: &QueryParams,
) -> (datafusion::arrow::datatypes::SchemaRef, Vec<datafusion::arrow::array::RecordBatch>) {
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
        eprintln!("POSTGRES_TEST_PORT not set; skipping");
        return;
    };
    let none = QueryParams::default();

    // Empty results keep their schema.
    let (schema, batches) = run(&pool, "SELECT 1::int4 AS a, 'x'::text AS b WHERE false", &none).await;
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert!(batches.iter().all(|b| b.num_rows() == 0));

    // numeric mapping.
    let (schema, batches) = run(
        &pool,
        "SELECT 1.5::numeric(10,2) AS d128, \
                12345678901234567890123456789012345678901234.12345678::numeric(60,8) AS d256, \
                1::numeric(77,0) AS wide, \
                0.1234567890123456789012345678901234567890::numeric AS bare",
        &none,
    )
    .await;
    assert_eq!(schema.field(0).data_type(), &DataType::Decimal128(10, 2));
    assert_eq!(schema.field(1).data_type(), &DataType::Decimal256(60, 8));
    assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
    assert_eq!(
        schema.field(3).metadata().get(SOURCE_ENCODING_METADATA_KEY).map(String::as_str),
        Some("text")
    );
    assert_eq!(
        schema.field(0).metadata().get(SOURCE_TYPE_METADATA_KEY).map(String::as_str),
        Some("numeric(10,2)")
    );
    let batch = &batches[0];
    assert_eq!(batch.schema(), schema);
    let bare = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(bare.value(0), "0.1234567890123456789012345678901234567890");
    let d256 = batch.column(1).as_primitive::<datafusion::arrow::datatypes::Decimal256Type>();
    assert_eq!(
        d256.value_as_string(0),
        "12345678901234567890123456789012345678901234.12345678"
    );

    // NaN in a decimal column is an error.
    assert!(pool
        .query("SELECT 'NaN'::numeric(10,2) AS n", &none, &QueryOptions::default())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .is_err());

    // Arrays with NULL elements.
    let (_, batches) = run(&pool, "SELECT ARRAY[1,NULL]::int4[] AS a", &none).await;
    let list = batches[0].column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.value(0);
    let values = values.as_primitive::<Int32Type>();
    assert_eq!(values.len(), 2);
    assert!(values.is_null(1));

    // void.
    let (schema, batches) = run(&pool, "SELECT pg_sleep(0) AS v", &none).await;
    assert_eq!(schema.field(0).data_type(), &DataType::Null);
    assert_eq!(batches[0].num_rows(), 1);

    // Positional parameters, coerced to the placeholder types.
    let params = QueryParams::positional([
        ScalarValue::Int64(Some(41)),
        ScalarValue::Utf8(Some("2024-01-02".into())),
        ScalarValue::Utf8(None),
    ]);
    let (schema, batches) = run(
        &pool,
        "SELECT $1::int4 + 1 AS n, $2::date AS d, $3::text AS t",
        &params,
    )
    .await;
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(
        batches[0].column(0).as_primitive::<Int32Type>().value(0),
        42
    );
    assert!(batches[0].column(2).is_null(0));

    // Named parameters are rejected.
    assert!(pool
        .query(
            "SELECT 1",
            &QueryParams::named([("a", ScalarValue::Int32(Some(1)))]),
            &QueryOptions::default()
        )
        .await
        .is_err());

}

#[tokio::test]
async fn index_discovery() {
    let Some(pool) = pool().await else {
        eprintln!("POSTGRES_TEST_PORT not set; skipping");
        return;
    };
    let conn = pool.connect_direct().await.unwrap();
    conn.conn
        .batch_execute(
            "DROP TABLE IF EXISTS dtp_live_ix; \
             CREATE TABLE dtp_live_ix (id int PRIMARY KEY, a text); \
             CREATE INDEX dtp_live_ix_a ON dtp_live_ix (lower(a));",
        )
        .await
        .unwrap();
    let indexes = pool
        .indexes(&TableReference::bare("dtp_live_ix"))
        .await
        .unwrap();
    assert_eq!(indexes.len(), 2);
    assert!(indexes[0].primary && indexes[0].columns == vec!["id".to_string()]);
    assert_eq!(indexes[1].columns, vec!["lower(a)".to_string()]);
    assert_eq!(indexes[1].kind.as_deref(), Some("btree"));
}

#[tokio::test]
async fn varchar_arrays_and_json_preserve_values_and_schemas() {
    let Some(pool) = pool().await else {
        eprintln!("POSTGRES_TEST_PORT not set; skipping");
        return;
    };
    let params = QueryParams::new();
    let sql = "SELECT a, j, b FROM (VALUES \
        (ARRAY['alpha', NULL, 'é']::varchar[], '{\"key\": [1, null]}'::json, '{\"key\": [1, null]}'::jsonb), \
        (ARRAY[]::varchar[], 'null'::json, 'null'::jsonb), \
        (NULL::varchar[], NULL::json, NULL::jsonb)) AS v(a,j,b)";
    let (schema, batches) = run(&pool, sql, &params).await;
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::List(std::sync::Arc::new(
            datafusion::arrow::datatypes::Field::new("item", DataType::Utf8, true)
        ))
    );
    for (i, native) in [(0, "_varchar"), (1, "json"), (2, "jsonb")] {
        assert_eq!(
            schema
                .field(i)
                .metadata()
                .get(SOURCE_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some(native)
        );
    }
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.schema(), schema);
    let arrays = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let first = arrays.value(0);
    let first = first.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        first.iter().collect::<Vec<_>>(),
        vec![Some("alpha"), None, Some("é")]
    );
    assert_eq!(arrays.value_length(1), 0);
    assert!(arrays.is_null(2));
    let json = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(json.value(0), "{\"key\": [1, null]}");
    assert_eq!(json.value(1), "null");
    assert!(json.is_null(2));
    let jsonb = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(jsonb.value(0), "{\"key\": [1, null]}");
    assert_eq!(jsonb.value(1), "null");
    assert!(jsonb.is_null(2));

    let (empty_schema, empty) = run(
        &pool,
        &format!("SELECT * FROM ({sql}) q WHERE false"),
        &params,
    )
    .await;
    assert_eq!(empty_schema, schema);
    assert!(empty.iter().all(|batch| batch.num_rows() == 0));

    let (stream_schema, streamed) = run(
        &pool,
        &format!("SELECT q.* FROM ({sql}) q CROSS JOIN generate_series(1, 3000)"),
        &params,
    )
    .await;
    assert_eq!(stream_schema, schema);
    assert!(streamed.len() > 1);
    assert_eq!(
        streamed.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        9000
    );
    for batch in &streamed {
        assert_eq!(batch.schema(), schema);
    }

    // Catalog inference must accept JSONB under the default unsupported-type policy.
    run(
        &pool,
        "CREATE TABLE quill_json_array_types (a varchar[], j json, b jsonb)",
        &params,
    )
    .await;
    let pool = std::sync::Arc::new(pool);
    let factory = datafusion_table_providers_postgres::PostgresTableFactory::new(pool.clone());
    let result = factory
        .table_provider(TableReference::partial("public", "quill_json_array_types"))
        .await;
    run(&pool, "DROP TABLE quill_json_array_types", &params).await;
    let table = result.expect("catalog schema supports varchar arrays and JSON/JSONB");
    assert_eq!(
        table.schema().field(0).data_type(),
        schema.field(0).data_type()
    );
    assert_eq!(table.schema().field(1).data_type(), &DataType::Utf8);
    assert_eq!(table.schema().field(2).data_type(), &DataType::Utf8);
}
