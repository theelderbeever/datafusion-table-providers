//! Tests against a live ClickHouse server. Set `CLICKHOUSE_TEST_URL` (e.g.
//! `http://localhost:8123`) to run them; they are skipped otherwise.

use clickhouse::Client;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::TableReference;
use datafusion::scalar::ScalarValue;
use datafusion_table_providers_clickhouse::pool::ClickHouseConnectionPool;
use datafusion_table_providers_common::sql::db_connection_pool::indexes::IndexDiscovery;
use datafusion_table_providers_common::sql::db_connection_pool::query::{
    NativeQueryExecutor, QueryOptions, QueryParams,
};
use futures::TryStreamExt;

fn client() -> Option<Client> {
    let url = std::env::var("CLICKHOUSE_TEST_URL").ok()?;
    Some(Client::default().with_url(url))
}

#[tokio::test]
async fn native_query_executor_end_to_end() {
    let Some(client) = client() else {
        eprintln!("CLICKHOUSE_TEST_URL not set; skipping");
        return;
    };
    let pool = ClickHouseConnectionPool::from_client(
        client
            .clone()
            .with_setting("output_format_arrow_string_as_string", "0"),
        "live-test",
    );
    let none = QueryParams::default();
    let opts = QueryOptions::default();

    client
        .query("DROP TABLE IF EXISTS dtp_live")
        .execute()
        .await
        .unwrap();
    client
        .query(
            "CREATE TABLE dtp_live (id UInt32, s String, INDEX is s TYPE bloom_filter GRANULARITY 1) \
             ENGINE = MergeTree ORDER BY id",
        )
        .execute()
        .await
        .unwrap();
    client
        .query("INSERT INTO dtp_live VALUES (1, 'a'), (2, unhex('FF'))")
        .execute()
        .await
        .unwrap();

    // Empty results keep their schema.
    let stream = pool
        .query("SELECT id, s FROM dtp_live WHERE false", &none, &opts)
        .await
        .unwrap();
    assert_eq!(stream.schema().fields().len(), 2);

    // Strings stay binary with string_as_string = 0.
    let batches: Vec<_> = pool
        .query("SELECT s FROM dtp_live ORDER BY id", &none, &opts)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Binary);

    // Positional and named parameters.
    let params = QueryParams::positional([ScalarValue::UInt32(Some(1))])
        .with_named("s", ScalarValue::Utf8(Some("a".into())));
    let batches: Vec<_> = pool
        .query(
            "SELECT count() AS c FROM dtp_live WHERE id = ? AND s = {s:String}",
            &params,
            &opts,
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);

    // query_id is sent with the query.
    let query_id = format!("dtp-live-{}", std::process::id());
    let _: Vec<_> = pool
        .query("SELECT 1", &none, &QueryOptions::new().with_query_id(&query_id))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    client.query("SYSTEM FLUSH LOGS").execute().await.unwrap();
    let n: u64 = client
        .query("SELECT count() FROM system.query_log WHERE query_id = ?")
        .bind(&query_id)
        .fetch_one()
        .await
        .unwrap();
    assert!(n > 0, "query_id not found in system.query_log");

    // Index discovery.
    let indexes = pool
        .indexes(&TableReference::bare("dtp_live"))
        .await
        .unwrap();
    assert!(indexes.iter().any(|i| i.primary && i.columns == vec!["id".to_string()]));
    assert!(indexes
        .iter()
        .any(|i| i.name == "is" && i.kind.as_deref() == Some("bloom_filter")));
}
