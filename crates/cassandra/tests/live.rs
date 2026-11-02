//! Tests against a live Cassandra node. Set `CASSANDRA_TEST_NODE` (e.g. `127.0.0.1:9042`)
//! to run them. When the node advertises `127.0.0.1:9042` but is published on another
//! port, that address is translated to `CASSANDRA_TEST_NODE`.

use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, Int32Type};
use datafusion::common::TableReference;
use datafusion::scalar::ScalarValue;
use datafusion_table_providers_cassandra::params::CassandraHistoryListener;
use datafusion_table_providers_cassandra::pool::CassandraConnectionPool;
use datafusion_table_providers_common::sql::db_connection_pool::indexes::IndexDiscovery;
use datafusion_table_providers_common::sql::db_connection_pool::query::{
    NativeQueryExecutor, QueryOptions, QueryParams,
};
use datafusion_table_providers_common::SOURCE_TYPE_METADATA_KEY;
use futures::TryStreamExt;
use scylla::client::session_builder::SessionBuilder;
use scylla::observability::history::HistoryCollector;

#[tokio::test]
async fn native_query_executor_end_to_end() {
    let Ok(node) = std::env::var("CASSANDRA_TEST_NODE") else {
        eprintln!("CASSANDRA_TEST_NODE not set; skipping");
        return;
    };
    let node_addr: std::net::SocketAddr = node.parse().expect("CASSANDRA_TEST_NODE is ip:port");
    let advertised: std::net::SocketAddr = "127.0.0.1:9042".parse().unwrap();
    let translator: std::collections::HashMap<_, _> = [(advertised, node_addr), (node_addr, node_addr)].into();
    let session = SessionBuilder::new()
        .known_node(node)
        .address_translator(Arc::new(translator))
        .build()
        .await
        .unwrap();
    for cql in [
        "CREATE KEYSPACE IF NOT EXISTS dtp_live WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "DROP TABLE IF EXISTS dtp_live.t",
        "CREATE TABLE dtp_live.t (pk int, ck int, v text, PRIMARY KEY (pk, ck))",
        "CREATE INDEX t_v ON dtp_live.t (v)",
        "INSERT INTO dtp_live.t (pk, ck, v) VALUES (1, 1, 'a')",
        "INSERT INTO dtp_live.t (pk, ck, v) VALUES (1, 2, 'b')",
    ] {
        session.query_unpaged(cql, ()).await.unwrap();
    }
    let pool = CassandraConnectionPool::from_session(Arc::new(session));

    // Positional parameters coerced to the bind marker types (Int64 -> int).
    let stream = pool
        .query(
            "SELECT ck, v FROM dtp_live.t WHERE pk = ? AND ck = ?",
            &QueryParams::positional([ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))]),
            &QueryOptions::default(),
        )
        .await
        .unwrap();
    let schema = stream.schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(
        schema.field(1).metadata().get(SOURCE_TYPE_METADATA_KEY).map(String::as_str),
        Some("text")
    );
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    assert_eq!(batches[0].column(0).as_primitive::<Int32Type>().value(0), 2);

    // A history listener in the extensions sees the query.
    let collector = Arc::new(HistoryCollector::new());
    let options =
        QueryOptions::new().with_extension(CassandraHistoryListener(collector.clone()));
    let _: Vec<_> = pool
        .query("SELECT * FROM dtp_live.t", &QueryParams::default(), &options)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(!collector.clone_structured_history().requests.is_empty());

    // Empty results keep their schema.
    let stream = pool
        .query(
            "SELECT pk, v FROM dtp_live.t WHERE pk = 99",
            &QueryParams::default(),
            &QueryOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(stream.schema().fields().len(), 2);

    // Index discovery.
    let indexes = pool
        .indexes(&TableReference::partial("dtp_live", "t"))
        .await
        .unwrap();
    assert_eq!(indexes[0].columns, vec!["pk".to_string(), "ck".to_string()]);
    assert!(indexes[0].primary);
    assert!(indexes
        .iter()
        .any(|i| i.name == "t_v" && i.columns == vec!["v".to_string()]));
}
