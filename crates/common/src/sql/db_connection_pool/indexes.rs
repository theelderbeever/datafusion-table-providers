//! Provider-neutral index discovery.

use async_trait::async_trait;
use datafusion::common::TableReference;

use super::dbconnection::GenericError;

/// An index (or key) defined on a table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexInfo {
    pub name: String,
    /// Indexed columns (or expressions) in index order.
    pub columns: Vec<String>,
    pub unique: bool,
    pub primary: bool,
    /// Provider-specific index kind, e.g. `btree`, `gin`, `minmax`, `SAI`.
    pub kind: Option<String>,
    /// Full definition as reported by the source, when available.
    pub definition: Option<String>,
}

#[async_trait]
pub trait IndexDiscovery: Send + Sync {
    async fn indexes(&self, table: &TableReference) -> Result<Vec<IndexInfo>, GenericError>;
}
