//! Provider-neutral bind parameters and per-query options for ad-hoc SQL.

use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, HashMap},
    fmt,
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{execution::SendableRecordBatchStream, scalar::ScalarValue};

use super::dbconnection::GenericError;

/// Bind parameters for a query, expressed as DataFusion [`ScalarValue`]s.
///
/// Positional parameters bind in order (`$1`, `?`, ...). Named parameters bind by name
/// (`:name`, `{name:Type}`, ...) for providers that support them; providers that don't
/// return an error when `named` is non-empty.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryParams {
    pub positional: Vec<ScalarValue>,
    pub named: BTreeMap<String, ScalarValue>,
}

impl QueryParams {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn positional(values: impl IntoIterator<Item = ScalarValue>) -> Self {
        Self {
            positional: values.into_iter().collect(),
            named: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn named<K: Into<String>>(values: impl IntoIterator<Item = (K, ScalarValue)>) -> Self {
        Self {
            positional: Vec::new(),
            named: values.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }

    #[must_use]
    pub fn with_positional(mut self, value: ScalarValue) -> Self {
        self.positional.push(value);
        self
    }

    #[must_use]
    pub fn with_named(mut self, name: impl Into<String>, value: ScalarValue) -> Self {
        self.named.insert(name.into(), value);
        self
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.positional.is_empty() && self.named.is_empty()
    }
}

/// A type-keyed map for provider-specific query options (e.g. a driver history listener).
#[derive(Clone, Default)]
pub struct Extensions {
    map: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl Extensions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a value, replacing any previous value of the same type.
    pub fn insert<T: Any + Send + Sync>(&mut self, value: T) {
        self.map.insert(TypeId::of::<T>(), Arc::new(value));
    }

    #[must_use]
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|v| v.downcast_ref::<T>())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl fmt::Debug for Extensions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Extensions")
            .field("len", &self.map.len())
            .finish()
    }
}

/// Per-query execution options.
///
/// Providers apply what they understand: ClickHouse sends `query_id` and `settings` with the
/// query, other providers ignore `query_id` and reject unknown `settings`.
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub query_id: Option<String>,
    pub settings: BTreeMap<String, String>,
    pub extensions: Extensions,
}

impl QueryOptions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_query_id(mut self, query_id: impl Into<String>) -> Self {
        self.query_id = Some(query_id.into());
        self
    }

    #[must_use]
    pub fn with_setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.insert(name.into(), value.into());
        self
    }

    #[must_use]
    pub fn with_extension<T: Any + Send + Sync>(mut self, value: T) -> Self {
        self.extensions.insert(value);
        self
    }
}

/// Runs ad-hoc SQL against a connection pool in the source database's own dialect.
#[async_trait]
pub trait NativeQueryExecutor: Send + Sync {
    async fn query(
        &self,
        sql: &str,
        params: &QueryParams,
        options: &QueryOptions,
    ) -> Result<SendableRecordBatchStream, GenericError>;
}

/// Error returned when a scalar or parameter style can't be bound by a provider.
#[derive(Debug, snafu::Snafu)]
#[snafu(visibility(pub))]
pub enum ParamError {
    #[snafu(display("Bind parameters are not supported by this provider"))]
    ParamsUnsupported {},

    #[snafu(display("Named bind parameters are not supported by this provider"))]
    NamedParamsUnsupported {},

    #[snafu(display("Unsupported bind parameter type: {data_type}"))]
    UnsupportedParamType { data_type: String },

    #[snafu(display("Unknown named bind parameter: {name}"))]
    UnknownNamedParam { name: String },

    #[snafu(display("Unsupported query setting: {name}"))]
    UnsupportedSetting { name: String },
}

/// Returns an error if `options` contains anything a provider that only supports extensions
/// can't apply (any `settings`). `query_id` is ignored.
pub fn reject_settings(options: &QueryOptions) -> Result<(), ParamError> {
    match options.settings.keys().next() {
        Some(name) => Err(ParamError::UnsupportedSetting { name: name.clone() }),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extensions_roundtrip() {
        #[derive(Debug, PartialEq)]
        struct Marker(u8);
        let opts = QueryOptions::new().with_extension(Marker(3));
        assert_eq!(opts.extensions.get::<Marker>(), Some(&Marker(3)));
        assert!(opts.extensions.get::<u8>().is_none());
    }

    #[test]
    fn params_builders() {
        let p = QueryParams::new()
            .with_positional(ScalarValue::Int64(Some(1)))
            .with_named("a", ScalarValue::Utf8(Some("x".into())));
        assert!(!p.is_empty());
        assert_eq!(p.positional.len(), 1);
        assert!(QueryParams::default().is_empty());
    }

    #[test]
    fn rejects_settings() {
        assert!(reject_settings(&QueryOptions::new().with_query_id("q")).is_ok());
        assert!(reject_settings(&QueryOptions::new().with_setting("a", "b")).is_err());
    }
}
