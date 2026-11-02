//! Binding [`ScalarValue`] parameters and [`QueryOptions`] to ClickHouse queries.

use clickhouse::query::Query;
use datafusion::scalar::ScalarValue;
use datafusion_table_providers_common::sql::db_connection_pool::query::{
    ParamError, QueryOptions, QueryParams,
};
use serde::{Serialize, Serializer};

/// The ClickHouse setting carrying the query id.
pub const QUERY_ID_SETTING: &str = "query_id";

/// A [`ScalarValue`] in a form the ClickHouse client can serialize.
///
/// Decimals bind as their exact text, dates as `YYYY-MM-DD` and timestamps as
/// `YYYY-MM-DD HH:MM:SS[.fffffffff]` (UTC), which ClickHouse parses into the target types.
#[derive(Debug, Clone, PartialEq)]
pub enum ChParam {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
}

impl Serialize for ChParam {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            ChParam::Null => s.serialize_none(),
            ChParam::Bool(b) => s.serialize_bool(*b),
            ChParam::Int(i) => s.serialize_i64(*i),
            ChParam::UInt(u) => s.serialize_u64(*u),
            ChParam::Float(f) => s.serialize_f64(*f),
            ChParam::String(v) => s.serialize_str(v),
        }
    }
}

/// Converts a scalar to a ClickHouse parameter.
///
/// # Errors
///
/// Returns an error for scalar types without a ClickHouse equivalent, and for binary values
/// that aren't valid UTF-8 (the client binds strings only).
pub fn scalar_to_ch_param(v: &ScalarValue) -> Result<ChParam, ParamError> {
    if v.is_null() {
        return Ok(ChParam::Null);
    }
    let unsupported = || ParamError::UnsupportedParamType {
        data_type: v.data_type().to_string(),
    };
    Ok(match v {
        ScalarValue::Boolean(Some(b)) => ChParam::Bool(*b),
        ScalarValue::Int8(Some(x)) => ChParam::Int(i64::from(*x)),
        ScalarValue::Int16(Some(x)) => ChParam::Int(i64::from(*x)),
        ScalarValue::Int32(Some(x)) => ChParam::Int(i64::from(*x)),
        ScalarValue::Int64(Some(x)) => ChParam::Int(*x),
        ScalarValue::UInt8(Some(x)) => ChParam::UInt(u64::from(*x)),
        ScalarValue::UInt16(Some(x)) => ChParam::UInt(u64::from(*x)),
        ScalarValue::UInt32(Some(x)) => ChParam::UInt(u64::from(*x)),
        ScalarValue::UInt64(Some(x)) => ChParam::UInt(*x),
        ScalarValue::Float32(Some(x)) => ChParam::Float(f64::from(*x)),
        ScalarValue::Float64(Some(x)) => ChParam::Float(*x),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => {
            ChParam::String(s.clone())
        }
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => {
            ChParam::String(String::from_utf8(b.clone()).map_err(|_| unsupported())?)
        }
        ScalarValue::Decimal128(Some(_), _, _)
        | ScalarValue::Decimal256(Some(_), _, _)
        | ScalarValue::Date32(Some(_)) => ChParam::String(v.to_string()),
        ScalarValue::TimestampSecond(Some(_), _)
        | ScalarValue::TimestampMillisecond(Some(_), _)
        | ScalarValue::TimestampMicrosecond(Some(_), _)
        | ScalarValue::TimestampNanosecond(Some(_), _) => {
            let nanos = match v {
                ScalarValue::TimestampSecond(Some(x), _) => x.checked_mul(1_000_000_000),
                ScalarValue::TimestampMillisecond(Some(x), _) => x.checked_mul(1_000_000),
                ScalarValue::TimestampMicrosecond(Some(x), _) => x.checked_mul(1_000),
                ScalarValue::TimestampNanosecond(Some(x), _) => Some(*x),
                _ => None,
            }
            .ok_or_else(unsupported)?;
            let dt = chrono::DateTime::from_timestamp_nanos(nanos).naive_utc();
            ChParam::String(dt.format("%Y-%m-%d %H:%M:%S%.f").to_string())
        }
        _ => return Err(unsupported()),
    })
}

/// Applies parameters and options to `query`.
///
/// Positional parameters replace `?` placeholders client-side; named parameters are sent as
/// server-side query parameters for `{name:Type}` placeholders. `query_id` and `settings` are
/// sent as ClickHouse settings.
///
/// # Errors
///
/// Returns an error for unsupported scalar types.
pub fn apply_params_and_options(
    mut query: Query,
    params: &QueryParams,
    options: &QueryOptions,
) -> Result<Query, ParamError> {
    for value in &params.positional {
        query = query.bind(scalar_to_ch_param(value)?);
    }
    for (name, value) in &params.named {
        query = query.param(name, scalar_to_ch_param(value)?);
    }
    if let Some(query_id) = &options.query_id {
        query = query.with_setting(QUERY_ID_SETTING, query_id.clone());
    }
    for (name, value) in &options.settings {
        query = query.with_setting(name.clone(), value.clone());
    }
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_scalars() {
        assert_eq!(
            scalar_to_ch_param(&ScalarValue::Int32(Some(1))).unwrap(),
            ChParam::Int(1)
        );
        assert_eq!(
            scalar_to_ch_param(&ScalarValue::Decimal128(Some(150), 5, 2)).unwrap(),
            ChParam::String("1.50".to_string())
        );
        assert_eq!(
            scalar_to_ch_param(&ScalarValue::TimestampSecond(Some(0), None)).unwrap(),
            ChParam::String("1970-01-01 00:00:00".to_string())
        );
        assert!(scalar_to_ch_param(&ScalarValue::Binary(Some(vec![0xff]))).is_err());
    }
}
