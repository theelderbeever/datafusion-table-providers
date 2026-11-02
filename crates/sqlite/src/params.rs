//! Binding [`ScalarValue`] parameters to SQLite statements.

use datafusion::scalar::ScalarValue;
use datafusion_table_providers_common::sql::db_connection_pool::query::{
    reject_settings, ParamError, QueryOptions, QueryParams,
};
use rusqlite::types::Value;

/// Converts a scalar to an owned SQLite value.
///
/// Dates and timestamps bind as ISO-8601 text, decimals as their exact text form.
///
/// # Errors
///
/// Returns an error for scalar types without a SQLite equivalent.
pub fn scalar_to_sqlite_value(v: &ScalarValue) -> Result<Value, ParamError> {
    if v.is_null() {
        return Ok(Value::Null);
    }
    Ok(match v {
        ScalarValue::Boolean(Some(b)) => Value::Integer(i64::from(*b)),
        ScalarValue::Int8(Some(x)) => Value::Integer(i64::from(*x)),
        ScalarValue::Int16(Some(x)) => Value::Integer(i64::from(*x)),
        ScalarValue::Int32(Some(x)) => Value::Integer(i64::from(*x)),
        ScalarValue::Int64(Some(x)) => Value::Integer(*x),
        ScalarValue::UInt8(Some(x)) => Value::Integer(i64::from(*x)),
        ScalarValue::UInt16(Some(x)) => Value::Integer(i64::from(*x)),
        ScalarValue::UInt32(Some(x)) => Value::Integer(i64::from(*x)),
        ScalarValue::UInt64(Some(x)) => {
            Value::Integer(i64::try_from(*x).map_err(|_| ParamError::UnsupportedParamType {
                data_type: "UInt64 value larger than i64::MAX".to_string(),
            })?)
        }
        ScalarValue::Float32(Some(x)) => Value::Real(f64::from(*x)),
        ScalarValue::Float64(Some(x)) => Value::Real(*x),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => {
            Value::Text(s.clone())
        }
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => Value::Blob(b.clone()),
        ScalarValue::Decimal128(Some(_), _, _)
        | ScalarValue::Decimal256(Some(_), _, _)
        | ScalarValue::Date32(Some(_))
        | ScalarValue::Date64(Some(_))
        | ScalarValue::Time64Microsecond(Some(_))
        | ScalarValue::Time64Nanosecond(Some(_))
        | ScalarValue::TimestampSecond(Some(_), _)
        | ScalarValue::TimestampMillisecond(Some(_), _)
        | ScalarValue::TimestampMicrosecond(Some(_), _)
        | ScalarValue::TimestampNanosecond(Some(_), _) => Value::Text(v.to_string()),
        _ => {
            return Err(ParamError::UnsupportedParamType {
                data_type: v.data_type().to_string(),
            })
        }
    })
}

/// Owned SQLite parameters: positional values (bound to `?`/`?NNN` in order) and named
/// values (bound to `:name`, `@name` or `$name`).
#[derive(Debug, Clone, Default)]
pub struct SqliteParams {
    pub positional: Vec<Value>,
    pub named: Vec<(String, Value)>,
}

/// Converts query parameters to owned SQLite values. Settings are rejected.
///
/// # Errors
///
/// Returns an error for settings or unsupported scalar types.
pub fn to_sqlite_params(
    params: &QueryParams,
    options: &QueryOptions,
) -> Result<SqliteParams, ParamError> {
    reject_settings(options)?;
    Ok(SqliteParams {
        positional: params
            .positional
            .iter()
            .map(scalar_to_sqlite_value)
            .collect::<Result<_, _>>()?,
        named: params
            .named
            .iter()
            .map(|(k, v)| Ok((k.clone(), scalar_to_sqlite_value(v)?)))
            .collect::<Result<_, ParamError>>()?,
    })
}

/// Binds `params` to `stmt`. Named parameters may be given with or without their prefix.
///
/// # Errors
///
/// Returns an error if a named parameter doesn't appear in the statement or binding fails.
pub fn bind_sqlite_params(
    stmt: &mut rusqlite::Statement<'_>,
    params: &SqliteParams,
) -> rusqlite::Result<()> {
    for (i, value) in params.positional.iter().enumerate() {
        stmt.raw_bind_parameter(i + 1, value)?;
    }
    for (name, value) in &params.named {
        let candidates = [
            name.clone(),
            format!(":{name}"),
            format!("@{name}"),
            format!("${name}"),
        ];
        let mut index = None;
        for candidate in &candidates {
            if let Some(i) = stmt.parameter_index(candidate)? {
                index = Some(i);
                break;
            }
        }
        let Some(index) = index else {
            return Err(rusqlite::Error::InvalidParameterName(name.clone()));
        };
        stmt.raw_bind_parameter(index, value)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_positional_and_named() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT ? + :b").unwrap();
        let params = to_sqlite_params(
            &QueryParams::positional([ScalarValue::Int64(Some(2))])
                .with_named("b", ScalarValue::Int32(Some(3))),
            &QueryOptions::default(),
        )
        .unwrap();
        bind_sqlite_params(&mut stmt, &params).unwrap();
        let mut rows = stmt.raw_query();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row.get::<_, i64>(0).unwrap(), 5);
    }

    #[test]
    fn unknown_named_param_is_an_error() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let mut stmt = conn.prepare("SELECT :a").unwrap();
        let params = to_sqlite_params(
            &QueryParams::named([("nope", ScalarValue::Int32(Some(3)))]),
            &QueryOptions::default(),
        )
        .unwrap();
        assert!(bind_sqlite_params(&mut stmt, &params).is_err());
    }
}
