//! Binding [`ScalarValue`] parameters to MySQL statements.

use std::collections::HashMap;

use chrono::{DateTime, Datelike, NaiveDate, Timelike};
use datafusion::scalar::ScalarValue;
use datafusion_table_providers_common::sql::db_connection_pool::query::{
    reject_settings, ParamError, QueryOptions, QueryParams,
};
use mysql_async::{Params, Value};

/// Converts a scalar to a MySQL value.
///
/// # Errors
///
/// Returns an error for scalar types without a MySQL equivalent.
pub fn scalar_to_mysql_value(v: &ScalarValue) -> Result<Value, ParamError> {
    if v.is_null() {
        return Ok(Value::NULL);
    }
    let unsupported = || ParamError::UnsupportedParamType {
        data_type: v.data_type().to_string(),
    };
    Ok(match v {
        ScalarValue::Boolean(Some(b)) => Value::Int(i64::from(*b)),
        ScalarValue::Int8(Some(x)) => Value::Int(i64::from(*x)),
        ScalarValue::Int16(Some(x)) => Value::Int(i64::from(*x)),
        ScalarValue::Int32(Some(x)) => Value::Int(i64::from(*x)),
        ScalarValue::Int64(Some(x)) => Value::Int(*x),
        ScalarValue::UInt8(Some(x)) => Value::UInt(u64::from(*x)),
        ScalarValue::UInt16(Some(x)) => Value::UInt(u64::from(*x)),
        ScalarValue::UInt32(Some(x)) => Value::UInt(u64::from(*x)),
        ScalarValue::UInt64(Some(x)) => Value::UInt(*x),
        ScalarValue::Float32(Some(x)) => Value::Float(*x),
        ScalarValue::Float64(Some(x)) => Value::Double(*x),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => {
            Value::Bytes(s.as_bytes().to_vec())
        }
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => Value::Bytes(b.clone()),
        // Decimals bind as their exact text form; MySQL converts it to DECIMAL.
        ScalarValue::Decimal128(Some(_), _, _) | ScalarValue::Decimal256(Some(_), _, _) => {
            Value::Bytes(v.to_string().into_bytes())
        }
        ScalarValue::Date32(Some(days)) => {
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|e| e.checked_add_signed(chrono::Duration::days(i64::from(*days))))
                .ok_or_else(unsupported)?;
            Value::Date(
                u16::try_from(date.year()).map_err(|_| unsupported())?,
                date.month() as u8,
                date.day() as u8,
                0,
                0,
                0,
                0,
            )
        }
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
            let dt = DateTime::from_timestamp_nanos(nanos).naive_utc();
            Value::Date(
                u16::try_from(dt.year()).map_err(|_| unsupported())?,
                dt.month() as u8,
                dt.day() as u8,
                dt.hour() as u8,
                dt.minute() as u8,
                dt.second() as u8,
                dt.nanosecond() / 1_000,
            )
        }
        ScalarValue::Time64Microsecond(Some(_)) | ScalarValue::Time64Nanosecond(Some(_)) => {
            let micros = match v {
                ScalarValue::Time64Microsecond(Some(x)) => *x,
                ScalarValue::Time64Nanosecond(Some(x)) => x / 1_000,
                _ => 0,
            };
            let negative = micros < 0;
            let micros = micros.unsigned_abs();
            let secs = micros / 1_000_000;
            Value::Time(
                negative,
                u32::try_from(secs / 86_400).map_err(|_| unsupported())?,
                ((secs / 3600) % 24) as u8,
                ((secs / 60) % 60) as u8,
                (secs % 60) as u8,
                (micros % 1_000_000) as u32,
            )
        }
        _ => return Err(unsupported()),
    })
}

/// Converts query parameters to MySQL parameters: positional for `?` placeholders, named
/// for `:name` placeholders. Mixing the two is an error, as are settings.
///
/// # Errors
///
/// Returns an error for mixed parameter styles, settings or unsupported scalar types.
pub fn to_mysql_params(params: &QueryParams, options: &QueryOptions) -> Result<Params, ParamError> {
    reject_settings(options)?;
    if !params.named.is_empty() {
        if !params.positional.is_empty() {
            return Err(ParamError::NamedParamsUnsupported {});
        }
        let named = params
            .named
            .iter()
            .map(|(k, v)| Ok((k.as_bytes().to_vec(), scalar_to_mysql_value(v)?)))
            .collect::<Result<HashMap<_, _>, ParamError>>()?;
        return Ok(Params::Named(named));
    }
    if params.positional.is_empty() {
        return Ok(Params::Empty);
    }
    Ok(Params::Positional(
        params
            .positional
            .iter()
            .map(scalar_to_mysql_value)
            .collect::<Result<_, _>>()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_scalars() {
        assert_eq!(
            scalar_to_mysql_value(&ScalarValue::Int32(Some(5))).unwrap(),
            Value::Int(5)
        );
        assert_eq!(
            scalar_to_mysql_value(&ScalarValue::Utf8(None)).unwrap(),
            Value::NULL
        );
        assert_eq!(
            scalar_to_mysql_value(&ScalarValue::Decimal128(Some(12345), 10, 2)).unwrap(),
            Value::Bytes(b"123.45".to_vec())
        );
        assert_eq!(
            scalar_to_mysql_value(&ScalarValue::Date32(Some(1))).unwrap(),
            Value::Date(1970, 1, 2, 0, 0, 0, 0)
        );
    }

    #[test]
    fn builds_params() {
        let p = to_mysql_params(
            &QueryParams::named([("id", ScalarValue::Int64(Some(1)))]),
            &QueryOptions::default(),
        )
        .unwrap();
        assert!(matches!(p, Params::Named(_)));
        let mixed = QueryParams::positional([ScalarValue::Int64(Some(1))])
            .with_named("a", ScalarValue::Int64(Some(2)));
        assert!(to_mysql_params(&mixed, &QueryOptions::default()).is_err());
    }
}
