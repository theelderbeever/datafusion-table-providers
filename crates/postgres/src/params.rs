//! Binding [`ScalarValue`] parameters to Postgres statements.

use std::error::Error;
use std::str::FromStr;

use bytes::BytesMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use datafusion::scalar::ScalarValue;
use datafusion_table_providers_common::sql::db_connection_pool::query::{
    reject_settings, ParamError, QueryOptions, QueryParams,
};
use rust_decimal::Decimal;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};

type BoxError = Box<dyn Error + Sync + Send>;

/// A [`ScalarValue`] bound as a Postgres parameter.
///
/// The value is converted to the parameter type the server inferred for its placeholder
/// (e.g. an `Int64` bound to an `int4` placeholder is narrowed with a range check), so
/// callers don't need to match Postgres' types exactly. NULLs bind to any type.
#[derive(Debug, Clone)]
pub struct PgParam(pub ScalarValue);

/// Converts query parameters to Postgres parameters.
///
/// # Errors
///
/// Returns an error for named parameters (Postgres only has positional `$n`), settings, or
/// unsupported scalar types.
pub fn to_pg_params(
    params: &QueryParams,
    options: &QueryOptions,
) -> Result<Vec<PgParam>, ParamError> {
    if !params.named.is_empty() {
        return Err(ParamError::NamedParamsUnsupported {});
    }
    reject_settings(options)?;
    params
        .positional
        .iter()
        .map(|v| {
            if is_supported(v) {
                Ok(PgParam(v.clone()))
            } else {
                Err(ParamError::UnsupportedParamType {
                    data_type: v.data_type().to_string(),
                })
            }
        })
        .collect()
}

fn is_supported(v: &ScalarValue) -> bool {
    v.is_null()
        || matches!(
            v,
            ScalarValue::Boolean(_)
                | ScalarValue::Int8(_)
                | ScalarValue::Int16(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
                | ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Float32(_)
                | ScalarValue::Float64(_)
                | ScalarValue::Utf8(_)
                | ScalarValue::LargeUtf8(_)
                | ScalarValue::Utf8View(_)
                | ScalarValue::Binary(_)
                | ScalarValue::LargeBinary(_)
                | ScalarValue::BinaryView(_)
                | ScalarValue::FixedSizeBinary(_, _)
                | ScalarValue::Decimal128(_, _, _)
                | ScalarValue::Date32(_)
                | ScalarValue::Time64Microsecond(_)
                | ScalarValue::Time64Nanosecond(_)
                | ScalarValue::TimestampSecond(_, _)
                | ScalarValue::TimestampMillisecond(_, _)
                | ScalarValue::TimestampMicrosecond(_, _)
                | ScalarValue::TimestampNanosecond(_, _)
        )
}

fn as_i128(v: &ScalarValue) -> Option<i128> {
    Some(match v {
        ScalarValue::Int8(Some(x)) => i128::from(*x),
        ScalarValue::Int16(Some(x)) => i128::from(*x),
        ScalarValue::Int32(Some(x)) => i128::from(*x),
        ScalarValue::Int64(Some(x)) => i128::from(*x),
        ScalarValue::UInt8(Some(x)) => i128::from(*x),
        ScalarValue::UInt16(Some(x)) => i128::from(*x),
        ScalarValue::UInt32(Some(x)) => i128::from(*x),
        ScalarValue::UInt64(Some(x)) => i128::from(*x),
        ScalarValue::Boolean(Some(b)) => i128::from(*b),
        _ => return None,
    })
}

#[allow(clippy::cast_precision_loss)]
fn as_f64(v: &ScalarValue) -> Option<f64> {
    match v {
        ScalarValue::Float32(Some(x)) => Some(f64::from(*x)),
        ScalarValue::Float64(Some(x)) => Some(*x),
        _ => as_i128(v).map(|x| x as f64),
    }
}

fn as_str(v: &ScalarValue) -> Option<&str> {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => {
            Some(s)
        }
        _ => None,
    }
}

fn as_bytes(v: &ScalarValue) -> Option<&[u8]> {
    match v {
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => Some(b),
        _ => None,
    }
}

fn as_decimal(v: &ScalarValue) -> Result<Option<Decimal>, BoxError> {
    Ok(match v {
        ScalarValue::Decimal128(Some(m), _, s) => {
            Some(Decimal::try_from_i128_with_scale(*m, u32::try_from(*s)?)?)
        }
        ScalarValue::Float32(_) | ScalarValue::Float64(_) => {
            as_f64(v).map(Decimal::try_from).transpose()?
        }
        _ => match (as_i128(v), as_str(v)) {
            (Some(i), _) => Some(Decimal::from_i128_with_scale(i, 0)),
            (None, Some(s)) => Some(Decimal::from_str(s)?),
            _ => None,
        },
    })
}

/// Nanoseconds since the epoch, plus whether the timestamp has a time zone.
fn as_timestamp_nanos(v: &ScalarValue) -> Option<(i64, bool)> {
    let (nanos, tz) = match v {
        ScalarValue::TimestampSecond(Some(x), tz) => (x.checked_mul(1_000_000_000)?, tz),
        ScalarValue::TimestampMillisecond(Some(x), tz) => (x.checked_mul(1_000_000)?, tz),
        ScalarValue::TimestampMicrosecond(Some(x), tz) => (x.checked_mul(1_000)?, tz),
        ScalarValue::TimestampNanosecond(Some(x), tz) => (*x, tz),
        _ => return None,
    };
    Some((nanos, tz.is_some()))
}

fn mismatch(v: &ScalarValue, ty: &Type) -> BoxError {
    format!("cannot bind {} to a Postgres {ty} parameter", v.data_type()).into()
}

impl ToSql for PgParam {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, BoxError> {
        let v = &self.0;
        if v.is_null() {
            return Ok(IsNull::Yes);
        }
        match *ty {
            Type::BOOL => match v {
                ScalarValue::Boolean(Some(b)) => b.to_sql(ty, out),
                _ => Err(mismatch(v, ty)),
            },
            Type::INT2 => i16::try_from(as_i128(v).ok_or_else(|| mismatch(v, ty))?)?.to_sql(ty, out),
            Type::INT4 => i32::try_from(as_i128(v).ok_or_else(|| mismatch(v, ty))?)?.to_sql(ty, out),
            Type::INT8 => i64::try_from(as_i128(v).ok_or_else(|| mismatch(v, ty))?)?.to_sql(ty, out),
            Type::OID => u32::try_from(as_i128(v).ok_or_else(|| mismatch(v, ty))?)?.to_sql(ty, out),
            #[allow(clippy::cast_possible_truncation)]
            Type::FLOAT4 => (as_f64(v).ok_or_else(|| mismatch(v, ty))? as f32).to_sql(ty, out),
            Type::FLOAT8 => as_f64(v).ok_or_else(|| mismatch(v, ty))?.to_sql(ty, out),
            Type::NUMERIC => as_decimal(v)?.ok_or_else(|| mismatch(v, ty))?.to_sql(ty, out),
            Type::BYTEA => as_bytes(v).ok_or_else(|| mismatch(v, ty))?.to_sql(ty, out),
            Type::DATE => match v {
                ScalarValue::Date32(Some(days)) => {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or("invalid epoch")?;
                    epoch
                        .checked_add_signed(chrono::Duration::days(i64::from(*days)))
                        .ok_or("date out of range")?
                        .to_sql(ty, out)
                }
                _ => match as_str(v) {
                    Some(s) => NaiveDate::from_str(s)?.to_sql(ty, out),
                    None => Err(mismatch(v, ty)),
                },
            },
            Type::TIME => {
                let nanos = match v {
                    ScalarValue::Time64Microsecond(Some(x)) => x * 1_000,
                    ScalarValue::Time64Nanosecond(Some(x)) => *x,
                    _ => match as_str(v) {
                        Some(s) => return NaiveTime::from_str(s)?.to_sql(ty, out),
                        None => return Err(mismatch(v, ty)),
                    },
                };
                let secs = u32::try_from(nanos / 1_000_000_000)?;
                let sub = u32::try_from(nanos % 1_000_000_000)?;
                NaiveTime::from_num_seconds_from_midnight_opt(secs, sub)
                    .ok_or("time out of range")?
                    .to_sql(ty, out)
            }
            Type::TIMESTAMP | Type::TIMESTAMPTZ => {
                let Some((nanos, _)) = as_timestamp_nanos(v) else {
                    return match as_str(v) {
                        Some(s) if *ty == Type::TIMESTAMP => {
                            NaiveDateTime::from_str(s)?.to_sql(ty, out)
                        }
                        Some(s) => DateTime::<chrono::FixedOffset>::parse_from_rfc3339(s)?
                            .with_timezone(&Utc)
                            .to_sql(ty, out),
                        None => Err(mismatch(v, ty)),
                    };
                };
                let dt = DateTime::<Utc>::from_timestamp_nanos(nanos);
                if *ty == Type::TIMESTAMP {
                    dt.naive_utc().to_sql(ty, out)
                } else {
                    dt.to_sql(ty, out)
                }
            }
            Type::UUID => match as_str(v) {
                Some(s) => uuid::Uuid::from_str(s)?.to_sql(ty, out),
                None => match as_bytes(v) {
                    Some(b) => uuid::Uuid::from_slice(b)?.to_sql(ty, out),
                    None => Err(mismatch(v, ty)),
                },
            },
            Type::JSON | Type::JSONB => match as_str(v) {
                Some(s) => serde_json::Value::from_str(s)?.to_sql(ty, out),
                None => Err(mismatch(v, ty)),
            },
            // Text-like (text, varchar, bpchar, name, citext, unknown, enums...) take the
            // value's string form.
            _ => match as_str(v) {
                Some(s) => s.to_sql(ty, out),
                None if as_bytes(v).is_some() => Err(mismatch(v, ty)),
                None => v.to_string().as_str().to_sql(ty, out),
            },
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn rejects_named_and_unsupported() {
        let named = QueryParams {
            positional: vec![],
            named: BTreeMap::from([("a".to_string(), ScalarValue::Int32(Some(1)))]),
        };
        assert!(to_pg_params(&named, &QueryOptions::default()).is_err());
        let unsupported = QueryParams::positional([ScalarValue::IntervalYearMonth(Some(1))]);
        assert!(to_pg_params(&unsupported, &QueryOptions::default()).is_err());
        let ok = QueryParams::positional([ScalarValue::Int64(Some(1)), ScalarValue::Null]);
        assert_eq!(to_pg_params(&ok, &QueryOptions::default()).unwrap().len(), 2);
    }

    #[test]
    fn narrows_to_param_type() {
        let mut out = BytesMut::new();
        PgParam(ScalarValue::Int64(Some(7)))
            .to_sql(&Type::INT4, &mut out)
            .unwrap();
        assert_eq!(&out[..], &7i32.to_be_bytes());
        let mut out = BytesMut::new();
        assert!(PgParam(ScalarValue::Int64(Some(i64::MAX)))
            .to_sql(&Type::INT4, &mut out)
            .is_err());
    }
}
