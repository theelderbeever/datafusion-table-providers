//! Binding [`ScalarValue`] parameters to CQL statements.

use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::scalar::ScalarValue;
use datafusion_table_providers_common::sql::db_connection_pool::query::{
    reject_settings, ParamError, QueryOptions, QueryParams,
};
use scylla::frame::response::result::{ColumnType, NativeType};
use scylla::observability::history::HistoryListener;
use scylla::value::{
    CqlDate, CqlDecimal, CqlTime, CqlTimestamp, CqlTimeuuid, CqlValue, CqlVarint,
};

/// A driver history listener to attach to a query, passed via
/// [`QueryOptions::extensions`]: `options.with_extension(CassandraHistoryListener(listener))`.
#[derive(Debug, Clone)]
pub struct CassandraHistoryListener(pub Arc<dyn HistoryListener>);

/// Returns the history listener in `options`, if any.
#[must_use]
pub fn history_listener(options: &QueryOptions) -> Option<Arc<dyn HistoryListener>> {
    options
        .extensions
        .get::<CassandraHistoryListener>()
        .map(|l| Arc::clone(&l.0))
}

/// Checks that `params`/`options` can be applied to a CQL query: only positional parameters,
/// no settings.
///
/// # Errors
///
/// Returns an error for named parameters or settings.
pub fn check_params(params: &QueryParams, options: &QueryOptions) -> Result<(), ParamError> {
    if !params.named.is_empty() {
        return Err(ParamError::NamedParamsUnsupported {});
    }
    reject_settings(options)
}

fn unsupported(v: &ScalarValue, target: Option<&ColumnType<'_>>) -> ParamError {
    ParamError::UnsupportedParamType {
        data_type: match target {
            Some(t) => format!("{} for CQL {}", v.data_type(), crate::types::cql_type_name(t)),
            None => v.data_type().to_string(),
        },
    }
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
        _ => return None,
    })
}

fn as_str(v: &ScalarValue) -> Option<&str> {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => {
            Some(s)
        }
        _ => None,
    }
}

fn timestamp_millis(v: &ScalarValue) -> Option<i64> {
    match v {
        ScalarValue::TimestampSecond(Some(x), _) => x.checked_mul(1_000),
        ScalarValue::TimestampMillisecond(Some(x), _) => Some(*x),
        ScalarValue::TimestampMicrosecond(Some(x), _) => Some(x.div_euclid(1_000)),
        ScalarValue::TimestampNanosecond(Some(x), _) => Some(x.div_euclid(1_000_000)),
        _ => None,
    }
}

/// Converts a scalar to a CQL value, coerced to `target` (the type of the bind marker, from
/// the prepared statement) when known. `None` is CQL `NULL`.
///
/// # Errors
///
/// Returns an error if the scalar can't be represented as `target`.
#[allow(clippy::too_many_lines)]
pub fn scalar_to_cql(
    v: &ScalarValue,
    target: Option<&ColumnType<'_>>,
) -> Result<Option<CqlValue>, ParamError> {
    if v.is_null() {
        return Ok(None);
    }
    let err = || unsupported(v, target);
    let int = |f: fn(i128) -> Option<CqlValue>| as_i128(v).and_then(f).ok_or_else(err);

    let Some(ColumnType::Native(native)) = target else {
        // No (or a composite) target: the natural mapping.
        return Ok(Some(match v {
            ScalarValue::Boolean(Some(b)) => CqlValue::Boolean(*b),
            ScalarValue::Int8(Some(x)) => CqlValue::TinyInt(*x),
            ScalarValue::Int16(Some(x)) => CqlValue::SmallInt(*x),
            ScalarValue::Int32(Some(x)) => CqlValue::Int(*x),
            ScalarValue::Int64(Some(x)) => CqlValue::BigInt(*x),
            ScalarValue::UInt8(Some(_))
            | ScalarValue::UInt16(Some(_))
            | ScalarValue::UInt32(Some(_))
            | ScalarValue::UInt64(Some(_)) => int(|i| i64::try_from(i).ok().map(CqlValue::BigInt))?,
            ScalarValue::Float32(Some(x)) => CqlValue::Float(*x),
            ScalarValue::Float64(Some(x)) => CqlValue::Double(*x),
            ScalarValue::Binary(Some(b))
            | ScalarValue::LargeBinary(Some(b))
            | ScalarValue::BinaryView(Some(b))
            | ScalarValue::FixedSizeBinary(_, Some(b)) => CqlValue::Blob(b.clone()),
            ScalarValue::Decimal128(Some(m), _, s) => CqlValue::Decimal(
                CqlDecimal::from_signed_be_bytes_slice_and_exponent(&m.to_be_bytes(), i32::from(*s)),
            ),
            ScalarValue::Date32(Some(d)) => CqlValue::Date(CqlDate(
                (1u32 << 31).wrapping_add_signed(*d),
            )),
            ScalarValue::Time64Nanosecond(Some(x)) => CqlValue::Time(CqlTime(*x)),
            ScalarValue::Time64Microsecond(Some(x)) => {
                CqlValue::Time(CqlTime(x.checked_mul(1_000).ok_or_else(err)?))
            }
            _ => match (timestamp_millis(v), as_str(v)) {
                (Some(ms), _) => CqlValue::Timestamp(CqlTimestamp(ms)),
                (None, Some(s)) => CqlValue::Text(s.to_string()),
                _ => return Err(err()),
            },
        }));
    };

    Ok(Some(match native {
        NativeType::TinyInt => int(|i| i8::try_from(i).ok().map(CqlValue::TinyInt))?,
        NativeType::SmallInt => int(|i| i16::try_from(i).ok().map(CqlValue::SmallInt))?,
        NativeType::Int => int(|i| i32::try_from(i).ok().map(CqlValue::Int))?,
        NativeType::BigInt | NativeType::Counter => {
            int(|i| i64::try_from(i).ok().map(CqlValue::BigInt))?
        }
        NativeType::Varint => int(|i| {
            Some(CqlValue::Varint(CqlVarint::from_signed_bytes_be(
                i.to_be_bytes().to_vec(),
            )))
        })?,
        NativeType::Float => match v {
            ScalarValue::Float32(Some(x)) => CqlValue::Float(*x),
            #[allow(clippy::cast_possible_truncation)]
            ScalarValue::Float64(Some(x)) => CqlValue::Float(*x as f32),
            #[allow(clippy::cast_precision_loss)]
            _ => CqlValue::Float(as_i128(v).ok_or_else(err)? as f32),
        },
        NativeType::Double => match v {
            ScalarValue::Float32(Some(x)) => CqlValue::Double(f64::from(*x)),
            ScalarValue::Float64(Some(x)) => CqlValue::Double(*x),
            #[allow(clippy::cast_precision_loss)]
            _ => CqlValue::Double(as_i128(v).ok_or_else(err)? as f64),
        },
        NativeType::Decimal => match v {
            ScalarValue::Decimal128(Some(m), _, s) => CqlValue::Decimal(
                CqlDecimal::from_signed_be_bytes_slice_and_exponent(&m.to_be_bytes(), i32::from(*s)),
            ),
            _ => match (as_i128(v), as_str(v)) {
                (Some(i), _) => CqlValue::Decimal(
                    CqlDecimal::from_signed_be_bytes_slice_and_exponent(&i.to_be_bytes(), 0),
                ),
                (None, Some(s)) => {
                    let d = bigdecimal::BigDecimal::from_str(s).map_err(|_| err())?;
                    let (int, scale) = d.as_bigint_and_exponent();
                    CqlValue::Decimal(CqlDecimal::from_signed_be_bytes_slice_and_exponent(
                        &int.to_signed_bytes_be(),
                        i32::try_from(scale).map_err(|_| err())?,
                    ))
                }
                _ => return Err(err()),
            },
        },
        NativeType::Boolean => match v {
            ScalarValue::Boolean(Some(b)) => CqlValue::Boolean(*b),
            _ => return Err(err()),
        },
        NativeType::Text | NativeType::Ascii => {
            let s = as_str(v).map_or_else(|| v.to_string(), str::to_string);
            if *native == NativeType::Ascii {
                CqlValue::Ascii(s)
            } else {
                CqlValue::Text(s)
            }
        }
        NativeType::Blob => match scalar_to_cql(v, None)? {
            Some(CqlValue::Blob(b)) => CqlValue::Blob(b),
            _ => return Err(err()),
        },
        NativeType::Uuid | NativeType::Timeuuid => {
            let uuid = match v {
                ScalarValue::FixedSizeBinary(16, Some(b)) | ScalarValue::Binary(Some(b)) => {
                    uuid::Uuid::from_slice(b).map_err(|_| err())?
                }
                _ => uuid::Uuid::from_str(as_str(v).ok_or_else(err)?).map_err(|_| err())?,
            };
            if *native == NativeType::Uuid {
                CqlValue::Uuid(uuid)
            } else {
                CqlValue::Timeuuid(CqlTimeuuid::from(uuid))
            }
        }
        NativeType::Inet => {
            CqlValue::Inet(IpAddr::from_str(as_str(v).ok_or_else(err)?).map_err(|_| err())?)
        }
        NativeType::Timestamp => CqlValue::Timestamp(CqlTimestamp(
            timestamp_millis(v)
                .or_else(|| match v {
                    ScalarValue::Int64(Some(x)) => Some(*x),
                    _ => None,
                })
                .ok_or_else(err)?,
        )),
        NativeType::Date | NativeType::Time => match scalar_to_cql(v, None)? {
            Some(value @ (CqlValue::Date(_) | CqlValue::Time(_))) => value,
            _ => return Err(err()),
        },
        _ => scalar_to_cql(v, None)?.ok_or_else(err)?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coerces_to_target() {
        let int = ColumnType::Native(NativeType::Int);
        assert_eq!(
            scalar_to_cql(&ScalarValue::Int64(Some(5)), Some(&int)).unwrap(),
            Some(CqlValue::Int(5))
        );
        assert!(scalar_to_cql(&ScalarValue::Int64(Some(i64::MAX)), Some(&int)).is_err());
        let uuid = ColumnType::Native(NativeType::Uuid);
        assert!(matches!(
            scalar_to_cql(
                &ScalarValue::Utf8(Some("67e55044-10b1-426f-9247-bb680e5fe0c8".into())),
                Some(&uuid)
            )
            .unwrap(),
            Some(CqlValue::Uuid(_))
        ));
        assert_eq!(scalar_to_cql(&ScalarValue::Int32(None), Some(&int)).unwrap(), None);
    }

    #[test]
    fn rejects_named() {
        let p = QueryParams::named([("a", ScalarValue::Int32(Some(1)))]);
        assert!(check_params(&p, &QueryOptions::default()).is_err());
    }
}
