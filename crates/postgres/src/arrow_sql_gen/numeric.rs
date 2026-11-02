//! Exact decoding of the Postgres binary `numeric` wire format.
//!
//! `rust_decimal` only holds 96-bit mantissas, so it can't represent `numeric(60,8)` or an
//! unconstrained `numeric` with many digits. This decodes the wire format into its decimal
//! digits so values can be written to `Decimal128`, `Decimal256` or `Utf8` without loss.

use std::error::Error;

use arrow::datatypes::i256;
use byteorder::{BigEndian, ReadBytesExt};
use tokio_postgres::types::{FromSql, Type};

const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_PINF: u16 = 0xD000;
const NUMERIC_NINF: u16 = 0xF000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PgNumeric {
    Finite {
        negative: bool,
        /// Integer digits without leading zeros (`"0"` when the integer part is zero).
        int_digits: String,
        /// Exactly `dscale` fractional digits.
        frac_digits: String,
    },
    NaN,
    PosInfinity,
    NegInfinity,
}

/// Why a numeric can't be written as a decimal mantissa.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MantissaError {
    NotFinite,
    /// More non-zero fractional digits than the destination scale.
    ExceedsScale,
    Overflow,
}

impl PgNumeric {
    /// The Postgres text rendering (`numeric_out`) of the value.
    pub(crate) fn to_text(&self) -> String {
        match self {
            PgNumeric::NaN => "NaN".to_string(),
            PgNumeric::PosInfinity => "Infinity".to_string(),
            PgNumeric::NegInfinity => "-Infinity".to_string(),
            PgNumeric::Finite {
                negative,
                int_digits,
                frac_digits,
            } => {
                let mut s = String::with_capacity(int_digits.len() + frac_digits.len() + 2);
                let is_zero = int_digits == "0" && frac_digits.bytes().all(|b| b == b'0');
                if *negative && !is_zero {
                    s.push('-');
                }
                s.push_str(int_digits);
                if !frac_digits.is_empty() {
                    s.push('.');
                    s.push_str(frac_digits);
                }
                s
            }
        }
    }

    /// The unscaled integer representing this value at `scale`, without rounding.
    pub(crate) fn mantissa(&self, scale: i8) -> Result<i256, MantissaError> {
        let PgNumeric::Finite {
            negative,
            int_digits,
            frac_digits,
        } = self
        else {
            return Err(MantissaError::NotFinite);
        };

        let mut digits = String::with_capacity(int_digits.len() + frac_digits.len());
        digits.push_str(int_digits);
        digits.push_str(frac_digits);

        let shift = i32::from(scale) - i32::try_from(frac_digits.len()).unwrap_or(i32::MAX);
        if shift >= 0 {
            digits.extend(std::iter::repeat_n('0', shift as usize));
        } else {
            let drop = usize::try_from(-shift).unwrap_or(usize::MAX);
            let keep = digits.len().saturating_sub(drop);
            if digits[keep..].bytes().any(|b| b != b'0') {
                return Err(MantissaError::ExceedsScale);
            }
            digits.truncate(keep);
        }

        let ten = i256::from_i128(10);
        let mut value = i256::ZERO;
        for b in digits.bytes() {
            let d = i256::from_i128(i128::from(b - b'0'));
            value = value
                .checked_mul(ten)
                .and_then(|v| v.checked_add(d))
                .ok_or(MantissaError::Overflow)?;
        }
        Ok(if *negative { value.wrapping_neg() } else { value })
    }

    pub(crate) fn mantissa_i128(&self, scale: i8) -> Result<i128, MantissaError> {
        self.mantissa(scale)?
            .to_i128()
            .ok_or(MantissaError::Overflow)
    }
}

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let mut rdr = raw;
        let ndigits = rdr.read_u16::<BigEndian>()?;
        let weight = rdr.read_i16::<BigEndian>()?;
        let sign = rdr.read_u16::<BigEndian>()?;
        let dscale = rdr.read_u16::<BigEndian>()?;

        let negative = match sign {
            NUMERIC_POS => false,
            NUMERIC_NEG => true,
            NUMERIC_NAN => return Ok(PgNumeric::NaN),
            NUMERIC_PINF => return Ok(PgNumeric::PosInfinity),
            NUMERIC_NINF => return Ok(PgNumeric::NegInfinity),
            other => return Err(format!("invalid numeric sign: {other:#x}").into()),
        };

        let mut groups = Vec::with_capacity(usize::from(ndigits));
        for _ in 0..ndigits {
            groups.push(rdr.read_u16::<BigEndian>()?);
        }
        // Group `i` has the value `groups[i] * 10000^(weight - i)`.
        let group_at = |exp: i32| -> u16 {
            let idx = i32::from(weight) - exp;
            if idx < 0 {
                return 0;
            }
            groups.get(idx as usize).copied().unwrap_or(0)
        };

        let mut int_digits = String::new();
        if weight >= 0 {
            for exp in (0..=i32::from(weight)).rev() {
                int_digits.push_str(&format!("{:04}", group_at(exp)));
            }
            let trimmed = int_digits.trim_start_matches('0');
            int_digits = if trimmed.is_empty() {
                "0".to_string()
            } else {
                trimmed.to_string()
            };
        } else {
            int_digits.push('0');
        }

        let mut frac_digits = String::new();
        let frac_groups = usize::from(dscale).div_ceil(4);
        for k in 1..=frac_groups {
            let exp = -i32::try_from(k).unwrap_or(i32::MAX);
            frac_digits.push_str(&format!("{:04}", group_at(exp)));
        }
        frac_digits.truncate(usize::from(dscale));

        Ok(PgNumeric::Finite {
            negative,
            int_digits,
            frac_digits,
        })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(words: &[u16]) -> Vec<u8> {
        words.iter().flat_map(|w| w.to_be_bytes()).collect()
    }

    fn decode(words: &[u16]) -> PgNumeric {
        PgNumeric::from_sql(&Type::NUMERIC, &raw(words)).expect("decodes")
    }

    #[test]
    fn decodes_text() {
        // 9345129329031293.0932: ndigits=5, weight=3, dscale=4
        assert_eq!(
            decode(&[5, 3, 0, 4, 9345, 1293, 2903, 1293, 932]).to_text(),
            "9345129329031293.0932"
        );
        assert_eq!(
            decode(&[5, 3, NUMERIC_NEG, 4, 9345, 1293, 2903, 1293, 932]).to_text(),
            "-9345129329031293.0932"
        );
        // 0.0045: weight=-1, digits=[45], dscale=4
        assert_eq!(decode(&[1, u16::MAX, 0, 4, 45]).to_text(), "0.0045");
        // 1.50 (dscale=2): digits=[1, 5000]
        assert_eq!(decode(&[2, 0, 0, 2, 1, 5000]).to_text(), "1.50");
        // 20000 (weight=1, digits=[2]) – trailing groups omitted on the wire.
        assert_eq!(decode(&[1, 1, 0, 0, 2]).to_text(), "20000");
        // zero
        assert_eq!(decode(&[0, 0, 0, 0]).to_text(), "0");
        assert_eq!(decode(&[0, 0, NUMERIC_NAN, 0]), PgNumeric::NaN);
        assert_eq!(decode(&[0, 0, NUMERIC_PINF, 0]).to_text(), "Infinity");
        assert_eq!(decode(&[0, 0, NUMERIC_NINF, 0]).to_text(), "-Infinity");
    }

    #[test]
    fn mantissa_widens_and_rejects_rounding() {
        let v = decode(&[2, 0, 0, 3, 17, 6850]); // 17.685
        assert_eq!(v.mantissa_i128(20), Ok(17_685 * 10i128.pow(17)));
        assert_eq!(v.mantissa_i128(3), Ok(17_685));
        assert_eq!(v.mantissa_i128(2), Err(MantissaError::ExceedsScale));

        let v = decode(&[2, 0, NUMERIC_NEG, 2, 1, 5000]); // -1.50
        assert_eq!(v.mantissa_i128(1), Ok(-15));

        let v = decode(&[1, 1, 0, 0, 2]); // 20000
        assert_eq!(v.mantissa_i128(-2), Ok(200));

        assert_eq!(
            PgNumeric::NaN.mantissa_i128(2),
            Err(MantissaError::NotFinite)
        );
    }

    #[test]
    fn mantissa_beyond_i128_fits_i256() {
        // 10^40 (weight=10, digits=[1])
        let v = decode(&[1, 10, 0, 0, 1]);
        assert_eq!(v.to_text(), format!("1{}", "0".repeat(40)));
        assert_eq!(v.mantissa_i128(0), Err(MantissaError::Overflow));
        let expected = i256::from_i128(10i128.pow(20)) * i256::from_i128(10i128.pow(20));
        assert_eq!(v.mantissa(0), Ok(expected));
    }
}
