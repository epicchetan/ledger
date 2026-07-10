pub mod bars;
mod delivery;

use crate::LedgerError;

pub use bars::*;
pub use delivery::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionSpec {
    Bars(BarsParams),
}

impl ProjectionSpec {
    /// Parse a projection spec string, e.g. "bars:1m".
    pub fn parse(spec: &str) -> Result<Self, LedgerError> {
        let Some(interval) = spec.strip_prefix("bars:") else {
            return Err(invalid_spec(spec, "expected `bars:<interval>`"));
        };
        if interval.is_empty() {
            return Err(invalid_spec(spec, "missing interval"));
        }

        let mut digit_end = 0;
        for byte in interval.bytes() {
            if byte.is_ascii_digit() {
                digit_end += 1;
            } else {
                break;
            }
        }
        if digit_end == 0 {
            return Err(invalid_spec(spec, "interval value must be digits"));
        }

        let digits = &interval[..digit_end];
        let unit = &interval[digit_end..];
        if unit.len() != 1 {
            return Err(invalid_spec(spec, "interval unit must be s, m, or h"));
        }

        let value = digits
            .parse::<u64>()
            .map_err(|_| invalid_spec(spec, "interval value is too large"))?;
        if value == 0 {
            return Err(invalid_spec(
                spec,
                "interval value must be greater than zero",
            ));
        }

        let unit_ns = match unit {
            "s" => bars::SECOND_NS,
            "m" => bars::MINUTE_NS,
            "h" => bars::HOUR_NS,
            _ => return Err(invalid_spec(spec, "interval unit must be s, m, or h")),
        };
        let interval_ns = value
            .checked_mul(unit_ns)
            .ok_or_else(|| invalid_spec(spec, "interval is too large"))?;

        Ok(Self::Bars(BarsParams { interval_ns }))
    }

    /// Canonical form, e.g. "bars:1m". Parsing the canonical form yields
    /// an equal spec.
    pub fn canonical(&self) -> String {
        match self {
            Self::Bars(params) => bars::canonical_spec(*params),
        }
    }
}

fn invalid_spec(spec: &str, reason: impl Into<String>) -> LedgerError {
    LedgerError::InvalidProjectionSpec {
        spec: spec.to_string(),
        reason: reason.into(),
    }
}
