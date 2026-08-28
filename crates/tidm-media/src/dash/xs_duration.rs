use once_cell::sync::Lazy;
use regex::Regex;

static XS_DURATION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)^(-)?P(([0-9]*)Y)?(([0-9]*)M)?(([0-9]*)D)?(T(([0-9]*)H)?(([0-9]*)M)?(([0-9.]*)S)?)?$",
    )
    .unwrap()
});

pub fn parse_xs_duration(value: &str) -> anyhow::Result<i64> {
    if let Some(caps) = XS_DURATION_PATTERN.captures(value) {
        let negated = caps.get(1).is_some();
        let mut duration_seconds = 0.0f64;

        if let Some(years) = caps.get(3).map(|m| m.as_str()).filter(|s| !s.is_empty()) {
            duration_seconds += years.parse::<f64>()? * 31_556_908.0;
        }
        if let Some(months) = caps.get(5).map(|m| m.as_str()).filter(|s| !s.is_empty()) {
            duration_seconds += months.parse::<f64>()? * 2_629_739.0;
        }
        if let Some(days) = caps.get(7).map(|m| m.as_str()).filter(|s| !s.is_empty()) {
            duration_seconds += days.parse::<f64>()? * 86_400.0;
        }
        if let Some(hours) = caps.get(10).map(|m| m.as_str()).filter(|s| !s.is_empty()) {
            duration_seconds += hours.parse::<f64>()? * 3600.0;
        }
        if let Some(minutes) = caps.get(12).map(|m| m.as_str()).filter(|s| !s.is_empty()) {
            duration_seconds += minutes.parse::<f64>()? * 60.0;
        }
        if let Some(seconds) = caps.get(14).map(|m| m.as_str()).filter(|s| !s.is_empty()) {
            duration_seconds += seconds.parse::<f64>()?;
        }

        let duration_millis = (duration_seconds * 1000.0) as i64;
        Ok(if negated { -duration_millis } else { duration_millis })
    } else {
        Ok((value.parse::<f64>()? * 3600.0 * 1000.0) as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hours_minutes_seconds() {
        assert_eq!(parse_xs_duration("PT1H2M3S").unwrap(), (3600 + 120 + 3) * 1000);
    }

    #[test]
    fn parses_fractional_seconds() {
        assert_eq!(parse_xs_duration("PT6.5S").unwrap(), 6500);
    }

    #[test]
    fn parses_zero_duration() {
        assert_eq!(parse_xs_duration("PT0S").unwrap(), 0);
    }

    #[test]
    fn parses_days() {
        assert_eq!(parse_xs_duration("P1D").unwrap(), 86_400_000);
    }
}
