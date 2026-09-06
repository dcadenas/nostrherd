//! Host restraint on unprompted posts (D44, D47).
//!
//! Pure data and arithmetic. The host owns the publish path, the ledger
//! of accepted posts, and the clock this policy is evaluated against.

use std::fmt;

/// Default per-bot, per-channel ceiling on host-initiated posts per
/// rolling 24 hours.
pub const DEFAULT_POST_CEILING_24H: u32 = 24;

/// Length of the ceiling's rolling window in seconds.
pub const POST_CEILING_WINDOW_SECS: i64 = 24 * 60 * 60;

/// Daily quiet-hours window on the host's local clock, as minutes of
/// day. `23:00-07:00` crosses midnight and covers both ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuietHours {
    start_minute: u16,
    end_minute: u16,
}

impl QuietHours {
    /// Parse `HH:MM-HH:MM` on a 24-hour clock.
    ///
    /// `start == end` parses to a window that covers no minute; the
    /// natural reading of the half-open interval, kept rather than
    /// rejected so a degenerate entry is inert, not fatal.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        let (start, end) = raw.split_once('-')?;
        Some(Self {
            start_minute: parse_minute_of_day(start)?,
            end_minute: parse_minute_of_day(end)?,
        })
    }

    #[must_use]
    pub fn start_minute(&self) -> u16 {
        self.start_minute
    }

    #[must_use]
    pub fn end_minute(&self) -> u16 {
        self.end_minute
    }

    /// Whether a local minute of day falls inside the half-open window
    /// `[start, end)`. A window crossing midnight covers both ends.
    #[must_use]
    pub fn covers(&self, minute_of_day: u16) -> bool {
        if self.start_minute <= self.end_minute {
            self.start_minute <= minute_of_day && minute_of_day < self.end_minute
        } else {
            minute_of_day >= self.start_minute || minute_of_day < self.end_minute
        }
    }
}

impl fmt::Display for QuietHours {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_minute(f, self.start_minute)?;
        f.write_str("-")?;
        write_minute(f, self.end_minute)
    }
}

fn parse_minute_of_day(raw: &str) -> Option<u16> {
    let (hours, minutes) = raw.split_once(':')?;
    if hours.len() != 2 || minutes.len() != 2 {
        return None;
    }
    let hours: u16 = hours.parse().ok()?;
    let minutes: u16 = minutes.parse().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some(hours * 60 + minutes)
}

fn write_minute(f: &mut fmt::Formatter<'_>, minute: u16) -> fmt::Result {
    write!(f, "{:02}:{:02}", minute / 60, minute % 60)
}

/// Restraint applied to one bot's host-initiated posts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostRestraint {
    /// Maximum accepted host-initiated posts per channel in the rolling
    /// 24-hour window. Must be at least 1.
    ceiling_24h: u32,
    /// Optional quiet-hours window on the host's local clock.
    quiet_hours: Option<QuietHours>,
}

impl Default for HostRestraint {
    fn default() -> Self {
        Self {
            ceiling_24h: DEFAULT_POST_CEILING_24H,
            quiet_hours: None,
        }
    }
}

impl HostRestraint {
    /// Build a restraint, rejecting a ceiling below 1.
    #[must_use]
    pub fn new(ceiling_24h: u32, quiet_hours: Option<QuietHours>) -> Option<Self> {
        (ceiling_24h >= 1).then_some(Self {
            ceiling_24h,
            quiet_hours,
        })
    }

    #[must_use]
    pub fn ceiling_24h(&self) -> u32 {
        self.ceiling_24h
    }

    #[must_use]
    pub fn quiet_hours(&self) -> Option<&QuietHours> {
        self.quiet_hours.as_ref()
    }
}

/// Why one host-initiated post may not publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestraintVerdict {
    /// The post may publish.
    Allow,
    /// The host's local time is inside the quiet-hours window.
    QuietHours(QuietHours),
    /// The per-channel rolling-window count already reached the ceiling.
    Ceiling {
        /// Accepted host-initiated posts in the window.
        published: u32,
        /// The configured ceiling.
        ceiling: u32,
    },
}

/// Decide one host-initiated post. Quiet hours is evaluated first: it is
/// a time gate and independent of volume, and a suppressed post must not
/// depend on how much was published before the window closed.
#[must_use]
pub fn evaluate_restraint(
    restraint: &HostRestraint,
    local_minute_of_day: u16,
    published_last_24h: u32,
) -> RestraintVerdict {
    if let Some(quiet) = restraint.quiet_hours() {
        if quiet.covers(local_minute_of_day) {
            return RestraintVerdict::QuietHours(*quiet);
        }
    }
    if published_last_24h >= restraint.ceiling_24h() {
        return RestraintVerdict::Ceiling {
            published: published_last_24h,
            ceiling: restraint.ceiling_24h(),
        };
    }
    RestraintVerdict::Allow
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quiet_hours_parse_and_display_round_trip() {
        let window = QuietHours::parse("23:00-07:00").expect("window");
        assert_eq!(window.start_minute(), 23 * 60);
        assert_eq!(window.end_minute(), 7 * 60);
        assert_eq!(window.to_string(), "23:00-07:00");
    }

    #[test]
    fn quiet_hours_rejects_out_of_clock_values() {
        assert!(QuietHours::parse("24:00-07:00").is_none());
        assert!(QuietHours::parse("23:00-7:5").is_none());
        assert!(QuietHours::parse("2300-0700").is_none());
        assert!(QuietHours::parse("").is_none());
    }

    #[test]
    fn window_covering_a_day_boundary_covers_both_ends() {
        let window = QuietHours::parse("23:00-07:00").expect("window");
        assert!(window.covers(23 * 60));
        assert!(window.covers(23 * 60 + 59));
        assert!(window.covers(0));
        assert!(window.covers(6 * 60 + 59));
        assert!(!window.covers(7 * 60));
        assert!(!window.covers(22 * 60 + 59));
    }

    #[test]
    fn window_within_one_day_is_half_open() {
        let window = QuietHours::parse("09:00-17:00").expect("window");
        assert!(window.covers(9 * 60));
        assert!(window.covers(16 * 60 + 59));
        assert!(!window.covers(8 * 60 + 59));
        assert!(!window.covers(17 * 60));
    }

    #[test]
    fn equal_start_and_end_cover_nothing() {
        let window = QuietHours::parse("00:00-00:00").expect("window");
        for minute in [0, 1, 12 * 60, 23 * 60 + 59] {
            assert!(!window.covers(minute));
        }
    }

    #[test]
    fn evaluation_allows_under_the_ceiling_and_outside_the_window() {
        let restraint = HostRestraint::new(2, QuietHours::parse("23:00-07:00")).expect("restraint");
        assert_eq!(
            evaluate_restraint(&restraint, 12 * 60, 1),
            RestraintVerdict::Allow
        );
    }

    #[test]
    fn evaluation_suppresses_at_the_ceiling() {
        let restraint = HostRestraint::new(2, None).expect("restraint");
        assert_eq!(
            evaluate_restraint(&restraint, 12 * 60, 2),
            RestraintVerdict::Ceiling {
                published: 2,
                ceiling: 2
            }
        );
    }

    #[test]
    fn quiet_hours_is_evaluated_before_the_ceiling() {
        let restraint = HostRestraint::new(1, QuietHours::parse("23:00-07:00")).expect("restraint");
        let window = QuietHours::parse("23:00-07:00").expect("window");
        assert_eq!(
            evaluate_restraint(&restraint, 23 * 60, u32::MAX),
            RestraintVerdict::QuietHours(window)
        );
    }

    #[test]
    fn restraint_rejects_a_ceiling_below_one() {
        assert!(HostRestraint::new(0, None).is_none());
        assert_eq!(
            HostRestraint::default().ceiling_24h(),
            DEFAULT_POST_CEILING_24H
        );
        assert!(HostRestraint::default().quiet_hours().is_none());
    }
}
