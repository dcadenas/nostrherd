//! Progress relay policy (D42): the body cap and the create/edit clock.
//!
//! Pure data and arithmetic. The host owns the row, the relay, and the
//! tick that drives this policy.

/// Seconds an ask must have been open before its progress post exists.
pub const PROGRESS_INITIAL_HOLD_SECS: i64 = 20;

/// Minimum seconds between two sends (create or edit) of one progress post.
pub const PROGRESS_EDIT_INTERVAL_SECS: i64 = 30;

/// Maximum kind-40003 edits per ask; later bodies are dropped.
pub const PROGRESS_EDIT_CAP: u32 = 20;

/// Byte cap of one relayed progress body, trailing marker included.
pub const PROGRESS_BODY_MAX_BYTES: usize = 1024;

/// Marker appended to a truncated body.
pub const PROGRESS_ELLIPSIS: &str = "…";

/// Trim and cap one occupant progress body.
///
/// Returns `None` for a whitespace-only body: D42 relays only non-empty
/// prose. A body over the cap is cut on a char boundary so that body plus
/// [`PROGRESS_ELLIPSIS`] fits in [`PROGRESS_BODY_MAX_BYTES`].
#[must_use]
pub fn cap_progress_body(body: &str) -> Option<String> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.len() <= PROGRESS_BODY_MAX_BYTES {
        return Some(trimmed.to_owned());
    }
    let mut cut = PROGRESS_BODY_MAX_BYTES - PROGRESS_ELLIPSIS.len();
    while !trimmed.is_char_boundary(cut) {
        cut -= 1;
    }
    let mut capped = trimmed[..cut].trim_end().to_owned();
    capped.push_str(PROGRESS_ELLIPSIS);
    Some(capped)
}

/// What the host knows about one ask's progress post when the tick runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressClock {
    /// When the ask opened; the initial hold counts from here.
    pub opened_at: i64,
    /// Last accepted send (create or edit), if any.
    pub last_send_at: Option<i64>,
    /// Whether the progress post exists on the relay.
    pub post_exists: bool,
    /// Whether a newer body is waiting to be sent.
    pub pending_body: bool,
}

/// The one relay action the tick may take for a progress post.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressStep {
    /// Nothing to send yet, or the hold or edit interval has not elapsed.
    Wait,
    /// Create the post: the hold elapsed and a body is pending.
    Create,
    /// Edit the post in place with the newest pending body.
    Edit,
}

/// Decide the next step for one progress post at `now`.
#[must_use]
pub fn next_progress_step(clock: &ProgressClock, now: i64) -> ProgressStep {
    if !clock.pending_body {
        return ProgressStep::Wait;
    }
    if !clock.post_exists {
        return if now.saturating_sub(clock.opened_at) >= PROGRESS_INITIAL_HOLD_SECS {
            ProgressStep::Create
        } else {
            ProgressStep::Wait
        };
    }
    match clock.last_send_at {
        Some(last) if now.saturating_sub(last) < PROGRESS_EDIT_INTERVAL_SECS => ProgressStep::Wait,
        _ => ProgressStep::Edit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_or_whitespace_progress_body_is_none() {
        assert_eq!(cap_progress_body(""), None);
        assert_eq!(cap_progress_body("  \n\t "), None);
        assert_eq!(
            cap_progress_body("  working  \n"),
            Some("working".to_owned())
        );
    }

    #[test]
    fn body_at_the_cap_is_kept_whole() {
        let body = "x".repeat(PROGRESS_BODY_MAX_BYTES);
        assert_eq!(cap_progress_body(&body).as_deref(), Some(body.as_str()));
    }

    #[test]
    fn body_over_the_cap_is_cut_with_a_trailing_ellipsis_within_the_cap() {
        let body = "y".repeat(PROGRESS_BODY_MAX_BYTES + 500);
        let capped = cap_progress_body(&body).expect("capped");
        assert!(capped.ends_with(PROGRESS_ELLIPSIS));
        assert!(capped.len() <= PROGRESS_BODY_MAX_BYTES);
        assert_eq!(capped.len(), PROGRESS_BODY_MAX_BYTES);
    }

    #[test]
    fn body_over_the_cap_is_cut_on_a_char_boundary() {
        // Four-byte scalar values: a naive byte cut would split one.
        let body = "🦀".repeat(PROGRESS_BODY_MAX_BYTES / 2);
        let capped = cap_progress_body(&body).expect("capped");
        assert!(capped.ends_with(PROGRESS_ELLIPSIS));
        assert!(capped.len() <= PROGRESS_BODY_MAX_BYTES);
        let without_marker = capped.strip_suffix(PROGRESS_ELLIPSIS).expect("marker");
        assert!(without_marker.chars().all(|character| character == '🦀'));
    }

    fn clock(post_exists: bool, last_send_at: Option<i64>) -> ProgressClock {
        ProgressClock {
            opened_at: 1_000,
            last_send_at,
            post_exists,
            pending_body: true,
        }
    }

    #[test]
    fn no_pending_body_waits_regardless_of_time() {
        let mut idle = clock(false, None);
        idle.pending_body = false;
        assert_eq!(next_progress_step(&idle, 1_000_000), ProgressStep::Wait);
        let mut posted = clock(true, Some(0));
        posted.pending_body = false;
        assert_eq!(next_progress_step(&posted, 1_000_000), ProgressStep::Wait);
    }

    #[test]
    fn create_waits_for_the_initial_hold_from_open_time() {
        let waiting = clock(false, None);
        assert_eq!(
            next_progress_step(&waiting, 1_000 + PROGRESS_INITIAL_HOLD_SECS - 1),
            ProgressStep::Wait
        );
        assert_eq!(
            next_progress_step(&waiting, 1_000 + PROGRESS_INITIAL_HOLD_SECS),
            ProgressStep::Create
        );
    }

    #[test]
    fn edit_waits_for_the_interval_since_the_last_send() {
        let posted = clock(true, Some(2_000));
        assert_eq!(
            next_progress_step(&posted, 2_000 + PROGRESS_EDIT_INTERVAL_SECS - 1),
            ProgressStep::Wait
        );
        assert_eq!(
            next_progress_step(&posted, 2_000 + PROGRESS_EDIT_INTERVAL_SECS),
            ProgressStep::Edit
        );
    }
}
