//! Host side of D42: relay occupant progress as one edited stamped post.
//!
//! The delivery handler only records the row (before the ACK). The relay
//! work runs on the host refresh tick: one kind-9 create after the
//! initial hold, then coalesced kind-40003 edits under the interval and
//! cap. A cancel deletes the post (kind 9005). Every relay step is
//! best-effort and never fails the turn.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use botserver_domain::progress::{
    cap_progress_body, next_progress_step, ProgressClock, ProgressStep, PROGRESS_EDIT_CAP,
    PROGRESS_EDIT_INTERVAL_SECS,
};
use botserver_domain::{outbound_prefix_for, stamp_outbound, BotId, EventId, TurnState};

use crate::outbox::{
    record_and_send, OutboundAttempt, OutboundPublisher, PublishError, SendOutcome,
};
use crate::{HostRepository, TurnRecord};

/// Durable progress state for one ask (D42 Durability).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressPost {
    pub ask_id: String,
    pub channel_id: String,
    /// The triggering `EventId`: the post replies to it like the final.
    pub reply_to_event_id: EventId,
    pub thread_root_event_id: Option<EventId>,
    /// Hold start: the turn's open time, or the first progress delivery
    /// for an open turn recorded before `turns.opened_at` existed.
    pub opened_at: i64,
    /// Newest unsent body, trimmed and capped, unstamped.
    pub pending_body: Option<String>,
    /// Stamped body the create was signed with, so a redelivery
    /// re-prepares the identical event (D43).
    pub post_body: Option<String>,
    /// Create event id recorded before send (D28, amended by D43).
    pub prepared_event_id: Option<String>,
    pub prepared_created_at: Option<i64>,
    /// Create accepted by the relay; edits and the delete target this id.
    pub post_event_id: Option<String>,
    pub edit_count: u32,
    /// Last accepted send (create or edit).
    pub last_send_at: Option<i64>,
    /// No further relay for this ask: cancelled, lost create, or a
    /// non-retryable create failure.
    pub ended: bool,
    /// The one operator notice for the edit cap was already emitted.
    pub cap_noticed: bool,
    /// Last operator notice for a retryable relay failure.
    pub retry_noticed_at: Option<i64>,
    /// A cancelled post still needs a kind-9005 accepted by the relay.
    pub delete_pending: bool,
}

impl ProgressPost {
    fn clock(&self) -> ProgressClock {
        ProgressClock {
            opened_at: self.opened_at,
            last_send_at: self.last_send_at,
            post_exists: self.post_event_id.is_some(),
            pending_body: self.pending_body.is_some(),
        }
    }

    /// The relay event the delete must target, if a create may have landed.
    fn delete_target(&self) -> Option<EventId> {
        let id = self
            .post_event_id
            .as_deref()
            .or(self.prepared_event_id.as_deref())?;
        EventId::parse_hex(id)
    }

    fn create_attempt(&self, stamped_body: &str) -> OutboundAttempt {
        OutboundAttempt {
            ask_id: self.ask_id.clone(),
            body: stamped_body.to_owned(),
            channel_id: self.channel_id.clone(),
            reply_to_event_id: Some(self.reply_to_event_id.clone()),
            thread_root_event_id: self.thread_root_event_id.clone(),
            // D42: the progress post carries no mention.
            mention: String::new(),
            outbound_event_id: self.post_event_id.clone(),
            prepared_event_id: self.prepared_event_id.clone(),
            prepared_created_at: self.prepared_created_at,
            dispatched: self.prepared_event_id.is_some(),
        }
    }
}

/// Whether a progress relay operation completed or continues in the background.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressRelayDispatch {
    Accepted,
    Pending,
}

/// Result of one background progress relay operation.
#[derive(Debug)]
pub enum ProgressRelayCompletion {
    Edit {
        ask_id: String,
        body: String,
        completed_at: i64,
        result: Result<(), PublishError>,
    },
    Delete {
        ask_id: String,
        completed_at: i64,
        result: Result<(), PublishError>,
    },
}

/// Best-effort kind-40003 edit and kind-9005 delete of a progress post.
pub trait ProgressRelay: Send + Sync {
    /// Replace the post content in place.
    ///
    /// # Errors
    ///
    /// Returns an error when the relay did not accept the edit.
    fn edit(
        &self,
        ask_id: &str,
        channel_id: &str,
        post_event_id: &EventId,
        content: &str,
    ) -> Result<ProgressRelayDispatch, PublishError>;

    /// Buzz-delete the post.
    ///
    /// # Errors
    ///
    /// Returns an error when the relay did not accept the delete.
    fn delete(
        &self,
        ask_id: &str,
        channel_id: &str,
        post_event_id: &EventId,
    ) -> Result<ProgressRelayDispatch, PublishError>;

    /// Drain completed background operations.
    fn drain_completions(&self) -> Vec<ProgressRelayCompletion> {
        Vec::new()
    }
}

/// Ignore progress edits and deletes.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopProgressRelay;

impl ProgressRelay for NoopProgressRelay {
    fn edit(
        &self,
        _ask_id: &str,
        _channel_id: &str,
        _post_event_id: &EventId,
        _content: &str,
    ) -> Result<ProgressRelayDispatch, PublishError> {
        Ok(ProgressRelayDispatch::Accepted)
    }

    fn delete(
        &self,
        _ask_id: &str,
        _channel_id: &str,
        _post_event_id: &EventId,
    ) -> Result<ProgressRelayDispatch, PublishError> {
        Ok(ProgressRelayDispatch::Accepted)
    }
}

/// Schedule progress edits and deletes without waiting for relay acceptance.
#[derive(Clone)]
pub struct BackgroundProgressRelay {
    sender: tokio::sync::mpsc::UnboundedSender<ProgressCommand>,
    completions: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<ProgressRelayCompletion>>>,
    active: Arc<std::sync::Mutex<HashSet<ProgressOperation>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ProgressOperation {
    Edit(String),
    Delete(String),
}

#[derive(Debug)]
enum ProgressCommand {
    Edit {
        ask_id: String,
        channel_id: String,
        post_event_id: EventId,
        content: String,
        body: String,
    },
    Delete {
        ask_id: String,
        channel_id: String,
        post_event_id: EventId,
    },
}

impl fmt::Debug for BackgroundProgressRelay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BackgroundProgressRelay")
    }
}

impl BackgroundProgressRelay {
    /// Wrap a blocking relay adapter on the current multi-threaded runtime.
    #[must_use]
    pub fn new(inner: Arc<dyn ProgressRelay>) -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let (completion_sender, completion_receiver) = std::sync::mpsc::channel();
        let active = Arc::new(std::sync::Mutex::new(HashSet::new()));
        tokio::runtime::Handle::current().spawn(async move {
            while let Some(command) = receiver.recv().await {
                let completion = match command {
                    ProgressCommand::Edit {
                        ask_id,
                        channel_id,
                        post_event_id,
                        content,
                        body,
                    } => {
                        let result = inner
                            .edit(&ask_id, &channel_id, &post_event_id, &content)
                            .and_then(|dispatch| match dispatch {
                                ProgressRelayDispatch::Accepted => Ok(()),
                                ProgressRelayDispatch::Pending => Err(PublishError::NotAccepted {
                                    detail: "nested background progress relay".to_owned(),
                                }),
                            });
                        ProgressRelayCompletion::Edit {
                            ask_id,
                            body,
                            completed_at: crate::unix_now().unwrap_or_default(),
                            result,
                        }
                    }
                    ProgressCommand::Delete {
                        ask_id,
                        channel_id,
                        post_event_id,
                    } => {
                        let result = inner.delete(&ask_id, &channel_id, &post_event_id).and_then(
                            |dispatch| match dispatch {
                                ProgressRelayDispatch::Accepted => Ok(()),
                                ProgressRelayDispatch::Pending => Err(PublishError::NotAccepted {
                                    detail: "nested background progress relay".to_owned(),
                                }),
                            },
                        );
                        ProgressRelayCompletion::Delete {
                            ask_id,
                            completed_at: crate::unix_now().unwrap_or_default(),
                            result,
                        }
                    }
                };
                let _ = completion_sender.send(completion);
            }
        });
        Self {
            sender,
            completions: Arc::new(std::sync::Mutex::new(completion_receiver)),
            active,
        }
    }
}

impl ProgressRelay for BackgroundProgressRelay {
    fn edit(
        &self,
        ask_id: &str,
        channel_id: &str,
        post_event_id: &EventId,
        content: &str,
    ) -> Result<ProgressRelayDispatch, PublishError> {
        let operation = ProgressOperation::Edit(ask_id.to_owned());
        let mut active = self.active.lock().expect("active progress operations");
        if !active.insert(operation.clone()) {
            return Ok(ProgressRelayDispatch::Pending);
        }
        self.sender
            .send(ProgressCommand::Edit {
                ask_id: ask_id.to_owned(),
                channel_id: channel_id.to_owned(),
                post_event_id: post_event_id.clone(),
                content: content.to_owned(),
                body: content.to_owned(),
            })
            .map_err(|_| {
                active.remove(&operation);
                PublishError::NotAccepted {
                    detail: "background progress relay stopped".to_owned(),
                }
            })?;
        Ok(ProgressRelayDispatch::Pending)
    }

    fn delete(
        &self,
        ask_id: &str,
        channel_id: &str,
        post_event_id: &EventId,
    ) -> Result<ProgressRelayDispatch, PublishError> {
        let operation = ProgressOperation::Delete(ask_id.to_owned());
        let mut active = self.active.lock().expect("active progress operations");
        if !active.insert(operation.clone()) {
            return Ok(ProgressRelayDispatch::Pending);
        }
        self.sender
            .send(ProgressCommand::Delete {
                ask_id: ask_id.to_owned(),
                channel_id: channel_id.to_owned(),
                post_event_id: post_event_id.clone(),
            })
            .map_err(|_| {
                active.remove(&operation);
                PublishError::NotAccepted {
                    detail: "background progress relay stopped".to_owned(),
                }
            })?;
        Ok(ProgressRelayDispatch::Pending)
    }

    fn drain_completions(&self) -> Vec<ProgressRelayCompletion> {
        let completions = self
            .completions
            .lock()
            .expect("progress completions")
            .try_iter()
            .collect::<Vec<_>>();
        let mut active = self.active.lock().expect("active progress operations");
        for completion in &completions {
            let operation = match completion {
                ProgressRelayCompletion::Edit { ask_id, .. } => {
                    ProgressOperation::Edit(ask_id.clone())
                }
                ProgressRelayCompletion::Delete { ask_id, .. } => {
                    ProgressOperation::Delete(ask_id.clone())
                }
            };
            active.remove(&operation);
        }
        completions
    }
}

impl ProgressRelay for Arc<dyn ProgressRelay> {
    fn edit(
        &self,
        ask_id: &str,
        channel_id: &str,
        post_event_id: &EventId,
        content: &str,
    ) -> Result<ProgressRelayDispatch, PublishError> {
        (**self).edit(ask_id, channel_id, post_event_id, content)
    }

    fn delete(
        &self,
        ask_id: &str,
        channel_id: &str,
        post_event_id: &EventId,
    ) -> Result<ProgressRelayDispatch, PublishError> {
        (**self).delete(ask_id, channel_id, post_event_id)
    }

    fn drain_completions(&self) -> Vec<ProgressRelayCompletion> {
        (**self).drain_completions()
    }
}

/// Record one occupant `--progress` body for an ask (D33, D42).
///
/// Never relays: the row is persisted so the ACK can follow, and the
/// refresh tick does the send. Progress on a turn that is not open, on
/// a dispatched final attempt, with an empty body, or past the edit cap
/// records nothing and still ACKs.
///
/// # Errors
///
/// Returns an adapter error when the row cannot be read or persisted.
pub fn record_progress<R: HostRepository>(
    repository: &mut R,
    notice: &mut impl FnMut(&str),
    turn: &TurnRecord,
    body: &str,
    now: i64,
) -> Result<(), R::Error> {
    let Some(ask_id) = turn.ask_id.as_deref() else {
        return Ok(());
    };
    if turn.state != TurnState::Open {
        return Ok(());
    }
    if final_in_flight(repository, ask_id)? {
        return Ok(());
    }
    let Some(capped) = cap_progress_body(body) else {
        return Ok(());
    };
    let mut post = match repository.progress_post(ask_id)? {
        Some(post) => post,
        None => new_progress_post(repository, turn, ask_id, now)?,
    };
    if post.ended {
        return Ok(());
    }
    if drop_pending_at_cap(&mut post, notice) {
        repository.save_progress_post(&post)?;
        return Ok(());
    }
    post.pending_body = Some(capped);
    repository.save_progress_post(&post)
}

fn drop_pending_at_cap(post: &mut ProgressPost, notice: &mut impl FnMut(&str)) -> bool {
    if post.post_event_id.is_none() || post.edit_count < PROGRESS_EDIT_CAP {
        return false;
    }
    post.pending_body = None;
    if !post.cap_noticed {
        notice(&format!(
            "progress for ask {} reached the {PROGRESS_EDIT_CAP}-edit cap; later bodies are dropped",
            post.ask_id
        ));
        post.cap_noticed = true;
    }
    true
}

/// Whether a final for this ask has reached the host (D42: "whose outbound
/// attempt is already dispatched").
///
/// The attempt row is saved first thing on a final and never deleted, so
/// its presence is the durable signal. The row's `dispatched` flag is not:
/// a retryable publish failure clears it while the turn stays open and the
/// final is redelivered on the tick.
fn final_in_flight<R: HostRepository>(repository: &R, ask_id: &str) -> Result<bool, R::Error> {
    Ok(repository.outbound_attempt(ask_id)?.is_some())
}

fn new_progress_post<R: HostRepository>(
    repository: &R,
    turn: &TurnRecord,
    ask_id: &str,
    now: i64,
) -> Result<ProgressPost, R::Error> {
    let thread_root_event_id = crate::thread_root_for(repository, &turn.event_id)?;
    Ok(ProgressPost {
        ask_id: ask_id.to_owned(),
        channel_id: turn.channel_id.clone(),
        reply_to_event_id: turn.event_id.clone(),
        thread_root_event_id,
        opened_at: turn.opened_at.unwrap_or(now),
        pending_body: None,
        post_body: None,
        prepared_event_id: None,
        prepared_created_at: None,
        post_event_id: None,
        edit_count: 0,
        last_send_at: None,
        ended: false,
        cap_noticed: false,
        retry_noticed_at: None,
        delete_pending: false,
    })
}

/// End progress once a final has landed (D42).
///
/// A pending body is discarded and the progress post itself stays up.
///
/// # Errors
///
/// Returns an adapter error when the row cannot be read or persisted.
pub fn discard_pending<R: HostRepository>(
    repository: &mut R,
    ask_id: &str,
) -> Result<(), R::Error> {
    let Some(mut post) = repository.progress_post(ask_id)? else {
        return Ok(());
    };
    if post.pending_body.is_none() && post.ended {
        return Ok(());
    }
    post.pending_body = None;
    post.ended = true;
    repository.save_progress_post(&post)
}

/// End progress for a cancelled ask and Buzz-delete its post (D14, D15).
///
/// The row is marked ended before the delete so the tick never sends
/// again; the delete itself is best-effort and only reported.
///
/// # Errors
///
/// Returns an adapter error when the row cannot be read or persisted.
pub fn delete_progress_post<R: HostRepository>(
    repository: &mut R,
    relay: &impl ProgressRelay,
    notice: &mut impl FnMut(&str),
    ask_id: &str,
) -> Result<(), R::Error> {
    let Some(mut post) = repository.progress_post(ask_id)? else {
        return Ok(());
    };
    if post.ended && !post.delete_pending {
        return Ok(());
    }
    post.ended = true;
    post.pending_body = None;
    post.delete_pending = post.delete_target().is_some();
    post.retry_noticed_at = None;
    repository.save_progress_post(&post)?;
    if post.delete_pending {
        send_delete(
            repository,
            relay,
            notice,
            post,
            crate::unix_now().unwrap_or_default(),
        )?;
    }
    Ok(())
}

fn send_delete<R: HostRepository>(
    repository: &mut R,
    relay: &impl ProgressRelay,
    notice: &mut impl FnMut(&str),
    mut post: ProgressPost,
    now: i64,
) -> Result<(), R::Error> {
    let Some(target) = post.delete_target() else {
        post.delete_pending = false;
        return repository.save_progress_post(&post);
    };
    match relay.delete(&post.ask_id, &post.channel_id, &target) {
        Ok(ProgressRelayDispatch::Accepted) => {
            post.delete_pending = false;
            post.retry_noticed_at = None;
            repository.save_progress_post(&post)
        }
        Ok(ProgressRelayDispatch::Pending) => Ok(()),
        Err(error) => {
            if error.is_retryable() {
                let message = format!(
                    "progress post delete for ask {} failed; retrying: {error}",
                    post.ask_id
                );
                if notice_retry(&mut post, now, notice, &message) {
                    repository.save_progress_post(&post)?;
                }
            } else {
                notice(&format!(
                    "progress post delete for ask {} was rejected; not retrying: {error}",
                    post.ask_id
                ));
                post.delete_pending = false;
                repository.save_progress_post(&post)?;
            }
            Ok(())
        }
    }
}

fn notice_retry(
    post: &mut ProgressPost,
    now: i64,
    notice: &mut impl FnMut(&str),
    message: &str,
) -> bool {
    let should_notice = post
        .retry_noticed_at
        .is_none_or(|last| now.saturating_sub(last) >= PROGRESS_EDIT_INTERVAL_SECS);
    if should_notice {
        notice(message);
        post.retry_noticed_at = Some(now);
    }
    should_notice
}

/// Apply accepted or failed background relay results to durable progress state.
///
/// # Errors
///
/// Returns an adapter error when completed state cannot be read or persisted.
pub fn apply_relay_completions<R: HostRepository>(
    repository: &mut R,
    relay: &impl ProgressRelay,
    notice: &mut impl FnMut(&str),
) -> Result<(), R::Error> {
    for completion in relay.drain_completions() {
        match completion {
            ProgressRelayCompletion::Edit {
                ask_id,
                body,
                completed_at,
                result,
            } => {
                let Some(mut post) = repository.progress_post(&ask_id)? else {
                    continue;
                };
                match result {
                    Ok(()) => {
                        post.edit_count += 1;
                        post.last_send_at = Some(completed_at);
                        post.retry_noticed_at = None;
                        let matches_pending = repository
                            .turn_by_ask_id(&ask_id)?
                            .and_then(|turn| {
                                post.pending_body.as_ref().map(|pending| {
                                    stamp_outbound(pending, &outbound_prefix_for(&turn.bot_id))
                                        == body
                                })
                            })
                            .unwrap_or(false);
                        if matches_pending {
                            post.pending_body = None;
                        }
                        drop_pending_at_cap(&mut post, notice);
                        repository.save_progress_post(&post)?;
                    }
                    Err(error) => {
                        let message = format!(
                            "progress edit for ask {ask_id} failed; retrying the newest body: {error}"
                        );
                        if notice_retry(&mut post, completed_at, notice, &message) {
                            repository.save_progress_post(&post)?;
                        }
                    }
                }
            }
            ProgressRelayCompletion::Delete {
                ask_id,
                completed_at,
                result,
            } => {
                let Some(mut post) = repository.progress_post(&ask_id)? else {
                    continue;
                };
                match result {
                    Ok(()) => {
                        post.delete_pending = false;
                        post.retry_noticed_at = None;
                        repository.save_progress_post(&post)?;
                    }
                    Err(error) if error.is_retryable() => {
                        let message = format!(
                            "progress post delete for ask {ask_id} failed; retrying: {error}"
                        );
                        if notice_retry(&mut post, completed_at, notice, &message) {
                            repository.save_progress_post(&post)?;
                        }
                    }
                    Err(error) => {
                        notice(&format!(
                            "progress post delete for ask {ask_id} was rejected; not retrying: {error}"
                        ));
                        post.delete_pending = false;
                        repository.save_progress_post(&post)?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Run the D42 clock once for one ask on the refresh tick.
///
/// At most one relay action per call: finish a dispatched create, create
/// after the hold, or edit after the interval. Failures are notices;
/// `retryable` is not consulted for edits.
///
/// # Errors
///
/// Returns an adapter error when the row cannot be read or persisted.
pub fn flush_progress<R, P, L>(
    repository: &mut R,
    publisher: &P,
    relay: &L,
    notice: &mut impl FnMut(&str),
    turn: &TurnRecord,
    mut post: ProgressPost,
    now: i64,
) -> Result<(), R::Error>
where
    R: HostRepository,
    P: OutboundPublisher,
    P::Error: fmt::Display,
    L: ProgressRelay,
{
    if post.delete_pending {
        return send_delete(repository, relay, notice, post, now);
    }
    if post.ended {
        return Ok(());
    }
    let closed = turn.state != TurnState::Open;
    if closed || final_in_flight(repository, &post.ask_id)? {
        // The final landed or is landing (D42: no relay once the final's
        // attempt exists): keep the post, drop what was pending. Once the
        // turn has left Open, end the row so it leaves the working set; a
        // prepared id still excludes an unaccepted create from snapshots.
        // While the turn is Open the row stays live so a cancel during the
        // final's retry window can still delete it.
        let mut changed = post.pending_body.take().is_some();
        if closed {
            post.ended = true;
            changed = true;
        }
        if changed {
            repository.save_progress_post(&post)?;
        }
        return Ok(());
    }
    if post.prepared_event_id.is_some() && post.post_event_id.is_none() {
        return create_post(repository, publisher, notice, &turn.bot_id, post, now);
    }
    match next_progress_step(&post.clock(), now) {
        ProgressStep::Wait => Ok(()),
        ProgressStep::Create => create_post(repository, publisher, notice, &turn.bot_id, post, now),
        ProgressStep::Edit => edit_post(repository, relay, notice, &turn.bot_id, post, now),
    }
}

fn create_post<R, P>(
    repository: &mut R,
    publisher: &P,
    notice: &mut impl FnMut(&str),
    bot_id: &BotId,
    mut post: ProgressPost,
    now: i64,
) -> Result<(), R::Error>
where
    R: HostRepository,
    P: OutboundPublisher,
    P::Error: fmt::Display,
{
    let body = post.post_body.clone().or_else(|| {
        post.pending_body
            .take()
            .map(|body| stamp_outbound(&body, &outbound_prefix_for(bot_id)))
    });
    let Some(stamped) = body else {
        return Ok(());
    };
    post.post_body = Some(stamped);
    let mut attempt = post.create_attempt(post.post_body.as_deref().expect("post body"));
    let outcome = record_and_send(publisher, &mut attempt, |attempt| {
        post.prepared_event_id
            .clone_from(&attempt.prepared_event_id);
        post.prepared_created_at = attempt.prepared_created_at;
        repository.save_progress_post(&post)
    })?;
    match outcome {
        SendOutcome::Accepted(event_id) => {
            post.post_event_id = Some(event_id);
            post.last_send_at = Some(now);
            repository.save_progress_post(&post)
        }
        SendOutcome::Retry(error) => {
            // The prepared id stays recorded; the next tick redelivers it.
            let message = format!(
                "progress post for ask {} was not accepted; retrying: {error}",
                post.ask_id
            );
            if notice_retry(&mut post, now, notice, &message) {
                repository.save_progress_post(&post)?;
            }
            Ok(())
        }
        SendOutcome::PrepareFailed(error) => {
            notice(&format!(
                "progress post for ask {} could not be built; progress ended: {error}",
                post.ask_id
            ));
            post.ended = true;
            post.pending_body = None;
            repository.save_progress_post(&post)
        }
        SendOutcome::Rejected(error) => {
            notice(&format!(
                "progress post for ask {} was rejected; progress ended: {error}",
                post.ask_id
            ));
            post.ended = true;
            post.pending_body = None;
            repository.save_progress_post(&post)
        }
    }
}

fn edit_post<R, L>(
    repository: &mut R,
    relay: &L,
    notice: &mut impl FnMut(&str),
    bot_id: &BotId,
    mut post: ProgressPost,
    now: i64,
) -> Result<(), R::Error>
where
    R: HostRepository,
    L: ProgressRelay,
{
    let (Some(body), Some(target)) = (post.pending_body.clone(), post.post_event_id.as_deref())
    else {
        return Ok(());
    };
    let Some(target) = EventId::parse_hex(target) else {
        notice(&format!(
            "progress post id for ask {} is not an event id; progress ended",
            post.ask_id
        ));
        post.ended = true;
        return repository.save_progress_post(&post);
    };
    let stamped = stamp_outbound(&body, &outbound_prefix_for(bot_id));
    match relay.edit(&post.ask_id, &post.channel_id, &target, &stamped) {
        Ok(ProgressRelayDispatch::Accepted) => {
            post.edit_count += 1;
            post.last_send_at = Some(now);
            post.pending_body = None;
            post.retry_noticed_at = None;
            repository.save_progress_post(&post)?;
        }
        Ok(ProgressRelayDispatch::Pending) => {}
        Err(error) => {
            let message = format!(
                "progress edit for ask {} failed; retrying the newest body: {error}",
                post.ask_id
            );
            if notice_retry(&mut post, now, notice, &message) {
                repository.save_progress_post(&post)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct RecordingProgressRelay {
    pub edits: std::sync::Mutex<Vec<(String, String, String)>>,
    pub deletes: std::sync::Mutex<Vec<(String, String)>>,
    pub fail_edit: std::sync::Mutex<bool>,
    pub fail_delete: std::sync::Mutex<bool>,
    pub reject_delete: std::sync::Mutex<bool>,
}

#[cfg(test)]
impl ProgressRelay for RecordingProgressRelay {
    fn edit(
        &self,
        _ask_id: &str,
        channel_id: &str,
        post_event_id: &EventId,
        content: &str,
    ) -> Result<ProgressRelayDispatch, PublishError> {
        self.edits.lock().expect("edits").push((
            channel_id.to_owned(),
            post_event_id.as_str().to_owned(),
            content.to_owned(),
        ));
        if *self.fail_edit.lock().expect("fail") {
            return Err(PublishError::NotAccepted {
                detail: "edit dropped".to_owned(),
            });
        }
        Ok(ProgressRelayDispatch::Accepted)
    }

    fn delete(
        &self,
        _ask_id: &str,
        channel_id: &str,
        post_event_id: &EventId,
    ) -> Result<ProgressRelayDispatch, PublishError> {
        self.deletes
            .lock()
            .expect("deletes")
            .push((channel_id.to_owned(), post_event_id.as_str().to_owned()));
        if *self.fail_delete.lock().expect("fail") {
            return Err(PublishError::NotAccepted {
                detail: "delete dropped".to_owned(),
            });
        }
        if *self.reject_delete.lock().expect("reject") {
            return Err(PublishError::Rejected {
                detail: "delete forbidden".to_owned(),
            });
        }
        Ok(ProgressRelayDispatch::Accepted)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use botserver_domain::progress::{
        PROGRESS_BODY_MAX_BYTES, PROGRESS_EDIT_INTERVAL_SECS, PROGRESS_INITIAL_HOLD_SECS,
    };

    use super::*;
    use crate::sqlite::SqliteRepository;
    use crate::test_support::{
        event_id, open_repository as open_repo, open_repository_with_tags, quiet, FakePublisher,
        CHANNEL,
    };

    #[derive(Debug)]
    struct BlockingRelay {
        started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl ProgressRelay for BlockingRelay {
        fn edit(
            &self,
            _ask_id: &str,
            _channel_id: &str,
            _post_event_id: &EventId,
            _content: &str,
        ) -> Result<ProgressRelayDispatch, PublishError> {
            Ok(self.block_until_released())
        }

        fn delete(
            &self,
            _ask_id: &str,
            _channel_id: &str,
            _post_event_id: &EventId,
        ) -> Result<ProgressRelayDispatch, PublishError> {
            Ok(self.block_until_released())
        }
    }

    impl BlockingRelay {
        fn block_until_released(&self) -> ProgressRelayDispatch {
            if let Some(started) = self.started.lock().unwrap().take() {
                let _ = started.send(());
            }
            let _ = self
                .release
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(1));
            ProgressRelayDispatch::Accepted
        }
    }

    /// Fix the open time so the hold is deterministic in tests.
    fn open_turn_at(repository: &mut SqliteRepository, opened_at: i64) -> TurnRecord {
        let mut turn = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        let mut post = new_progress_post(repository, &turn, "ask-1", opened_at).unwrap();
        post.opened_at = opened_at;
        repository.save_progress_post(&post).unwrap();
        turn.opened_at = Some(opened_at);
        turn
    }

    fn flush_all<L: ProgressRelay>(
        repository: &mut SqliteRepository,
        publisher: &FakePublisher,
        relay: &L,
        notices: &mut Vec<String>,
        turn: &TurnRecord,
        now: i64,
    ) {
        let bot_id = turn.bot_id.clone();
        for (post, stored_turn) in repository.progress_posts_pending_flush(&bot_id).unwrap() {
            flush_progress(
                repository,
                publisher,
                relay,
                &mut |text: &str| notices.push(text.to_owned()),
                &stored_turn,
                post,
                now,
            )
            .unwrap();
        }
    }

    async fn apply_until(
        repository: &mut SqliteRepository,
        relay: &BackgroundProgressRelay,
        done: impl Fn(&ProgressPost) -> bool,
    ) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                apply_relay_completions(repository, relay, &mut quiet()).unwrap();
                let post = repository.progress_post("ask-1").unwrap().unwrap();
                if done(&post) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("progress completion");
    }

    #[test]
    fn progress_before_hold_is_recorded_not_posted() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "  working  ", 1_005).unwrap();
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert_eq!(post.pending_body.as_deref(), Some("working"));
        assert_eq!(post.reply_to_event_id, event_id('a'));
        assert!(post.prepared_event_id.is_none());
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            1_000 + PROGRESS_INITIAL_HOLD_SECS - 1,
        );
        assert!(publisher.sends.lock().unwrap().is_empty());
        assert!(notices.is_empty());
    }

    #[test]
    fn progress_posts_once_after_hold_without_mention() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        let now = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            now,
        );
        let prepared = publisher.prepared.lock().unwrap();
        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].body, "[bot]: working");
        assert_eq!(prepared[0].reply_to_event_id, Some(event_id('a')));
        assert_eq!(prepared[0].channel_id, CHANNEL);
        assert!(prepared[0].mention.is_empty(), "no --mention on progress");
        drop(prepared);
        let sends = publisher.sends.lock().unwrap().clone();
        assert_eq!(sends.len(), 1);
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert_eq!(post.post_event_id.as_deref(), Some(sends[0].as_str()));
        assert_eq!(post.prepared_event_id.as_deref(), Some(sends[0].as_str()));
        assert_eq!(post.post_body.as_deref(), Some("[bot]: working"));
        assert_eq!(post.last_send_at, Some(now));
        assert_eq!(post.edit_count, 0);
        assert!(post.pending_body.is_none());
        // The tick is idle once the body is sent.
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            now + 1,
        );
        assert_eq!(publisher.sends.lock().unwrap().len(), 1);
        assert!(relay.edits.lock().unwrap().is_empty());
        assert!(notices.is_empty());
    }

    #[test]
    fn later_bodies_coalesce_into_one_edit_after_the_interval() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "step 1", 1_005).unwrap();
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        let post_id = repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .post_event_id
            .unwrap();

        record_progress(&mut repository, &mut quiet(), &turn, "step 2", created + 2).unwrap();
        record_progress(&mut repository, &mut quiet(), &turn, "step 3", created + 4).unwrap();
        // Inside the interval: the newest body waits, nothing is sent.
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created + PROGRESS_EDIT_INTERVAL_SECS - 1,
        );
        assert!(relay.edits.lock().unwrap().is_empty());
        assert_eq!(
            repository
                .progress_post("ask-1")
                .unwrap()
                .unwrap()
                .pending_body
                .as_deref(),
            Some("step 3")
        );

        let edited = created + PROGRESS_EDIT_INTERVAL_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            edited,
        );
        let edits = relay.edits.lock().unwrap().clone();
        assert_eq!(
            edits,
            vec![(
                CHANNEL.to_owned(),
                post_id.clone(),
                "[bot]: step 3".to_owned()
            )]
        );
        assert_eq!(publisher.sends.lock().unwrap().len(), 1, "no second post");
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert_eq!(post.edit_count, 1);
        assert_eq!(post.last_send_at, Some(edited));
        assert!(post.pending_body.is_none());
        assert!(notices.is_empty());
    }

    #[test]
    fn edit_cap_drops_later_bodies_with_one_notice() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "start", 1_005).unwrap();
        let mut now = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            now,
        );
        for index in 0..PROGRESS_EDIT_CAP {
            let mut recorded = Vec::new();
            record_progress(
                &mut repository,
                &mut |text: &str| recorded.push(text.to_owned()),
                &turn,
                &format!("edit {index}"),
                now + 1,
            )
            .unwrap();
            assert!(recorded.is_empty());
            now += PROGRESS_EDIT_INTERVAL_SECS;
            flush_all(
                &mut repository,
                &publisher,
                &relay,
                &mut notices,
                &turn,
                now,
            );
        }
        let cap = usize::try_from(PROGRESS_EDIT_CAP).expect("cap");
        assert_eq!(relay.edits.lock().unwrap().len(), cap, "exactly the cap");
        let mut recorded = Vec::new();
        for _ in 0..3 {
            record_progress(
                &mut repository,
                &mut |text: &str| recorded.push(text.to_owned()),
                &turn,
                "past the cap",
                now + 1,
            )
            .unwrap();
        }
        assert_eq!(recorded.len(), 1, "one operator notice");
        assert!(recorded[0].contains("edit cap"));
        now += PROGRESS_EDIT_INTERVAL_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            now,
        );
        assert_eq!(relay.edits.lock().unwrap().len(), cap);
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.pending_body.is_none());
        assert!(post.cap_noticed);
        assert!(notices.is_empty());
    }

    #[test]
    fn progress_body_is_trimmed_and_capped_at_1024_bytes() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let long = format!("  {}  ", "z".repeat(PROGRESS_BODY_MAX_BYTES * 2));
        record_progress(&mut repository, &mut quiet(), &turn, &long, 1_005).unwrap();
        let pending = repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .pending_body
            .unwrap();
        assert_eq!(pending.len(), PROGRESS_BODY_MAX_BYTES);
        assert!(pending.ends_with('…'));
        assert!(!pending.starts_with(' '));
    }

    #[test]
    fn empty_progress_body_records_nothing() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        record_progress(&mut repository, &mut quiet(), &turn, " \n ", 1_005).unwrap();
        assert!(repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .pending_body
            .is_none());
    }

    #[test]
    fn final_first_discards_the_pending_body_and_never_posts() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "almost", 1_005).unwrap();
        discard_pending(&mut repository, "ask-1").unwrap();
        repository
            .set_turn_state("ask-1", TurnState::Posted)
            .unwrap();
        let posted = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &posted,
            1_000_000,
        );
        assert!(publisher.sends.lock().unwrap().is_empty());
        assert!(repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .pending_body
            .is_none());
        assert!(repository.progress_post("ask-1").unwrap().unwrap().ended);
    }

    #[test]
    fn final_leaves_the_progress_post_up_and_drops_a_pending_edit() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        record_progress(&mut repository, &mut quiet(), &turn, "nearly", created + 1).unwrap();
        repository
            .set_turn_state("ask-1", TurnState::Posted)
            .unwrap();
        let posted = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &posted,
            created + PROGRESS_EDIT_INTERVAL_SECS,
        );
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.post_event_id.is_some(), "post stays up");
        assert!(post.pending_body.is_none());
        assert!(post.ended);
        assert!(relay.edits.lock().unwrap().is_empty());
        assert!(relay.deletes.lock().unwrap().is_empty());
    }

    #[test]
    fn cancel_ends_progress_and_buzz_deletes_the_post() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        let post_id = repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .post_event_id
            .unwrap();
        record_progress(&mut repository, &mut quiet(), &turn, "later", created + 1).unwrap();
        delete_progress_post(&mut repository, &relay, &mut quiet(), "ask-1").unwrap();
        assert_eq!(
            relay.deletes.lock().unwrap().clone(),
            vec![(CHANNEL.to_owned(), post_id)]
        );
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.ended);
        assert!(post.pending_body.is_none());
        // Ended rows never flush again, even with an open turn.
        assert!(repository
            .progress_posts_pending_flush(&turn.bot_id)
            .unwrap()
            .is_empty());
        record_progress(&mut repository, &mut quiet(), &turn, "ignored", created + 2).unwrap();
        assert!(repository
            .progress_posts_pending_flush(&turn.bot_id)
            .unwrap()
            .is_empty());
        // A second cancel does not delete twice.
        delete_progress_post(&mut repository, &relay, &mut quiet(), "ask-1").unwrap();
        assert_eq!(relay.deletes.lock().unwrap().len(), 1);
    }

    #[test]
    fn cancel_before_any_post_deletes_nothing() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let relay = RecordingProgressRelay::default();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        delete_progress_post(&mut repository, &relay, &mut quiet(), "ask-1").unwrap();
        assert!(relay.deletes.lock().unwrap().is_empty());
        assert!(repository.progress_post("ask-1").unwrap().unwrap().ended);
    }

    #[test]
    fn crash_after_create_dispatch_redelivers_the_same_id() {
        let root = event_id('b');
        let parent = event_id('c');
        let mut repository = open_repository_with_tags(&format!(
            r#"[["e","{}","","root"],["e","{}","","reply"]]"#,
            root.as_str(),
            parent.as_str()
        ));
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        *publisher.fail.lock().unwrap() = Some(PublishError::NotAccepted {
            detail: "connection dropped before OK".to_owned(),
        });
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.prepared_event_id.is_some());
        assert!(post.post_event_id.is_none());
        let prepared_id = post
            .prepared_event_id
            .clone()
            .expect("recorded before send");
        assert!(post.pending_body.is_none(), "body moved to post_body");
        assert_eq!(post.post_body.as_deref(), Some("[bot]: working"));
        assert_eq!(notices.len(), 1);
        assert!(notices[0].contains("retrying"));
        // The dispatched-without-accept row is redelivered next tick.
        assert_eq!(
            repository
                .progress_posts_pending_flush(&turn.bot_id)
                .unwrap()
                .len(),
            1
        );
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created + 1,
        );
        let sends = publisher.sends.lock().unwrap().clone();
        assert_eq!(sends, vec![prepared_id.clone(), prepared_id.clone()]);
        assert!(publisher
            .prepared
            .lock()
            .unwrap()
            .iter()
            .all(|attempt| attempt.thread_root_event_id.as_ref() == Some(&root)));
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert_eq!(post.post_event_id.as_deref(), Some(prepared_id.as_str()));
        assert_eq!(post.last_send_at, Some(created + 1));
        assert!(!post.ended);
    }

    #[test]
    fn rejected_create_ends_progress_without_a_retry() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        *publisher.fail.lock().unwrap() = Some(PublishError::Rejected {
            detail: "invalid: not a member".to_owned(),
        });
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created + 1,
        );
        assert_eq!(publisher.sends.lock().unwrap().len(), 1);
        assert!(repository.progress_post("ask-1").unwrap().unwrap().ended);
        assert_eq!(notices.len(), 1);
    }

    #[test]
    fn create_build_failure_keeps_its_operator_notice() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        *publisher.fail_prepare.lock().unwrap() =
            Some(PublishError::Build("invalid event".to_owned()));
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            1_000 + PROGRESS_INITIAL_HOLD_SECS,
        );
        assert!(publisher.sends.lock().unwrap().is_empty());
        assert!(repository.progress_post("ask-1").unwrap().unwrap().ended);
        assert_eq!(notices.len(), 1);
        assert!(notices[0].contains("could not be built"));
    }

    #[test]
    fn failed_edit_is_superseded_by_the_next_body() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "one", 1_005).unwrap();
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        record_progress(&mut repository, &mut quiet(), &turn, "two", created + 1).unwrap();
        *relay.fail_edit.lock().unwrap() = true;
        let first_edit = created + PROGRESS_EDIT_INTERVAL_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            first_edit,
        );
        assert_eq!(notices.len(), 1);
        assert!(notices[0].contains("retrying"));
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert_eq!(post.edit_count, 0, "failed edits do not consume the cap");
        assert_eq!(post.pending_body.as_deref(), Some("two"));
        *relay.fail_edit.lock().unwrap() = false;
        record_progress(
            &mut repository,
            &mut quiet(),
            &turn,
            "three",
            first_edit + 1,
        )
        .unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            first_edit + PROGRESS_EDIT_INTERVAL_SECS,
        );
        let edits = relay.edits.lock().unwrap();
        assert_eq!(edits.len(), 2);
        assert_eq!(edits[1].2, "[bot]: three");
        assert_eq!(
            repository
                .progress_post("ask-1")
                .unwrap()
                .unwrap()
                .edit_count,
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn progress_tick_returns_while_an_edit_or_delete_is_outstanding() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "one", 1_005).unwrap();
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        record_progress(&mut repository, &mut quiet(), &turn, "two", created + 1).unwrap();

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocking = Arc::new(BlockingRelay {
            started: Mutex::new(Some(started_tx)),
            release: Mutex::new(release_rx),
        });
        let background =
            BackgroundProgressRelay::new(Arc::clone(&blocking) as Arc<dyn ProgressRelay>);
        let flush_started = std::time::Instant::now();
        flush_all(
            &mut repository,
            &publisher,
            &background,
            &mut notices,
            &turn,
            created + PROGRESS_EDIT_INTERVAL_SECS,
        );
        assert!(
            flush_started.elapsed() < std::time::Duration::from_millis(500),
            "progress tick waited for the relay"
        );

        tokio::time::timeout(std::time::Duration::from_secs(1), started_rx)
            .await
            .expect("edit started")
            .expect("start signal");
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert_eq!(post.edit_count, 0, "queued edit is not accepted yet");
        assert_eq!(post.pending_body.as_deref(), Some("two"));
        release_tx.send(()).unwrap();
        apply_until(&mut repository, &background, |post| post.edit_count == 1).await;

        let (delete_started_tx, delete_started_rx) = tokio::sync::oneshot::channel();
        *blocking.started.lock().unwrap() = Some(delete_started_tx);
        let delete_started = std::time::Instant::now();
        delete_progress_post(&mut repository, &background, &mut quiet(), "ask-1").unwrap();
        assert!(
            delete_started.elapsed() < std::time::Duration::from_millis(500),
            "progress delete waited for the relay"
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), delete_started_rx)
            .await
            .expect("delete started")
            .expect("delete start signal");
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.ended);
        assert!(post.delete_pending);
        release_tx.send(()).unwrap();
        apply_until(&mut repository, &background, |post| !post.delete_pending).await;
    }

    #[test]
    fn failed_edits_do_not_consume_the_edit_cap() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "start", 1_005).unwrap();
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        *relay.fail_edit.lock().unwrap() = true;
        for index in 0..PROGRESS_EDIT_CAP {
            record_progress(
                &mut repository,
                &mut quiet(),
                &turn,
                &format!("failed {index}"),
                created + 1,
            )
            .unwrap();
            flush_all(
                &mut repository,
                &publisher,
                &relay,
                &mut notices,
                &turn,
                created + PROGRESS_EDIT_INTERVAL_SECS,
            );
        }
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert_eq!(post.edit_count, 0);
        assert!(post.pending_body.is_some());

        *relay.fail_edit.lock().unwrap() = false;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created + PROGRESS_EDIT_INTERVAL_SECS,
        );
        assert_eq!(
            repository
                .progress_post("ask-1")
                .unwrap()
                .unwrap()
                .edit_count,
            1
        );
    }

    #[test]
    fn failed_cancel_delete_stays_pending_and_retries() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            1_000 + PROGRESS_INITIAL_HOLD_SECS,
        );
        *relay.fail_delete.lock().unwrap() = true;
        delete_progress_post(&mut repository, &relay, &mut quiet(), "ask-1").unwrap();
        assert!(
            repository
                .progress_post("ask-1")
                .unwrap()
                .unwrap()
                .delete_pending
        );
        assert_eq!(
            repository
                .progress_posts_pending_flush(&turn.bot_id)
                .unwrap()
                .len(),
            1
        );

        *relay.fail_delete.lock().unwrap() = false;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            2_000,
        );
        assert!(
            !repository
                .progress_post("ask-1")
                .unwrap()
                .unwrap()
                .delete_pending
        );
        assert_eq!(relay.deletes.lock().unwrap().len(), 2);
    }

    #[test]
    fn retryable_create_notices_are_rate_limited() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        let start = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        for offset in 0..300 {
            *publisher.fail.lock().unwrap() = Some(PublishError::NotAccepted {
                detail: "relay unavailable".to_owned(),
            });
            flush_all(
                &mut repository,
                &publisher,
                &relay,
                &mut notices,
                &turn,
                start + offset,
            );
        }
        assert_eq!(publisher.sends.lock().unwrap().len(), 300);
        assert_eq!(notices.len(), 10, "at most one notice per 30 seconds");
    }

    #[test]
    fn retryable_edit_notices_are_rate_limited() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "start", 1_005).unwrap();
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        record_progress(&mut repository, &mut quiet(), &turn, "pending", created + 1).unwrap();
        *relay.fail_edit.lock().unwrap() = true;
        for offset in 0..300 {
            flush_all(
                &mut repository,
                &publisher,
                &relay,
                &mut notices,
                &turn,
                created + PROGRESS_EDIT_INTERVAL_SECS + offset,
            );
        }
        assert_eq!(notices.len(), 10, "at most one notice per 30 seconds");
        assert_eq!(
            repository
                .progress_post("ask-1")
                .unwrap()
                .unwrap()
                .edit_count,
            0
        );
    }

    #[test]
    fn retryable_delete_notices_are_rate_limited() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "start", 1_005).unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            1_000 + PROGRESS_INITIAL_HOLD_SECS,
        );
        *relay.fail_delete.lock().unwrap() = true;
        let start = crate::unix_now().unwrap();
        delete_progress_post(
            &mut repository,
            &relay,
            &mut |text| notices.push(text.to_owned()),
            "ask-1",
        )
        .unwrap();
        for offset in 0..300 {
            flush_all(
                &mut repository,
                &publisher,
                &relay,
                &mut notices,
                &turn,
                start + offset,
            );
        }
        assert!(notices.len() <= 11, "at most one notice per 30 seconds");
        assert!(
            repository
                .progress_post("ask-1")
                .unwrap()
                .unwrap()
                .delete_pending
        );
    }

    #[test]
    fn rejected_delete_ends_best_effort_retry() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "start", 1_005).unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            1_000 + PROGRESS_INITIAL_HOLD_SECS,
        );
        *relay.reject_delete.lock().unwrap() = true;
        delete_progress_post(
            &mut repository,
            &relay,
            &mut |text| notices.push(text.to_owned()),
            "ask-1",
        )
        .unwrap();
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(!post.delete_pending);
        assert!(notices.iter().any(|notice| notice.contains("not retrying")));
    }

    /// The stored attempt after a retryable final publish failure: the
    /// turn stays open, `record_and_send` cleared `dispatched`, and
    /// `retry_outbound` redelivers the prepared event on the tick (D28).
    fn final_attempt_in_retry_window() -> OutboundAttempt {
        OutboundAttempt {
            ask_id: "ask-1".to_owned(),
            body: "[bot]: done".to_owned(),
            channel_id: CHANNEL.to_owned(),
            reply_to_event_id: Some(event_id('a')),
            thread_root_event_id: None,
            mention: "c".repeat(64),
            outbound_event_id: None,
            prepared_event_id: Some("e".repeat(64)),
            prepared_created_at: Some(1_700_000_000),
            dispatched: false,
        }
    }

    #[test]
    fn flush_skips_relay_once_a_final_is_in_flight() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "pending", 1_005).unwrap();
        repository
            .save_outbound_attempt(&final_attempt_in_retry_window())
            .unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            1_000 + PROGRESS_INITIAL_HOLD_SECS,
        );
        assert!(publisher.sends.lock().unwrap().is_empty());
        assert!(relay.edits.lock().unwrap().is_empty());
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.pending_body.is_none(), "the pending body is discarded");
        assert!(post.post_event_id.is_none());
        assert!(!post.ended, "the turn is still open");
        assert!(notices.is_empty());
    }

    #[test]
    fn cancel_during_the_final_retry_window_still_deletes_an_unaccepted_create() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        *publisher.fail.lock().unwrap() = Some(PublishError::NotAccepted {
            detail: "dropped".to_owned(),
        });
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        let prepared = repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .prepared_event_id
            .unwrap();
        // A final arrives and its publish also fails transiently; the turn
        // is still open, so a trigger delete can still cancel it (D28).
        repository
            .save_outbound_attempt(&final_attempt_in_retry_window())
            .unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created + 1,
        );
        assert_eq!(
            publisher.sends.lock().unwrap().len(),
            1,
            "no redelivery once a final is in flight"
        );
        assert!(!repository.progress_post("ask-1").unwrap().unwrap().ended);
        delete_progress_post(&mut repository, &relay, &mut quiet(), "ask-1").unwrap();
        assert_eq!(
            relay.deletes.lock().unwrap().clone(),
            vec![(CHANNEL.to_owned(), prepared)]
        );
    }

    #[test]
    fn unaccepted_create_on_a_closed_turn_is_ended_and_leaves_the_scan() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        *publisher.fail.lock().unwrap() = Some(PublishError::NotAccepted {
            detail: "dropped".to_owned(),
        });
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.prepared_event_id.is_some() && post.post_event_id.is_none());
        let prepared = post.prepared_event_id.clone().unwrap();
        repository
            .set_turn_state("ask-1", TurnState::Posted)
            .unwrap();
        let posted = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        assert_eq!(
            repository
                .progress_posts_pending_flush(&turn.bot_id)
                .unwrap()
                .len(),
            1
        );
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &posted,
            created + 1,
        );
        assert_eq!(publisher.sends.lock().unwrap().len(), 1, "no redelivery");
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        assert!(post.ended);
        assert!(repository
            .progress_posts_pending_flush(&turn.bot_id)
            .unwrap()
            .is_empty());
        // The dispatched create may have landed: it stays excluded.
        assert_eq!(
            repository.progress_post_event_ids(CHANNEL).unwrap(),
            vec![EventId::parse_hex(&prepared).unwrap()]
        );
    }

    #[test]
    fn progress_once_a_final_is_in_flight_records_nothing() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        repository
            .save_outbound_attempt(&final_attempt_in_retry_window())
            .unwrap();
        record_progress(&mut repository, &mut quiet(), &turn, "late", 1_005).unwrap();
        assert!(repository
            .progress_post("ask-1")
            .unwrap()
            .unwrap()
            .pending_body
            .is_none());
    }

    #[test]
    fn progress_on_a_non_open_turn_records_nothing() {
        let mut repository = open_repo();
        repository
            .set_turn_state("ask-1", TurnState::Cancelled)
            .unwrap();
        let turn = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        record_progress(&mut repository, &mut quiet(), &turn, "late", 1_005).unwrap();
        assert!(repository.progress_post("ask-1").unwrap().is_none());
    }

    #[test]
    fn hold_counts_from_the_turn_open_time_not_the_first_body() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        // First body arrives after the hold already elapsed: post at once.
        let now = 1_000 + PROGRESS_INITIAL_HOLD_SECS + 5;
        record_progress(&mut repository, &mut quiet(), &turn, "late start", now).unwrap();
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            now,
        );
        assert_eq!(publisher.sends.lock().unwrap().len(), 1);
    }

    #[test]
    fn open_next_turn_records_the_open_time() {
        let repository = open_repo();
        let turn = repository.turn_by_ask_id("ask-1").unwrap().unwrap();
        let opened_at = turn.opened_at.expect("opened_at set on open");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(i64::try_from(now).unwrap() - opened_at < 60);
    }

    #[test]
    fn progress_post_event_ids_cover_accepted_and_dispatched_creates() {
        let mut repository = open_repo();
        let turn = open_turn_at(&mut repository, 1_000);
        assert!(repository
            .progress_post_event_ids(CHANNEL)
            .unwrap()
            .is_empty());
        let publisher = FakePublisher::default();
        let relay = RecordingProgressRelay::default();
        let mut notices = Vec::new();
        record_progress(&mut repository, &mut quiet(), &turn, "working", 1_005).unwrap();
        *publisher.fail.lock().unwrap() = Some(PublishError::NotAccepted {
            detail: "dropped".to_owned(),
        });
        let created = 1_000 + PROGRESS_INITIAL_HOLD_SECS;
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created,
        );
        let post = repository.progress_post("ask-1").unwrap().unwrap();
        let prepared = post.prepared_event_id.clone().unwrap();
        assert_eq!(
            repository.progress_post_event_ids(CHANNEL).unwrap(),
            vec![EventId::parse_hex(&prepared).unwrap()],
            "a dispatched create may have landed"
        );
        flush_all(
            &mut repository,
            &publisher,
            &relay,
            &mut notices,
            &turn,
            created + 1,
        );
        assert_eq!(
            repository.progress_post_event_ids(CHANNEL).unwrap(),
            vec![EventId::parse_hex(&prepared).unwrap()]
        );
        assert!(repository
            .progress_post_event_ids("ffffffff-ffff-ffff-ffff-ffffffffffff")
            .unwrap()
            .is_empty());
    }
}
