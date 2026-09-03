//! SQLite host-state adapter.

use std::path::Path;
use std::time::Duration;

use botserver_domain::{BotId, EventId, TurnTransition};
use rusqlite::{params, Connection, OptionalExtension};

use crate::outbox::OutboundAttempt;
use crate::{
    HostRepository, IndexedRelayEvent, NewTurn, SessionRecord, TurnRecord, TurnReplacement,
    TurnState,
};

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

fn add_column_if_missing(connection: &Connection, sql: &str) -> rusqlite::Result<()> {
    if let Err(error) = connection.execute(sql, []) {
        let duplicate_column = match &error {
            rusqlite::Error::SqliteFailure(_, Some(message)) => {
                message.contains("duplicate column name")
            }
            _ => false,
        };
        if !duplicate_column {
            return Err(error);
        }
    }
    Ok(())
}

/// SQLite-backed host repository.
#[derive(Debug)]
pub struct SqliteRepository {
    connection: Connection,
}

impl SqliteRepository {
    /// Open a database and initialize its schema.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot open or initialize the database.
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        Self::from_connection(Connection::open(path)?)
    }

    /// Initialize a repository around an existing connection.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot configure or initialize the database.
    pub fn from_connection(connection: Connection) -> rusqlite::Result<Self> {
        connection.busy_timeout(SQLITE_BUSY_TIMEOUT)?;
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;

             CREATE TABLE IF NOT EXISTS processed_events (
                 event_id TEXT PRIMARY KEY NOT NULL
                     CHECK(length(event_id) = 64)
             ) STRICT;

             CREATE TABLE IF NOT EXISTS relay_events (
                 event_id TEXT PRIMARY KEY NOT NULL,
                 author_pubkey TEXT NOT NULL CHECK(length(author_pubkey) = 64),
                 created_at INTEGER NOT NULL,
                 kind INTEGER NOT NULL,
                 content TEXT NOT NULL,
                 tags_json TEXT NOT NULL,
                 channel_id TEXT,
                 target_event_id TEXT CHECK(
                     target_event_id IS NULL OR length(target_event_id) = 64
                 )
             ) STRICT;

             CREATE INDEX IF NOT EXISTS relay_events_channel_time
                 ON relay_events(channel_id, created_at);

             CREATE INDEX IF NOT EXISTS relay_events_target
                 ON relay_events(target_event_id);

             CREATE TABLE IF NOT EXISTS pending_ingest_events (
                 event_id TEXT PRIMARY KEY NOT NULL
                     REFERENCES relay_events(event_id)
             ) STRICT;

              CREATE TABLE IF NOT EXISTS sessions (
                  id INTEGER PRIMARY KEY,
                  bot_id TEXT NOT NULL,
                  channel_id TEXT NOT NULL,
                  session_name TEXT NOT NULL UNIQUE,
                  occupant_logical_id TEXT,
                  renew_id TEXT,
                  ask_context_event_id TEXT CHECK(
                      ask_context_event_id IS NULL OR length(ask_context_event_id) = 64
                  ),
                  ask_context_created_at INTEGER,
                  UNIQUE(bot_id, channel_id)
              ) STRICT;

             CREATE TABLE IF NOT EXISTS turns (
                 sequence INTEGER PRIMARY KEY,
                 session_id INTEGER NOT NULL REFERENCES sessions(id),
                 event_id TEXT NOT NULL CHECK(length(event_id) = 64),
                 ask_id TEXT UNIQUE,
                 reply_to_event_id TEXT CHECK(
                     reply_to_event_id IS NULL OR length(reply_to_event_id) = 64
                 ),
                  state TEXT NOT NULL CHECK(
                      state IN ('queued', 'open', 'posted', 'failed', 'cancelled')
                  ),
                  publish_claimed INTEGER NOT NULL DEFAULT 0 CHECK(
                      publish_claimed IN (0, 1)
                  ),
                  CHECK(
                      (state = 'queued' AND ask_id IS NULL)
                      OR (state = 'open' AND ask_id IS NOT NULL)
                      OR state IN ('posted', 'failed', 'cancelled')
                  ),
                  CHECK(publish_claimed = 0 OR state = 'open')
             ) STRICT;

             CREATE UNIQUE INDEX IF NOT EXISTS turns_one_open_per_session
                 ON turns(session_id) WHERE state = 'open';

             CREATE UNIQUE INDEX IF NOT EXISTS turns_one_live_per_event
                 ON turns(event_id) WHERE state IN ('queued', 'open');

              CREATE INDEX IF NOT EXISTS turns_session_order
                   ON turns(session_id, sequence);

              CREATE TABLE IF NOT EXISTS outbound_attempts (
                  ask_id TEXT PRIMARY KEY NOT NULL,
                  body TEXT NOT NULL,
                  channel_id TEXT NOT NULL,
                  reply_to_event_id TEXT NOT NULL CHECK(length(reply_to_event_id) = 64),
                  mention TEXT NOT NULL,
                  outbound_event_id TEXT CHECK(
                      outbound_event_id IS NULL OR length(outbound_event_id) = 64
                  ),
                  dispatched INTEGER NOT NULL DEFAULT 0 CHECK(dispatched IN (0, 1))
              ) STRICT;",
        )?;
        add_column_if_missing(
            &connection,
            "ALTER TABLE turns ADD COLUMN publish_claimed INTEGER NOT NULL DEFAULT 0",
        )?;
        add_column_if_missing(
            &connection,
            "ALTER TABLE outbound_attempts ADD COLUMN dispatched INTEGER NOT NULL DEFAULT 0",
        )?;
        add_column_if_missing(
            &connection,
            "ALTER TABLE sessions ADD COLUMN ask_context_event_id TEXT",
        )?;
        add_column_if_missing(
            &connection,
            "ALTER TABLE sessions ADD COLUMN ask_context_created_at INTEGER",
        )?;
        Ok(Self { connection })
    }

    fn read_turn(row: &rusqlite::Row<'_>) -> rusqlite::Result<TurnRecord> {
        let bot_id: String = row.get(1)?;
        let event_id: String = row.get(3)?;
        let reply_to_event_id: Option<String> = row.get(5)?;
        let state: String = row.get(6)?;

        Ok(TurnRecord {
            sequence: row.get(0)?,
            bot_id: parse_bot_id(&bot_id, 1)?,
            channel_id: row.get(2)?,
            event_id: parse_event_id(&event_id, 3)?,
            ask_id: row.get(4)?,
            reply_to_event_id: reply_to_event_id
                .as_deref()
                .map(|value| parse_event_id(value, 5))
                .transpose()?,
            state: TurnState::parse(&state)
                .ok_or_else(|| invalid_value(6, "invalid turn state"))?,
        })
    }

    fn read_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRecord> {
        let stored_bot_id: String = row.get(0)?;
        let event_id: Option<String> = row.get(5)?;
        Ok(SessionRecord {
            bot_id: parse_bot_id(&stored_bot_id, 0)?,
            channel_id: row.get(1)?,
            session_name: row.get(2)?,
            occupant_logical_id: row.get(3)?,
            renew_id: row.get(4)?,
            ask_context_event_id: event_id
                .as_deref()
                .map(|value| parse_event_id(value, 5))
                .transpose()?,
            ask_context_created_at: row.get(6)?,
        })
    }

    fn read_indexed_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<IndexedRelayEvent> {
        let event_id: String = row.get(0)?;
        let target_event_id: Option<String> = row.get(7)?;
        Ok(IndexedRelayEvent {
            event_id: parse_event_id(&event_id, 0)?,
            author_pubkey: row.get(1)?,
            created_at: row.get(2)?,
            kind: row.get(3)?,
            content: row.get(4)?,
            tags_json: row.get(5)?,
            channel_id: row.get(6)?,
            target_event_id: target_event_id
                .as_deref()
                .map(|value| parse_event_id(value, 7))
                .transpose()?,
        })
    }
}

impl HostRepository for SqliteRepository {
    type Error = rusqlite::Error;

    fn mark_event_processed(&mut self, event_id: &EventId) -> Result<bool, Self::Error> {
        let transaction = self.connection.transaction()?;
        let changed = transaction.execute(
            "INSERT INTO processed_events(event_id) VALUES (?1)
             ON CONFLICT(event_id) DO NOTHING",
            [event_id.as_str()],
        )?;
        transaction.execute(
            "DELETE FROM pending_ingest_events WHERE event_id = ?1",
            [event_id.as_str()],
        )?;
        transaction.commit()?;
        Ok(changed == 1)
    }

    fn index_event(
        &mut self,
        event: &IndexedRelayEvent,
        pending_action: bool,
    ) -> Result<bool, Self::Error> {
        let transaction = self.connection.transaction()?;
        let changed = transaction.execute(
            "INSERT INTO relay_events(
                 event_id, author_pubkey, created_at, kind, content, tags_json,
                 channel_id, target_event_id
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(event_id) DO NOTHING",
            params![
                event.event_id.as_str(),
                event.author_pubkey,
                event.created_at,
                event.kind,
                event.content,
                event.tags_json,
                event.channel_id,
                event.target_event_id.as_ref().map(EventId::as_str),
            ],
        )?;
        if pending_action {
            transaction.execute(
                "INSERT INTO pending_ingest_events(event_id) VALUES (?1)
                 ON CONFLICT(event_id) DO NOTHING",
                [event.event_id.as_str()],
            )?;
        }
        transaction.commit()?;
        Ok(changed == 1)
    }

    fn event_processed(&self, event_id: &EventId) -> Result<bool, Self::Error> {
        self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = ?1)",
            [event_id.as_str()],
            |row| row.get(0),
        )
    }

    fn indexed_event(&self, event_id: &EventId) -> Result<Option<IndexedRelayEvent>, Self::Error> {
        self.connection
            .query_row(
                "SELECT event_id, author_pubkey, created_at, kind, content, tags_json,
                        channel_id, target_event_id
                 FROM relay_events
                 WHERE event_id = ?1",
                [event_id.as_str()],
                Self::read_indexed_event,
            )
            .optional()
    }

    fn latest_body_for_event(&self, event_id: &EventId) -> Result<Option<String>, Self::Error> {
        self.connection
            .query_row(
                "SELECT content FROM relay_events
                 WHERE event_id = ?1
                    OR (
                        target_event_id = ?1
                        AND kind = 40003
                        AND author_pubkey = (
                            SELECT author_pubkey FROM relay_events WHERE event_id = ?1
                        )
                    )
                 ORDER BY created_at DESC, event_id DESC
                 LIMIT 1",
                [event_id.as_str()],
                |row| row.get(0),
            )
            .optional()
    }

    fn relay_replay_since(&self) -> Result<Option<i64>, Self::Error> {
        self.connection.query_row(
            "SELECT COALESCE(
                 (
                     SELECT MIN(e.created_at)
                     FROM pending_ingest_events AS pending
                     JOIN relay_events AS e ON e.event_id = pending.event_id
                     LEFT JOIN processed_events AS processed
                         ON processed.event_id = pending.event_id
                     WHERE processed.event_id IS NULL
                 ),
                 MAX(0, (SELECT MAX(created_at) FROM relay_events) - 900)
             )",
            [],
            |row| row.get(0),
        )
    }

    fn indexed_events_for_channel(
        &self,
        channel_id: &str,
    ) -> Result<Vec<IndexedRelayEvent>, Self::Error> {
        let mut statement = self.connection.prepare(
            "SELECT event_id, author_pubkey, created_at, kind, content, tags_json,
                    channel_id, target_event_id
             FROM relay_events
             WHERE channel_id = ?1
             ORDER BY created_at, event_id",
        )?;
        let events = statement
            .query_map([channel_id], Self::read_indexed_event)?
            .collect();
        events
    }

    fn enqueue_unprocessed_turn(
        &mut self,
        turn: &NewTurn,
    ) -> Result<Option<TurnRecord>, Self::Error> {
        let transaction = self.connection.transaction()?;
        let changed = transaction.execute(
            "INSERT INTO processed_events(event_id) VALUES (?1)
             ON CONFLICT(event_id) DO NOTHING",
            [turn.event_id.as_str()],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(None);
        }

        transaction.execute(
            "DELETE FROM pending_ingest_events WHERE event_id = ?1",
            [turn.event_id.as_str()],
        )?;
        let record = insert_queued_turn(&transaction, turn)?;
        transaction.commit()?;
        Ok(Some(record))
    }

    fn enqueue_turn(&mut self, turn: &NewTurn) -> Result<TurnRecord, Self::Error> {
        insert_queued_turn(&self.connection, turn)
    }

    fn save_session(&mut self, session: &SessionRecord) -> Result<(), Self::Error> {
        self.connection.execute(
            "INSERT INTO sessions(
                 bot_id, channel_id, session_name, occupant_logical_id, renew_id,
                 ask_context_event_id, ask_context_created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(bot_id, channel_id) DO UPDATE SET
                 session_name = excluded.session_name,
                 occupant_logical_id = excluded.occupant_logical_id,
                 renew_id = excluded.renew_id,
                 ask_context_event_id = excluded.ask_context_event_id,
                 ask_context_created_at = excluded.ask_context_created_at",
            params![
                session.bot_id.as_str(),
                session.channel_id,
                session.session_name,
                session.occupant_logical_id,
                session.renew_id,
                session.ask_context_event_id.as_ref().map(EventId::as_str),
                session.ask_context_created_at,
            ],
        )?;
        Ok(())
    }

    fn session(
        &self,
        bot_id: &BotId,
        channel_id: &str,
    ) -> Result<Option<SessionRecord>, Self::Error> {
        self.connection
            .query_row(
                "SELECT bot_id, channel_id, session_name, occupant_logical_id, renew_id,
                        ask_context_event_id, ask_context_created_at
                 FROM sessions WHERE bot_id = ?1 AND channel_id = ?2",
                params![bot_id.as_str(), channel_id],
                Self::read_session,
            )
            .optional()
    }

    fn session_by_name(&self, session_name: &str) -> Result<Option<SessionRecord>, Self::Error> {
        self.connection
            .query_row(
                "SELECT bot_id, channel_id, session_name, occupant_logical_id, renew_id,
                        ask_context_event_id, ask_context_created_at
                 FROM sessions WHERE session_name = ?1",
                [session_name],
                Self::read_session,
            )
            .optional()
    }

    fn open_next_turn(
        &mut self,
        bot_id: &BotId,
        channel_id: &str,
        ask_id: &str,
    ) -> Result<Option<TurnRecord>, Self::Error> {
        let transaction = self.connection.transaction()?;
        let sequence = transaction
            .query_row(
                "SELECT t.sequence
                 FROM turns AS t
                 JOIN sessions AS s ON s.id = t.session_id
                 WHERE s.bot_id = ?1 AND s.channel_id = ?2
                   AND t.state = 'queued'
                   AND NOT EXISTS (
                       SELECT 1 FROM turns AS active
                       WHERE active.session_id = s.id AND active.state = 'open'
                   )
                 ORDER BY t.sequence
                 LIMIT 1",
                params![bot_id.as_str(), channel_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        let Some(sequence) = sequence else {
            transaction.commit()?;
            return Ok(None);
        };
        let Some(transition) = TurnTransition::parse(TurnState::Queued, TurnState::Open) else {
            return Ok(None);
        };
        transaction.execute(
            "UPDATE turns SET ask_id = ?1, state = ?2
             WHERE sequence = ?3 AND state = ?4",
            params![
                ask_id,
                transition.to_state().as_str(),
                sequence,
                transition.from_state().as_str()
            ],
        )?;
        let record = transaction.query_row(
            "SELECT t.sequence, s.bot_id, s.channel_id, t.event_id, t.ask_id,
                    t.reply_to_event_id, t.state
             FROM turns AS t
             JOIN sessions AS s ON s.id = t.session_id
             WHERE t.sequence = ?1",
            [sequence],
            Self::read_turn,
        )?;
        transaction.commit()?;
        Ok(Some(record))
    }

    fn set_turn_state(&mut self, ask_id: &str, state: TurnState) -> Result<bool, Self::Error> {
        let Some(current) = self.turn_by_ask_id(ask_id)? else {
            return Ok(false);
        };
        let Some(transition) = TurnTransition::parse(current.state, state) else {
            return Ok(false);
        };
        let sql = if transition.to_state() == TurnState::Cancelled {
            "UPDATE turns SET state = ?1, publish_claimed = 0
             WHERE ask_id = ?2 AND state = ?3 AND publish_claimed = 0"
        } else {
            "UPDATE turns SET state = ?1, publish_claimed = 0
             WHERE ask_id = ?2 AND state = ?3"
        };
        let changed = self.connection.execute(
            sql,
            params![
                transition.to_state().as_str(),
                ask_id,
                transition.from_state().as_str()
            ],
        )?;
        Ok(changed == 1)
    }

    fn claim_turn_for_publish(&mut self, ask_id: &str) -> Result<bool, Self::Error> {
        let changed = self.connection.execute(
            "UPDATE turns SET publish_claimed = 1
             WHERE ask_id = ?1 AND state = 'open' AND publish_claimed = 0",
            [ask_id],
        )?;
        Ok(changed == 1)
    }

    fn release_publish_claim(&mut self, ask_id: &str) -> Result<bool, Self::Error> {
        let changed = self.connection.execute(
            "UPDATE turns SET publish_claimed = 0
             WHERE ask_id = ?1 AND state = 'open' AND publish_claimed = 1",
            [ask_id],
        )?;
        Ok(changed == 1)
    }

    fn cancel_queued_turn(&mut self, event_id: &EventId) -> Result<bool, Self::Error> {
        let Some(transition) = TurnTransition::parse(TurnState::Queued, TurnState::Cancelled)
        else {
            return Ok(false);
        };
        let changed = self.connection.execute(
            "UPDATE turns SET state = ?1
             WHERE event_id = ?2 AND state = ?3",
            params![
                transition.to_state().as_str(),
                event_id.as_str(),
                transition.from_state().as_str()
            ],
        )?;
        Ok(changed == 1)
    }

    fn cancel_unclaimed_turn(
        &mut self,
        event_id: &EventId,
    ) -> Result<Option<TurnRecord>, Self::Error> {
        let Some(queued_cancel) = TurnTransition::parse(TurnState::Queued, TurnState::Cancelled)
        else {
            return Ok(None);
        };
        let Some(open_cancel) = TurnTransition::parse(TurnState::Open, TurnState::Cancelled) else {
            return Ok(None);
        };
        let transaction = self.connection.transaction()?;
        let changed = transaction.execute(
            "UPDATE turns SET state = ?1, publish_claimed = 0
             WHERE event_id = ?2
               AND (
                   state = ?3
                   OR (state = ?4 AND publish_claimed = 0)
               )",
            params![
                queued_cancel.to_state().as_str(),
                event_id.as_str(),
                queued_cancel.from_state().as_str(),
                open_cancel.from_state().as_str()
            ],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(None);
        }
        let record = transaction.query_row(
            "SELECT t.sequence, s.bot_id, s.channel_id, t.event_id, t.ask_id,
                    t.reply_to_event_id, t.state
             FROM turns AS t
             JOIN sessions AS s ON s.id = t.session_id
             WHERE t.event_id = ?1 AND t.state = ?2
             ORDER BY t.sequence DESC
             LIMIT 1",
            params![event_id.as_str(), queued_cancel.to_state().as_str()],
            Self::read_turn,
        )?;
        transaction.commit()?;
        Ok(Some(record))
    }

    fn replace_unclaimed_turn(
        &mut self,
        turn: &NewTurn,
    ) -> Result<Option<TurnReplacement>, Self::Error> {
        let Some(queued_cancel) = TurnTransition::parse(TurnState::Queued, TurnState::Cancelled)
        else {
            return Ok(None);
        };
        let Some(open_cancel) = TurnTransition::parse(TurnState::Open, TurnState::Cancelled) else {
            return Ok(None);
        };
        let transaction = self.connection.transaction()?;
        let changed = transaction.execute(
            "UPDATE turns SET state = ?1, publish_claimed = 0
             WHERE event_id = ?2
               AND (
                   state = ?3
                   OR (state = ?4 AND publish_claimed = 0)
               )",
            params![
                queued_cancel.to_state().as_str(),
                turn.event_id.as_str(),
                queued_cancel.from_state().as_str(),
                open_cancel.from_state().as_str()
            ],
        )?;
        if changed == 0 {
            transaction.commit()?;
            return Ok(None);
        }
        let cancelled = transaction.query_row(
            "SELECT t.sequence, s.bot_id, s.channel_id, t.event_id, t.ask_id,
                    t.reply_to_event_id, t.state
             FROM turns AS t
             JOIN sessions AS s ON s.id = t.session_id
             WHERE t.event_id = ?1 AND t.state = ?2
             ORDER BY t.sequence DESC
             LIMIT 1",
            params![turn.event_id.as_str(), queued_cancel.to_state().as_str()],
            Self::read_turn,
        )?;
        let queued = insert_queued_turn(&transaction, turn)?;
        transaction.commit()?;
        Ok(Some(TurnReplacement { cancelled, queued }))
    }

    fn turn_by_ask_id(&self, ask_id: &str) -> Result<Option<TurnRecord>, Self::Error> {
        self.connection
            .query_row(
                "SELECT t.sequence, s.bot_id, s.channel_id, t.event_id, t.ask_id,
                        t.reply_to_event_id, t.state
                 FROM turns AS t
                 JOIN sessions AS s ON s.id = t.session_id
                 WHERE t.ask_id = ?1",
                [ask_id],
                Self::read_turn,
            )
            .optional()
    }

    fn active_turn_for_event(&self, event_id: &EventId) -> Result<Option<TurnRecord>, Self::Error> {
        self.connection
            .query_row(
                "SELECT t.sequence, s.bot_id, s.channel_id, t.event_id, t.ask_id,
                        t.reply_to_event_id, t.state
                 FROM turns AS t
                 JOIN sessions AS s ON s.id = t.session_id
                 WHERE t.event_id = ?1 AND t.state IN ('queued', 'open')",
                [event_id.as_str()],
                Self::read_turn,
            )
            .optional()
    }

    fn turns_for_session(
        &self,
        bot_id: &BotId,
        channel_id: &str,
    ) -> Result<Vec<TurnRecord>, Self::Error> {
        let mut statement = self.connection.prepare(
            "SELECT t.sequence, s.bot_id, s.channel_id, t.event_id, t.ask_id,
                    t.reply_to_event_id, t.state
             FROM turns AS t
             JOIN sessions AS s ON s.id = t.session_id
             WHERE s.bot_id = ?1 AND s.channel_id = ?2
             ORDER BY t.sequence",
        )?;
        let turns = statement
            .query_map(params![bot_id.as_str(), channel_id], Self::read_turn)?
            .collect();
        turns
    }

    fn sessions_with_pending_turns(&self) -> Result<Vec<SessionRecord>, Self::Error> {
        let mut statement = self.connection.prepare(
            "SELECT DISTINCT s.bot_id, s.channel_id, s.session_name,
                    s.occupant_logical_id, s.renew_id,
                    s.ask_context_event_id, s.ask_context_created_at
             FROM sessions AS s
             JOIN turns AS t ON t.session_id = s.id
             WHERE t.state IN ('queued', 'open')
             ORDER BY s.bot_id, s.channel_id",
        )?;
        let sessions = statement.query_map([], Self::read_session)?.collect();
        sessions
    }

    fn known_channel_ids(&self) -> Result<Vec<String>, Self::Error> {
        let mut statement = self
            .connection
            .prepare("SELECT DISTINCT channel_id FROM sessions ORDER BY channel_id")?;
        let ids = statement.query_map([], |row| row.get(0))?.collect();
        ids
    }

    fn active_event_ids(&self) -> Result<Vec<EventId>, Self::Error> {
        let mut statement = self.connection.prepare(
            "SELECT t.event_id FROM turns AS t
             WHERE t.state IN ('queued', 'open')
             ORDER BY t.sequence",
        )?;
        let ids = statement
            .query_map([], |row| {
                let event_id: String = row.get(0)?;
                parse_event_id(&event_id, 0)
            })?
            .collect();
        ids
    }

    fn save_outbound_attempt(&mut self, attempt: &OutboundAttempt) -> Result<(), Self::Error> {
        self.connection.execute(
            "INSERT INTO outbound_attempts(
                 ask_id, body, channel_id, reply_to_event_id, mention,
                 outbound_event_id, dispatched
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(ask_id) DO UPDATE SET
                 body = excluded.body,
                 channel_id = excluded.channel_id,
                 reply_to_event_id = excluded.reply_to_event_id,
                 mention = excluded.mention,
                 outbound_event_id = COALESCE(
                     outbound_attempts.outbound_event_id,
                     excluded.outbound_event_id
                 ),
                 dispatched = excluded.dispatched",
            params![
                attempt.ask_id,
                attempt.body,
                attempt.channel_id,
                attempt.reply_to_event_id.as_str(),
                attempt.mention,
                attempt.outbound_event_id,
                i64::from(attempt.dispatched)
            ],
        )?;
        Ok(())
    }

    fn outbound_attempt(&self, ask_id: &str) -> Result<Option<OutboundAttempt>, Self::Error> {
        self.connection
            .query_row(
                "SELECT ask_id, body, channel_id, reply_to_event_id, mention,
                        outbound_event_id, dispatched
                 FROM outbound_attempts WHERE ask_id = ?1",
                [ask_id],
                |row| {
                    let reply_to: String = row.get(3)?;
                    let dispatched: i64 = row.get(6)?;
                    Ok(OutboundAttempt {
                        ask_id: row.get(0)?,
                        body: row.get(1)?,
                        channel_id: row.get(2)?,
                        reply_to_event_id: parse_event_id(&reply_to, 3)?,
                        mention: row.get(4)?,
                        outbound_event_id: row.get(5)?,
                        dispatched: dispatched != 0,
                    })
                },
            )
            .optional()
    }

    fn mark_outbound_accepted(
        &mut self,
        ask_id: &str,
        event_id: &str,
    ) -> Result<bool, Self::Error> {
        let changed = self.connection.execute(
            "UPDATE outbound_attempts SET outbound_event_id = ?1
             WHERE ask_id = ?2
               AND (outbound_event_id IS NULL OR outbound_event_id = ?1)",
            params![event_id, ask_id],
        )?;
        Ok(changed == 1)
    }
}

fn insert_queued_turn(connection: &Connection, turn: &NewTurn) -> rusqlite::Result<TurnRecord> {
    let session_id: i64 = connection.query_row(
        "SELECT id FROM sessions WHERE bot_id = ?1 AND channel_id = ?2",
        params![turn.bot_id.as_str(), turn.channel_id],
        |row| row.get(0),
    )?;
    connection.execute(
        "INSERT INTO turns(session_id, event_id, reply_to_event_id, state)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            session_id,
            turn.event_id.as_str(),
            turn.reply_to_event_id.as_ref().map(EventId::as_str),
            TurnState::Queued.as_str(),
        ],
    )?;

    Ok(TurnRecord {
        sequence: connection.last_insert_rowid(),
        bot_id: turn.bot_id.clone(),
        channel_id: turn.channel_id.clone(),
        event_id: turn.event_id.clone(),
        ask_id: None,
        reply_to_event_id: turn.reply_to_event_id.clone(),
        state: TurnState::Queued,
    })
}

fn parse_bot_id(value: &str, column: usize) -> rusqlite::Result<BotId> {
    BotId::new(value).ok_or_else(|| invalid_value(column, "invalid bot id"))
}

fn parse_event_id(value: &str, column: usize) -> rusqlite::Result<EventId> {
    EventId::parse_hex(value).ok_or_else(|| invalid_value(column, "invalid event id"))
}

fn invalid_value(column: usize, message: &'static str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(column, rusqlite::types::Type::Text, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event_id(character: char) -> EventId {
        EventId::parse_hex(&character.to_string().repeat(64)).expect("event id")
    }

    fn session(bot_id: &BotId, channel_id: &str) -> SessionRecord {
        SessionRecord {
            bot_id: bot_id.clone(),
            channel_id: channel_id.to_owned(),
            session_name: format!("{bot_id}-channel"),
            occupant_logical_id: Some("logical-agent-id".to_owned()),
            renew_id: Some("renew-id".to_owned()),
            ask_context_event_id: None,
            ask_context_created_at: None,
        }
    }

    fn turn(bot_id: &BotId, channel_id: &str, value: char) -> NewTurn {
        NewTurn {
            bot_id: bot_id.clone(),
            channel_id: channel_id.to_owned(),
            event_id: event_id(value),
            reply_to_event_id: Some(event_id('f')),
        }
    }

    #[test]
    fn processed_events_are_idempotent() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let event_id = event_id('a');

        assert!(repository.mark_event_processed(&event_id).unwrap());
        assert!(!repository.mark_event_processed(&event_id).unwrap());
    }

    #[test]
    fn relay_events_are_indexed_atomically_and_idempotently() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        assert_eq!(repository.relay_replay_since().unwrap(), None);
        let event = IndexedRelayEvent {
            event_id: event_id('a'),
            author_pubkey: "b".repeat(64),
            created_at: 1_000,
            kind: 9,
            content: "hello".to_owned(),
            tags_json: r#"[["h","channel"]]"#.to_owned(),
            channel_id: Some("channel".to_owned()),
            target_event_id: None,
        };

        assert!(repository.index_event(&event, true).unwrap());
        assert!(!repository.index_event(&event, true).unwrap());
        assert!(!repository.event_processed(&event.event_id).unwrap());
        assert_eq!(
            repository.indexed_event(&event.event_id).unwrap(),
            Some(event.clone())
        );
        let mut newer_ordinary = event.clone();
        newer_ordinary.event_id = event_id('c');
        newer_ordinary.created_at = 2_000;
        assert!(repository.index_event(&newer_ordinary, false).unwrap());
        assert_eq!(repository.relay_replay_since().unwrap(), Some(1_000));

        let bot_id = BotId::new("bot").expect("bot");
        repository
            .save_session(&session(&bot_id, "channel"))
            .unwrap();
        assert!(repository
            .enqueue_unprocessed_turn(&NewTurn {
                bot_id,
                channel_id: "channel".to_owned(),
                event_id: event.event_id.clone(),
                reply_to_event_id: None,
            })
            .unwrap()
            .is_some());
        assert!(repository.event_processed(&event.event_id).unwrap());
        assert_eq!(repository.relay_replay_since().unwrap(), Some(1_100));

        let mut declined = event.clone();
        declined.event_id = event_id('d');
        declined.created_at = 1_500;
        assert!(repository.index_event(&declined, true).unwrap());
        assert_eq!(repository.relay_replay_since().unwrap(), Some(1_500));
        assert!(repository.mark_event_processed(&declined.event_id).unwrap());
        assert_eq!(repository.relay_replay_since().unwrap(), Some(1_100));

        assert_eq!(
            repository.indexed_events_for_channel("channel").unwrap(),
            vec![event, declined, newer_ordinary]
        );
        assert!(repository
            .indexed_events_for_channel("different-channel")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn sessions_are_keyed_by_bot_and_channel() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new("bot").expect("bot id");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        let mut expected = session(&bot_id, channel_id);
        repository.save_session(&expected).unwrap();

        expected.renew_id = Some("replacement-renew-id".to_owned());
        repository.save_session(&expected).unwrap();

        expected.ask_context_event_id = Some(event_id('c'));
        expected.ask_context_created_at = Some(42);
        repository.save_session(&expected).unwrap();

        assert_eq!(
            repository.session(&bot_id, channel_id).unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            repository.session_by_name(&expected.session_name).unwrap(),
            Some(expected)
        );
        assert_eq!(repository.session_by_name("missing").unwrap(), None);
    }

    #[test]
    fn turns_queue_in_order_and_state_changes_are_terminal() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new("bot").expect("bot id");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        repository
            .save_session(&session(&bot_id, channel_id))
            .unwrap();

        let first = repository
            .enqueue_unprocessed_turn(&turn(&bot_id, channel_id, 'a'))
            .unwrap();
        let first = first.expect("new event");
        let second = repository
            .enqueue_unprocessed_turn(&turn(&bot_id, channel_id, 'b'))
            .unwrap();
        let second = second.expect("new event");
        assert_eq!(first.state, TurnState::Queued);
        assert_eq!(second.state, TurnState::Queued);

        let opened = repository
            .open_next_turn(&bot_id, channel_id, "ask-1")
            .unwrap()
            .expect("first queued turn");
        assert_eq!(opened.sequence, first.sequence);
        assert_eq!(opened.ask_id.as_deref(), Some("ask-1"));
        assert!(repository
            .open_next_turn(&bot_id, channel_id, "ask-2")
            .unwrap()
            .is_none());
        assert!(repository
            .set_turn_state("ask-1", TurnState::Posted)
            .unwrap());
        assert!(!repository.set_turn_state("ask-1", TurnState::Open).unwrap());
        assert_eq!(
            repository
                .turn_by_ask_id("ask-1")
                .unwrap()
                .expect("turn")
                .state,
            TurnState::Posted
        );
        let opened = repository
            .open_next_turn(&bot_id, channel_id, "ask-2")
            .unwrap()
            .expect("second queued turn");
        assert_eq!(opened.sequence, second.sequence);

        let turns = repository.turns_for_session(&bot_id, channel_id).unwrap();
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].sequence, first.sequence);
        assert_eq!(turns[0].state, TurnState::Posted);
        assert_eq!(turns[1].state, TurnState::Open);
    }

    #[test]
    fn queued_turns_can_be_cancelled_and_replaced() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new("bot").expect("bot id");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        repository
            .save_session(&session(&bot_id, channel_id))
            .unwrap();
        let turn = turn(&bot_id, channel_id, 'a');
        repository.enqueue_turn(&turn).unwrap();
        assert_eq!(
            repository
                .active_turn_for_event(&turn.event_id)
                .unwrap()
                .expect("queued turn")
                .state,
            TurnState::Queued
        );

        assert!(repository.cancel_queued_turn(&turn.event_id).unwrap());
        let replacement = repository.enqueue_turn(&turn).unwrap();
        let turns = repository.turns_for_session(&bot_id, channel_id).unwrap();
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].state, TurnState::Cancelled);
        assert_eq!(turns[0].ask_id, None);
        assert_eq!(turns[1], replacement);
    }

    #[test]
    fn enqueue_and_processed_marker_are_atomic() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new("bot").expect("bot id");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        let turn = turn(&bot_id, channel_id, 'a');

        assert!(repository.enqueue_unprocessed_turn(&turn).is_err());
        repository
            .save_session(&session(&bot_id, channel_id))
            .unwrap();
        assert!(repository
            .enqueue_unprocessed_turn(&turn)
            .unwrap()
            .is_some());
        assert!(repository
            .enqueue_unprocessed_turn(&turn)
            .unwrap()
            .is_none());
    }

    #[test]
    fn pending_sessions_can_be_enumerated_for_recovery() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new("bot").expect("bot id");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        let expected = session(&bot_id, channel_id);
        repository.save_session(&expected).unwrap();
        repository
            .enqueue_turn(&turn(&bot_id, channel_id, 'a'))
            .unwrap();
        assert_eq!(
            repository.sessions_with_pending_turns().unwrap(),
            vec![expected.clone()]
        );
        repository
            .open_next_turn(&bot_id, channel_id, "ask-1")
            .unwrap();
        assert_eq!(
            repository.sessions_with_pending_turns().unwrap(),
            vec![expected.clone()]
        );
        assert_eq!(
            repository.known_channel_ids().unwrap(),
            vec![channel_id.to_owned()]
        );
        assert_eq!(repository.active_event_ids().unwrap().len(), 1);
        repository
            .set_turn_state("ask-1", TurnState::Posted)
            .unwrap();
        assert!(repository.sessions_with_pending_turns().unwrap().is_empty());
        assert_eq!(
            repository.known_channel_ids().unwrap(),
            vec![channel_id.to_owned()]
        );
        assert!(repository.active_event_ids().unwrap().is_empty());
    }

    #[test]
    fn claimed_open_turn_cannot_be_cancelled() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new("bot").expect("bot id");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        repository
            .save_session(&session(&bot_id, channel_id))
            .unwrap();
        let turn = turn(&bot_id, channel_id, 'a');
        repository.enqueue_turn(&turn).unwrap();
        repository
            .open_next_turn(&bot_id, channel_id, "ask-1")
            .unwrap();

        assert!(repository.claim_turn_for_publish("ask-1").unwrap());
        assert!(repository
            .cancel_unclaimed_turn(&turn.event_id)
            .unwrap()
            .is_none());
        assert!(repository.replace_unclaimed_turn(&turn).unwrap().is_none());
        assert!(!repository
            .set_turn_state("ask-1", TurnState::Cancelled)
            .unwrap());
        assert!(repository.release_publish_claim("ask-1").unwrap());
        assert_eq!(
            repository
                .cancel_unclaimed_turn(&turn.event_id)
                .unwrap()
                .expect("cancelled")
                .state,
            TurnState::Cancelled
        );
    }

    #[test]
    fn replace_unclaimed_turn_cancels_and_enqueues() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new("bot").expect("bot id");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        repository
            .save_session(&session(&bot_id, channel_id))
            .unwrap();
        let turn = turn(&bot_id, channel_id, 'a');
        repository.enqueue_turn(&turn).unwrap();
        repository
            .open_next_turn(&bot_id, channel_id, "ask-1")
            .unwrap();

        let replaced = repository
            .replace_unclaimed_turn(&turn)
            .unwrap()
            .expect("replaced");
        assert_eq!(replaced.cancelled.ask_id.as_deref(), Some("ask-1"));
        assert_eq!(replaced.cancelled.state, TurnState::Cancelled);
        assert_eq!(replaced.queued.state, TurnState::Queued);
        assert_eq!(replaced.queued.event_id, turn.event_id);
    }

    #[test]
    fn latest_body_prefers_a_later_edit() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let original = IndexedRelayEvent {
            event_id: event_id('a'),
            author_pubkey: "b".repeat(64),
            created_at: 1_000,
            kind: 9,
            content: "bot: original".to_owned(),
            tags_json: "[]".to_owned(),
            channel_id: Some("channel".to_owned()),
            target_event_id: None,
        };
        let edit = IndexedRelayEvent {
            event_id: event_id('c'),
            author_pubkey: "b".repeat(64),
            created_at: 2_000,
            kind: 40_003,
            content: "bot: replacement".to_owned(),
            tags_json: "[]".to_owned(),
            channel_id: Some("channel".to_owned()),
            target_event_id: Some(event_id('a')),
        };
        repository.index_event(&original, false).unwrap();
        repository.index_event(&edit, false).unwrap();
        assert_eq!(
            repository.latest_body_for_event(&event_id('a')).unwrap(),
            Some("bot: replacement".to_owned())
        );
        let foreign = IndexedRelayEvent {
            event_id: event_id('d'),
            author_pubkey: "9".repeat(64),
            created_at: 9_999,
            kind: 40_003,
            content: "bot: attacker body".to_owned(),
            tags_json: "[]".to_owned(),
            channel_id: Some("channel".to_owned()),
            target_event_id: Some(event_id('a')),
        };
        repository.index_event(&foreign, false).unwrap();
        assert_eq!(
            repository.latest_body_for_event(&event_id('a')).unwrap(),
            Some("bot: replacement".to_owned())
        );
    }

    #[test]
    fn existing_turns_table_gains_publish_claimed() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE sessions (
                     id INTEGER PRIMARY KEY,
                     bot_id TEXT NOT NULL,
                     channel_id TEXT NOT NULL,
                     session_name TEXT NOT NULL UNIQUE,
                     occupant_logical_id TEXT,
                     renew_id TEXT,
                     UNIQUE(bot_id, channel_id)
                 ) STRICT;
                 CREATE TABLE turns (
                     sequence INTEGER PRIMARY KEY,
                     session_id INTEGER NOT NULL REFERENCES sessions(id),
                     event_id TEXT NOT NULL,
                     ask_id TEXT UNIQUE,
                     reply_to_event_id TEXT,
                     state TEXT NOT NULL
                 ) STRICT;",
            )
            .unwrap();
        let mut repository = SqliteRepository::from_connection(connection).expect("migrated");
        let bot_id = BotId::new("bot").expect("bot id");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        repository
            .save_session(&session(&bot_id, channel_id))
            .unwrap();
        repository
            .enqueue_turn(&turn(&bot_id, channel_id, 'a'))
            .unwrap();
        repository
            .open_next_turn(&bot_id, channel_id, "ask-1")
            .unwrap();
        assert!(repository.claim_turn_for_publish("ask-1").unwrap());
    }

    #[test]
    fn sqlite_schema_has_no_nsec_columns() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new("bot").expect("bot id");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        repository
            .save_session(&session(&bot_id, channel_id))
            .unwrap();
        repository
            .enqueue_turn(&turn(&bot_id, channel_id, 'a'))
            .unwrap();
        repository
            .index_event(
                &IndexedRelayEvent {
                    event_id: event_id('a'),
                    author_pubkey: "b".repeat(64),
                    created_at: 1_000,
                    kind: 9,
                    content: "bot: hello".to_owned(),
                    tags_json: "[]".to_owned(),
                    channel_id: Some(channel_id.to_owned()),
                    target_event_id: None,
                },
                false,
            )
            .unwrap();

        let tables: Vec<String> = repository
            .connection
            .prepare(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        for table in tables {
            let mut info = repository
                .connection
                .prepare(&format!("PRAGMA table_info({table})"))
                .unwrap();
            let columns: Vec<String> = info
                .query_map([], |row| row.get(1))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            for column in columns {
                let lower = column.to_ascii_lowercase();
                assert!(
                    !lower.contains("nsec")
                        && !lower.contains("secret")
                        && !lower.contains("private"),
                    "{column}"
                );
            }
            let mut rows = repository
                .connection
                .prepare(&format!("SELECT * FROM {table}"))
                .unwrap();
            let width = rows.column_count();
            let mut query = rows.query([]).unwrap();
            while let Some(row) = query.next().unwrap() {
                for index in 0..width {
                    if let rusqlite::types::ValueRef::Text(text) = row.get_ref(index).unwrap() {
                        let value = String::from_utf8_lossy(text);
                        assert!(!value.contains("nsec1"), "{value}");
                    }
                }
            }
        }
    }
}
