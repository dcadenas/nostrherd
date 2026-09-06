//! SQLite host-state adapter.

use std::path::Path;
use std::time::Duration;

use botserver_domain::restraint::POST_CEILING_WINDOW_SECS;
use botserver_domain::{BotId, EventId, TurnTransition};
use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};

use crate::outbox::OutboundAttempt;
use crate::progress::ProgressPost;
use crate::watch::{WatchFire, WatchRecord};
use crate::{
    HostRepository, IndexedRelayEvent, NewTurn, SessionRecord, TurnRecord, TurnReplacement,
    TurnState,
};

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

fn migrate_outbound_reply_to_nullable(connection: &Connection) -> rusqlite::Result<()> {
    let notnull: Option<i64> = connection
        .query_row(
            "SELECT \"notnull\" FROM pragma_table_info('outbound_attempts')
             WHERE name = 'reply_to_event_id'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if notnull != Some(1) {
        return Ok(());
    }
    connection.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE outbound_attempts_new (
             ask_id TEXT PRIMARY KEY NOT NULL,
             body TEXT NOT NULL,
             channel_id TEXT NOT NULL,
             reply_to_event_id TEXT CHECK(
                 reply_to_event_id IS NULL OR length(reply_to_event_id) = 64
             ),
             mention TEXT NOT NULL,
             outbound_event_id TEXT CHECK(
                 outbound_event_id IS NULL OR length(outbound_event_id) = 64
             ),
             dispatched INTEGER NOT NULL DEFAULT 0 CHECK(dispatched IN (0, 1))
         ) STRICT;
         INSERT INTO outbound_attempts_new(
             ask_id, body, channel_id, reply_to_event_id, mention,
             outbound_event_id, dispatched
         )
         SELECT ask_id, body, channel_id, reply_to_event_id, mention,
                outbound_event_id, dispatched
         FROM outbound_attempts;
         DROP TABLE outbound_attempts;
         ALTER TABLE outbound_attempts_new RENAME TO outbound_attempts;
         COMMIT;",
    )
}

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

fn column_exists(connection: &Connection, table: &str, column: &str) -> rusqlite::Result<bool> {
    connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name = ?2)",
        params![table, column],
        |row| row.get(0),
    )
}

fn migrate_turn_wake_columns(connection: &Connection) -> rusqlite::Result<()> {
    if !column_exists(connection, "turns", "ask_body")? {
        connection.execute("ALTER TABLE turns ADD COLUMN ask_body TEXT", [])?;
    }
    if !column_exists(connection, "turns", "publish_reply_to_event_id")? {
        connection.execute(
            "ALTER TABLE turns ADD COLUMN publish_reply_to_event_id TEXT",
            [],
        )?;
        connection.execute("UPDATE turns SET publish_reply_to_event_id = event_id", [])?;
    }
    Ok(())
}

/// Column migrations and the nullable-reply rebuild, in order.
///
/// The prepared-id columns are added after the rebuild: a pre-#54 table
/// is recreated without them and gains them here (D43).
fn run_column_migrations(connection: &Connection) -> rusqlite::Result<()> {
    add_column_if_missing(
        connection,
        "ALTER TABLE turns ADD COLUMN publish_claimed INTEGER NOT NULL DEFAULT 0",
    )?;
    add_column_if_missing(
        connection,
        "ALTER TABLE outbound_attempts ADD COLUMN dispatched INTEGER NOT NULL DEFAULT 0",
    )?;
    add_column_if_missing(
        connection,
        "ALTER TABLE sessions ADD COLUMN ask_context_event_id TEXT",
    )?;
    add_column_if_missing(
        connection,
        "ALTER TABLE sessions ADD COLUMN ask_context_created_at INTEGER",
    )?;
    migrate_outbound_reply_to_nullable(connection)?;
    add_column_if_missing(
        connection,
        "ALTER TABLE outbound_attempts ADD COLUMN thread_root_event_id TEXT",
    )?;
    add_column_if_missing(
        connection,
        "ALTER TABLE outbound_attempts ADD COLUMN prepared_event_id TEXT",
    )?;
    add_column_if_missing(
        connection,
        "ALTER TABLE outbound_attempts ADD COLUMN prepared_created_at INTEGER",
    )?;
    add_column_if_missing(connection, "ALTER TABLE turns ADD COLUMN opened_at INTEGER")?;
    migrate_turn_wake_columns(connection)?;
    add_column_if_missing(
        connection,
        "ALTER TABLE watches ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0",
    )?;
    add_column_if_missing(
        connection,
        "ALTER TABLE progress_posts ADD COLUMN retry_noticed_at INTEGER",
    )?;
    add_column_if_missing(
        connection,
        "ALTER TABLE progress_posts ADD COLUMN delete_pending INTEGER NOT NULL DEFAULT 0",
    )?;
    connection.execute_batch(
        "DROP INDEX IF EXISTS progress_posts_pending_flush;
         CREATE INDEX progress_posts_pending_flush
             ON progress_posts(opened_at, ask_id)
             WHERE (ended = 0 AND (
                 pending_body IS NOT NULL
                 OR (prepared_event_id IS NOT NULL AND post_event_id IS NULL)
             )) OR delete_pending = 1;",
    )?;
    Ok(())
}

/// Progress post rows (D42), one per ask.
fn create_progress_posts_table(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        "              CREATE TABLE IF NOT EXISTS progress_posts (
              ask_id TEXT PRIMARY KEY NOT NULL,
              channel_id TEXT NOT NULL,
              reply_to_event_id TEXT NOT NULL CHECK(length(reply_to_event_id) = 64),
              thread_root_event_id TEXT CHECK(
                  thread_root_event_id IS NULL OR length(thread_root_event_id) = 64
              ),
              opened_at INTEGER NOT NULL,
              pending_body TEXT,
              post_body TEXT,
              prepared_event_id TEXT CHECK(
                  prepared_event_id IS NULL OR length(prepared_event_id) = 64
              ),
              prepared_created_at INTEGER,
               post_event_id TEXT CHECK(
                  post_event_id IS NULL OR length(post_event_id) = 64
              ),
              edit_count INTEGER NOT NULL DEFAULT 0 CHECK(edit_count >= 0),
              last_send_at INTEGER,
              ended INTEGER NOT NULL DEFAULT 0 CHECK(ended IN (0, 1)),
              cap_noticed INTEGER NOT NULL DEFAULT 0 CHECK(cap_noticed IN (0, 1)),
              retry_noticed_at INTEGER,
              delete_pending INTEGER NOT NULL DEFAULT 0 CHECK(delete_pending IN (0, 1))
          ) STRICT;

         CREATE INDEX IF NOT EXISTS progress_posts_channel
              ON progress_posts(channel_id);",
    )
}

/// Derive progress dispatch from the prepared id while preserving old rows.
fn migrate_progress_posts_without_dispatched(connection: &Connection) -> rusqlite::Result<()> {
    let has_dispatched: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM pragma_table_info('progress_posts') WHERE name = 'dispatched'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if has_dispatched.is_none() {
        return Ok(());
    }
    connection.execute_batch(
        "BEGIN IMMEDIATE;
         DROP INDEX IF EXISTS progress_posts_channel;
         DROP INDEX IF EXISTS progress_posts_pending_flush;
         CREATE TABLE progress_posts_new (
             ask_id TEXT PRIMARY KEY NOT NULL,
             channel_id TEXT NOT NULL,
             reply_to_event_id TEXT NOT NULL CHECK(length(reply_to_event_id) = 64),
             thread_root_event_id TEXT CHECK(
                 thread_root_event_id IS NULL OR length(thread_root_event_id) = 64
             ),
             opened_at INTEGER NOT NULL,
             pending_body TEXT,
             post_body TEXT,
             prepared_event_id TEXT CHECK(
                 prepared_event_id IS NULL OR length(prepared_event_id) = 64
             ),
             prepared_created_at INTEGER,
             post_event_id TEXT CHECK(
                 post_event_id IS NULL OR length(post_event_id) = 64
             ),
             edit_count INTEGER NOT NULL DEFAULT 0 CHECK(edit_count >= 0),
             last_send_at INTEGER,
              ended INTEGER NOT NULL DEFAULT 0 CHECK(ended IN (0, 1)),
              cap_noticed INTEGER NOT NULL DEFAULT 0 CHECK(cap_noticed IN (0, 1)),
              retry_noticed_at INTEGER,
              delete_pending INTEGER NOT NULL DEFAULT 0 CHECK(delete_pending IN (0, 1))
         ) STRICT;
         INSERT INTO progress_posts_new(
             ask_id, channel_id, reply_to_event_id, thread_root_event_id, opened_at,
             pending_body, post_body, prepared_event_id, prepared_created_at,
              post_event_id, edit_count, last_send_at, ended, cap_noticed,
              retry_noticed_at, delete_pending
         )
         SELECT ask_id, channel_id, reply_to_event_id, thread_root_event_id, opened_at,
                CASE
                    WHEN dispatched = 1 AND prepared_event_id IS NULL
                         AND post_event_id IS NULL THEN NULL
                    ELSE pending_body
                END,
                post_body, prepared_event_id, prepared_created_at, post_event_id,
                edit_count, last_send_at,
                CASE
                    WHEN dispatched = 1 AND prepared_event_id IS NULL
                         AND post_event_id IS NULL THEN 1
                    ELSE ended
                END,
                cap_noticed, NULL, 0
         FROM progress_posts;
         DROP TABLE progress_posts;
         ALTER TABLE progress_posts_new RENAME TO progress_posts;
         CREATE INDEX progress_posts_channel ON progress_posts(channel_id);
         COMMIT;",
    )
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
    #[allow(clippy::too_many_lines)]
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
                  opened_at INTEGER,
                  ask_body TEXT,
                  publish_reply_to_event_id TEXT CHECK(
                      publish_reply_to_event_id IS NULL
                      OR length(publish_reply_to_event_id) = 64
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
                   reply_to_event_id TEXT CHECK(
                       reply_to_event_id IS NULL OR length(reply_to_event_id) = 64
                   ),
                   thread_root_event_id TEXT CHECK(
                       thread_root_event_id IS NULL OR length(thread_root_event_id) = 64
                   ),
                   mention TEXT NOT NULL,
                   outbound_event_id TEXT CHECK(
                       outbound_event_id IS NULL OR length(outbound_event_id) = 64
                   ),
                   prepared_event_id TEXT CHECK(
                       prepared_event_id IS NULL OR length(prepared_event_id) = 64
                   ),
                   prepared_created_at INTEGER,
                   dispatched INTEGER NOT NULL DEFAULT 0 CHECK(dispatched IN (0, 1))
               ) STRICT;

              CREATE TABLE IF NOT EXISTS watches (
                  watch_id TEXT PRIMARY KEY NOT NULL CHECK(length(watch_id) = 64),
                  created_at INTEGER NOT NULL,
                  session_id INTEGER NOT NULL REFERENCES sessions(id),
                  predicate_channel_id TEXT,
                  predicate_kind INTEGER,
                  cooldown_secs INTEGER NOT NULL CHECK(cooldown_secs > 0),
                  expires_at INTEGER,
                  max_fires INTEGER CHECK(max_fires IS NULL OR max_fires > 0),
                  fire_count INTEGER NOT NULL DEFAULT 0 CHECK(fire_count >= 0),
                  last_fired_at INTEGER,
                  state TEXT NOT NULL DEFAULT 'active' CHECK(
                      state IN ('active', 'cancelled', 'completed')
                  )
              ) STRICT;

              CREATE TABLE IF NOT EXISTS watch_authors (
                  watch_id TEXT NOT NULL REFERENCES watches(watch_id),
                  author_pubkey TEXT NOT NULL CHECK(length(author_pubkey) = 64),
                  PRIMARY KEY(watch_id, author_pubkey)
              ) STRICT;

               CREATE TABLE IF NOT EXISTS watch_fires (
                   watch_id TEXT NOT NULL REFERENCES watches(watch_id),
                   source_event_id TEXT NOT NULL CHECK(length(source_event_id) = 64),
                   wake_event_id TEXT NOT NULL UNIQUE CHECK(length(wake_event_id) = 64),
                   fired_at INTEGER NOT NULL,
                   PRIMARY KEY(watch_id, source_event_id)
               ) STRICT;

               CREATE TABLE IF NOT EXISTS host_initiated_posts (
                   attempt_key TEXT PRIMARY KEY NOT NULL,
                   bot_id TEXT NOT NULL,
                   channel_id TEXT NOT NULL,
                   published_at INTEGER NOT NULL
               ) STRICT;

               CREATE INDEX IF NOT EXISTS host_initiated_posts_window
                   ON host_initiated_posts(bot_id, channel_id, published_at);

               CREATE INDEX IF NOT EXISTS watch_authors_active
                   ON watch_authors(author_pubkey, watch_id);",
        )?;
        create_progress_posts_table(&connection)?;
        migrate_progress_posts_without_dispatched(&connection)?;
        run_column_migrations(&connection)?;
        Ok(Self { connection })
    }

    #[cfg(test)]
    pub(crate) fn execute_batch_for_test(&self, sql: &str) {
        self.connection.execute_batch(sql).expect("test SQL");
    }

    fn read_turn(row: &rusqlite::Row<'_>) -> rusqlite::Result<TurnRecord> {
        read_turn_at(row, 0)
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

    fn session_by_occupant_logical_id(
        &self,
        occupant_logical_id: &str,
    ) -> Result<Option<SessionRecord>, Self::Error> {
        let mut statement = self.connection.prepare(
            "SELECT bot_id, channel_id, session_name, occupant_logical_id, renew_id,
                    ask_context_event_id, ask_context_created_at
             FROM sessions WHERE occupant_logical_id = ?1",
        )?;
        let mut sessions = statement
            .query_map([occupant_logical_id], Self::read_session)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok((sessions.len() == 1).then(|| sessions.remove(0)))
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
            "UPDATE turns SET ask_id = ?1, state = ?2,
                 opened_at = CAST(strftime('%s', 'now') AS INTEGER)
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
                    t.reply_to_event_id, t.state, t.opened_at, t.ask_body,
                    t.publish_reply_to_event_id
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
                    t.reply_to_event_id, t.state, t.opened_at, t.ask_body,
                    t.publish_reply_to_event_id
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
                    t.reply_to_event_id, t.state, t.opened_at, t.ask_body,
                    t.publish_reply_to_event_id
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
                        t.reply_to_event_id, t.state, t.opened_at, t.ask_body,
                        t.publish_reply_to_event_id
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
                        t.reply_to_event_id, t.state, t.opened_at, t.ask_body,
                        t.publish_reply_to_event_id
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
                    t.reply_to_event_id, t.state, t.opened_at, t.ask_body,
                    t.publish_reply_to_event_id
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
            "SELECT t.publish_reply_to_event_id FROM turns AS t
             WHERE t.state IN ('queued', 'open')
               AND t.publish_reply_to_event_id IS NOT NULL
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

    fn save_watch(&mut self, watch: &WatchRecord) -> Result<(), Self::Error> {
        let transaction = self.connection.transaction()?;
        let session_id: i64 = transaction.query_row(
            "SELECT id FROM sessions WHERE bot_id = ?1 AND channel_id = ?2",
            params![watch.bot_id.as_str(), watch.channel_id],
            |row| row.get(0),
        )?;
        transaction.execute(
            "INSERT INTO watches(
                 watch_id, created_at, session_id, predicate_channel_id, predicate_kind,
                 cooldown_secs, expires_at, max_fires
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(watch_id) DO NOTHING",
            params![
                watch.watch_id.as_str(),
                watch.created_at,
                session_id,
                watch.predicate_channel_id,
                watch.predicate_kind,
                watch.cooldown_secs,
                watch.expires_at,
                watch.max_fires,
            ],
        )?;
        for author in &watch.author_pubkeys {
            transaction.execute(
                "INSERT INTO watch_authors(watch_id, author_pubkey) VALUES (?1, ?2)
                 ON CONFLICT(watch_id, author_pubkey) DO NOTHING",
                params![watch.watch_id.as_str(), author],
            )?;
        }
        transaction.commit()
    }

    fn cancel_watches(
        &mut self,
        bot_id: &BotId,
        channel_id: &str,
        author_pubkey: &str,
        cancel_event_id: &EventId,
        cancel_created_at: i64,
    ) -> Result<usize, Self::Error> {
        self.connection.execute(
            "UPDATE watches SET state = 'cancelled'
             WHERE state = 'active'
               AND session_id = (
                   SELECT id FROM sessions WHERE bot_id = ?1 AND channel_id = ?2
               )
               AND EXISTS (
                   SELECT 1 FROM watch_authors AS a
                   WHERE a.watch_id = watches.watch_id AND a.author_pubkey = ?3
               )
               AND (created_at < ?4 OR (created_at = ?4 AND watch_id < ?5))",
            params![
                bot_id.as_str(),
                channel_id,
                author_pubkey,
                cancel_created_at,
                cancel_event_id.as_str()
            ],
        )
    }

    fn watched_author_pubkeys(&self, now: i64) -> Result<Vec<String>, Self::Error> {
        let mut statement = self.connection.prepare(
            "SELECT DISTINCT a.author_pubkey
             FROM watch_authors AS a
             JOIN watches AS w ON w.watch_id = a.watch_id
             WHERE w.state = 'active'
               AND (w.expires_at IS NULL OR w.expires_at > ?1)
               AND (w.max_fires IS NULL OR w.fire_count < w.max_fires)
             ORDER BY a.author_pubkey",
        )?;
        let authors = statement.query_map([now], |row| row.get(0))?.collect();
        authors
    }

    fn record_matching_watch_fires(
        &mut self,
        event: &IndexedRelayEvent,
        now: i64,
    ) -> Result<Vec<WatchFire>, Self::Error> {
        let transaction = self.connection.transaction()?;
        let mut statement = transaction.prepare(
            "SELECT w.watch_id, s.bot_id, s.channel_id
             FROM watches AS w
             JOIN sessions AS s ON s.id = w.session_id
             JOIN watch_authors AS a ON a.watch_id = w.watch_id
             WHERE w.state = 'active'
               AND a.author_pubkey = ?1
               AND (w.predicate_channel_id IS NULL OR w.predicate_channel_id = ?2)
               AND (w.predicate_kind IS NULL OR w.predicate_kind = ?3)
               AND (
                   ?5 > w.created_at OR (?5 = w.created_at AND ?6 > w.watch_id)
               )
               AND (w.expires_at IS NULL OR w.expires_at > ?4)
               AND (w.max_fires IS NULL OR w.fire_count < w.max_fires)
               AND (w.last_fired_at IS NULL OR w.last_fired_at + w.cooldown_secs <= ?4)
             ORDER BY w.watch_id",
        )?;
        let candidates = statement
            .query_map(
                params![
                    event.author_pubkey,
                    event.channel_id,
                    event.kind,
                    now,
                    event.created_at,
                    event.event_id.as_str()
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        let mut fires = Vec::new();
        for (watch_id_raw, bot_id_raw, channel_id) in candidates {
            let wake_event_id = wake_event_id(&watch_id_raw, &event.event_id);
            let changed = transaction.execute(
                "INSERT INTO watch_fires(watch_id, source_event_id, wake_event_id, fired_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(watch_id, source_event_id) DO NOTHING",
                params![
                    watch_id_raw,
                    event.event_id.as_str(),
                    wake_event_id.as_str(),
                    now
                ],
            )?;
            if changed == 0 {
                continue;
            }
            transaction.execute(
                "UPDATE watches SET
                     fire_count = fire_count + 1,
                     last_fired_at = ?2,
                     state = CASE
                         WHEN max_fires IS NOT NULL AND fire_count + 1 >= max_fires
                         THEN 'completed' ELSE state END
                 WHERE watch_id = ?1",
                params![watch_id_raw, now],
            )?;
            fires.push(WatchFire {
                watch_id: parse_event_id(&watch_id_raw, 0)?,
                wake_event_id,
                source_event_id: event.event_id.clone(),
                bot_id: parse_bot_id(&bot_id_raw, 1)?,
                channel_id,
                author_pubkey: event.author_pubkey.clone(),
                source_channel_id: event.channel_id.clone(),
                source_kind: event.kind,
            });
        }
        transaction.commit()?;
        Ok(fires)
    }

    fn save_outbound_attempt(&mut self, attempt: &OutboundAttempt) -> Result<(), Self::Error> {
        self.connection.execute(
            "INSERT INTO outbound_attempts(
                 ask_id, body, channel_id, reply_to_event_id, thread_root_event_id,
                 mention, outbound_event_id, prepared_event_id, prepared_created_at,
                 dispatched
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(ask_id) DO UPDATE SET
                 body = excluded.body,
                 channel_id = excluded.channel_id,
                 reply_to_event_id = excluded.reply_to_event_id,
                 thread_root_event_id = COALESCE(
                     outbound_attempts.thread_root_event_id,
                     excluded.thread_root_event_id
                 ),
                 mention = excluded.mention,
                 outbound_event_id = COALESCE(
                     outbound_attempts.outbound_event_id,
                     excluded.outbound_event_id
                 ),
                 prepared_event_id = COALESCE(
                     outbound_attempts.prepared_event_id,
                     excluded.prepared_event_id
                 ),
                 prepared_created_at = COALESCE(
                     outbound_attempts.prepared_created_at,
                     excluded.prepared_created_at
                 ),
                 dispatched = excluded.dispatched",
            params![
                attempt.ask_id,
                attempt.body,
                attempt.channel_id,
                attempt.reply_to_event_id.as_ref().map(EventId::as_str),
                attempt.thread_root_event_id.as_ref().map(EventId::as_str),
                attempt.mention,
                attempt.outbound_event_id,
                attempt.prepared_event_id,
                attempt.prepared_created_at,
                i64::from(attempt.dispatched)
            ],
        )?;
        Ok(())
    }

    fn outbound_attempt(&self, ask_id: &str) -> Result<Option<OutboundAttempt>, Self::Error> {
        self.connection
            .query_row(
                "SELECT ask_id, body, channel_id, reply_to_event_id, thread_root_event_id,
                        mention, outbound_event_id, prepared_event_id, prepared_created_at,
                        dispatched
                 FROM outbound_attempts WHERE ask_id = ?1",
                [ask_id],
                |row| {
                    let reply_to: Option<String> = row.get(3)?;
                    let thread_root: Option<String> = row.get(4)?;
                    let dispatched: i64 = row.get(9)?;
                    Ok(OutboundAttempt {
                        ask_id: row.get(0)?,
                        body: row.get(1)?,
                        channel_id: row.get(2)?,
                        reply_to_event_id: reply_to
                            .as_deref()
                            .map(|value| parse_event_id(value, 3))
                            .transpose()?,
                        thread_root_event_id: thread_root
                            .as_deref()
                            .map(|value| parse_event_id(value, 4))
                            .transpose()?,
                        mention: row.get(5)?,
                        outbound_event_id: row.get(6)?,
                        prepared_event_id: row.get(7)?,
                        prepared_created_at: row.get(8)?,
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

    fn count_host_initiated_posts(
        &self,
        bot_id: &BotId,
        channel_id: &str,
        since_unix: i64,
    ) -> Result<u32, Self::Error> {
        let count: i64 = self.connection.query_row(
            "SELECT count(*) FROM host_initiated_posts
             WHERE bot_id = ?1 AND channel_id = ?2 AND published_at > ?3",
            params![bot_id.as_str(), channel_id, since_unix],
            |row| row.get(0),
        )?;
        Ok(u32::try_from(count).unwrap_or(u32::MAX))
    }

    fn note_host_initiated_post(
        &mut self,
        attempt_key: &str,
        bot_id: &BotId,
        channel_id: &str,
        published_at: i64,
    ) -> Result<(), Self::Error> {
        let prune_before = published_at
            .checked_sub(POST_CEILING_WINDOW_SECS)
            .unwrap_or(i64::MIN);
        self.connection.execute(
            "INSERT OR IGNORE INTO host_initiated_posts
                 (attempt_key, bot_id, channel_id, published_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![attempt_key, bot_id.as_str(), channel_id, published_at],
        )?;
        self.connection.execute(
            "DELETE FROM host_initiated_posts WHERE published_at <= ?1",
            params![prune_before],
        )?;
        Ok(())
    }

    fn progress_post(&self, ask_id: &str) -> Result<Option<ProgressPost>, Self::Error> {
        self.connection
            .query_row(
                &format!("{PROGRESS_SELECT} WHERE ask_id = ?1"),
                [ask_id],
                read_progress_post,
            )
            .optional()
    }

    fn save_progress_post(&mut self, post: &ProgressPost) -> Result<(), Self::Error> {
        self.connection.execute(
            "INSERT INTO progress_posts(
                 ask_id, channel_id, reply_to_event_id, thread_root_event_id, opened_at,
                 pending_body, post_body, prepared_event_id, prepared_created_at,
                 post_event_id, edit_count, last_send_at, ended, cap_noticed,
                 retry_noticed_at, delete_pending
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
             ON CONFLICT(ask_id) DO UPDATE SET
                 pending_body = excluded.pending_body,
                 post_body = COALESCE(progress_posts.post_body, excluded.post_body),
                 prepared_event_id = COALESCE(
                     progress_posts.prepared_event_id, excluded.prepared_event_id
                 ),
                 prepared_created_at = COALESCE(
                     progress_posts.prepared_created_at, excluded.prepared_created_at
                 ),
                  post_event_id = COALESCE(progress_posts.post_event_id, excluded.post_event_id),
                 edit_count = excluded.edit_count,
                 last_send_at = excluded.last_send_at,
                  ended = excluded.ended,
                  cap_noticed = excluded.cap_noticed,
                  retry_noticed_at = excluded.retry_noticed_at,
                  delete_pending = excluded.delete_pending",
            params![
                post.ask_id,
                post.channel_id,
                post.reply_to_event_id.as_str(),
                post.thread_root_event_id.as_ref().map(EventId::as_str),
                post.opened_at,
                post.pending_body,
                post.post_body,
                post.prepared_event_id,
                post.prepared_created_at,
                post.post_event_id,
                i64::from(post.edit_count),
                post.last_send_at,
                i64::from(post.ended),
                i64::from(post.cap_noticed),
                post.retry_noticed_at,
                i64::from(post.delete_pending),
            ],
        )?;
        Ok(())
    }

    fn progress_posts_pending_flush(
        &self,
        bot_id: &BotId,
    ) -> Result<Vec<(ProgressPost, TurnRecord)>, Self::Error> {
        let mut statement = self.connection.prepare(PROGRESS_PENDING_SELECT)?;
        let posts = statement
            .query_map([bot_id.as_str()], |row| {
                Ok((read_progress_post(row)?, read_turn_at(row, 16)?))
            })?
            .collect();
        posts
    }

    fn progress_post_event_ids(&self, channel_id: &str) -> Result<Vec<EventId>, Self::Error> {
        let mut statement = self.connection.prepare(
            "SELECT COALESCE(post_event_id, prepared_event_id)
             FROM progress_posts
             WHERE channel_id = ?1
               AND (post_event_id IS NOT NULL OR prepared_event_id IS NOT NULL)
             ORDER BY ask_id",
        )?;
        let ids = statement
            .query_map([channel_id], |row| {
                let event_id: String = row.get(0)?;
                parse_event_id(&event_id, 0)
            })?
            .collect();
        ids
    }
}

const PROGRESS_SELECT: &str = "SELECT ask_id, channel_id, reply_to_event_id, thread_root_event_id,
        opened_at, pending_body, post_body, prepared_event_id, prepared_created_at,
        post_event_id, edit_count, last_send_at, ended, cap_noticed,
        retry_noticed_at, delete_pending
 FROM progress_posts";

const PROGRESS_PENDING_SELECT: &str = "WITH pending AS MATERIALIZED (
         SELECT *
         FROM progress_posts
          WHERE (ended = 0
            AND (
                pending_body IS NOT NULL
                OR (prepared_event_id IS NOT NULL AND post_event_id IS NULL)
            )) OR delete_pending = 1
         ORDER BY opened_at, ask_id
     )
     SELECT p.ask_id, p.channel_id, p.reply_to_event_id, p.thread_root_event_id,
            p.opened_at, p.pending_body, p.post_body, p.prepared_event_id,
             p.prepared_created_at, p.post_event_id, p.edit_count,
             p.last_send_at, p.ended, p.cap_noticed,
             p.retry_noticed_at, p.delete_pending,
              t.sequence, s.bot_id, s.channel_id, t.event_id, t.ask_id,
             t.reply_to_event_id, t.state, t.opened_at, t.ask_body,
             t.publish_reply_to_event_id
     FROM pending AS p
     CROSS JOIN turns AS t ON t.ask_id = p.ask_id
     JOIN sessions AS s ON s.id = t.session_id
     WHERE s.bot_id = ?1
     ORDER BY p.opened_at, p.ask_id";

fn read_progress_post(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProgressPost> {
    let reply_to: String = row.get(2)?;
    let thread_root: Option<String> = row.get(3)?;
    let edit_count: i64 = row.get(10)?;
    let ended: i64 = row.get(12)?;
    let cap_noticed: i64 = row.get(13)?;
    Ok(ProgressPost {
        ask_id: row.get(0)?,
        channel_id: row.get(1)?,
        reply_to_event_id: parse_event_id(&reply_to, 2)?,
        thread_root_event_id: thread_root
            .as_deref()
            .map(|value| parse_event_id(value, 3))
            .transpose()?,
        opened_at: row.get(4)?,
        pending_body: row.get(5)?,
        post_body: row.get(6)?,
        prepared_event_id: row.get(7)?,
        prepared_created_at: row.get(8)?,
        post_event_id: row.get(9)?,
        edit_count: u32::try_from(edit_count)
            .map_err(|_| invalid_value(10, "invalid progress edit count"))?,
        last_send_at: row.get(11)?,
        ended: ended != 0,
        cap_noticed: cap_noticed != 0,
        retry_noticed_at: row.get(14)?,
        delete_pending: row.get::<_, i64>(15)? != 0,
    })
}

fn read_turn_at(row: &rusqlite::Row<'_>, offset: usize) -> rusqlite::Result<TurnRecord> {
    let bot_id: String = row.get(offset + 1)?;
    let event_id: String = row.get(offset + 3)?;
    let reply_to_event_id: Option<String> = row.get(offset + 5)?;
    let state: String = row.get(offset + 6)?;
    let publish_reply_to_event_id: Option<String> = row.get(offset + 9)?;
    Ok(TurnRecord {
        sequence: row.get(offset)?,
        bot_id: parse_bot_id(&bot_id, offset + 1)?,
        channel_id: row.get(offset + 2)?,
        event_id: parse_event_id(&event_id, offset + 3)?,
        ask_id: row.get(offset + 4)?,
        reply_to_event_id: reply_to_event_id
            .as_deref()
            .map(|value| parse_event_id(value, offset + 5))
            .transpose()?,
        state: TurnState::parse(&state)
            .ok_or_else(|| invalid_value(offset + 6, "invalid turn state"))?,
        opened_at: row.get(offset + 7)?,
        ask_body: row.get(offset + 8)?,
        publish_reply_to_event_id: publish_reply_to_event_id
            .as_deref()
            .map(|value| parse_event_id(value, offset + 9))
            .transpose()?,
    })
}

fn insert_queued_turn(connection: &Connection, turn: &NewTurn) -> rusqlite::Result<TurnRecord> {
    let session_id: i64 = connection.query_row(
        "SELECT id FROM sessions WHERE bot_id = ?1 AND channel_id = ?2",
        params![turn.bot_id.as_str(), turn.channel_id],
        |row| row.get(0),
    )?;
    connection.execute(
        "INSERT INTO turns(
             session_id, event_id, reply_to_event_id, state, ask_body,
             publish_reply_to_event_id
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            session_id,
            turn.event_id.as_str(),
            turn.reply_to_event_id.as_ref().map(EventId::as_str),
            TurnState::Queued.as_str(),
            turn.ask_body,
            turn.publish_reply_to_event_id.as_ref().map(EventId::as_str),
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
        opened_at: None,
        ask_body: turn.ask_body.clone(),
        publish_reply_to_event_id: turn.publish_reply_to_event_id.clone(),
    })
}

fn wake_event_id(watch_id: &str, source_event_id: &EventId) -> EventId {
    let digest = Sha256::digest(format!("watch:{watch_id}:{}", source_event_id.as_str()));
    EventId::parse_hex(&format!("{digest:x}")).expect("sha256 is 32 bytes")
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
            ask_body: None,
            publish_reply_to_event_id: Some(event_id(value)),
        }
    }

    fn pending_progress(turn: &TurnRecord) -> ProgressPost {
        ProgressPost {
            ask_id: turn.ask_id.clone().expect("open ask"),
            channel_id: turn.channel_id.clone(),
            reply_to_event_id: turn.event_id.clone(),
            thread_root_event_id: None,
            opened_at: turn.opened_at.unwrap_or(1_000),
            pending_body: Some("working".to_owned()),
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
        }
    }

    #[test]
    fn pending_progress_query_returns_only_the_requested_bot_and_its_turn() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot = BotId::new("bot").unwrap();
        let other = BotId::new("other").unwrap();
        for (bot_id, channel_id, value, ask_id) in [
            (&bot, "channel-bot", 'a', "ask-bot"),
            (&other, "channel-other", 'b', "ask-other"),
        ] {
            repository
                .save_session(&session(bot_id, channel_id))
                .unwrap();
            repository
                .enqueue_turn(&turn(bot_id, channel_id, value))
                .unwrap();
            let turn = repository
                .open_next_turn(bot_id, channel_id, ask_id)
                .unwrap()
                .unwrap();
            repository
                .save_progress_post(&pending_progress(&turn))
                .unwrap();
        }

        let rows = repository.progress_posts_pending_flush(&bot).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.ask_id, "ask-bot");
        assert_eq!(rows[0].1.bot_id, bot);
    }

    #[test]
    fn pending_progress_index_is_migrated_and_used() {
        let repository = SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
            .expect("repository");
        repository
            .connection
            .execute("DROP INDEX progress_posts_pending_flush", [])
            .unwrap();
        let repository = SqliteRepository::from_connection(repository.connection).unwrap();
        let plan_sql = format!("EXPLAIN QUERY PLAN {PROGRESS_PENDING_SELECT}");
        let details = repository
            .connection
            .prepare(&plan_sql)
            .unwrap()
            .query_map(["bot"], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("progress_posts_pending_flush")),
            "query plan did not use pending index: {details:?}"
        );
        assert!(
            details
                .iter()
                .all(|detail| !detail.contains("turns_session_order")),
            "query plan scanned the bot's turn history: {details:?}"
        );
    }

    #[test]
    fn progress_dispatched_column_is_removed_without_retrying_an_unknown_legacy_send() {
        let repository = SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
            .expect("repository");
        repository
            .connection
            .execute_batch(
                "DROP TABLE progress_posts;
                 CREATE TABLE progress_posts (
                     ask_id TEXT PRIMARY KEY NOT NULL,
                     channel_id TEXT NOT NULL,
                     reply_to_event_id TEXT NOT NULL,
                     thread_root_event_id TEXT,
                     opened_at INTEGER NOT NULL,
                     pending_body TEXT,
                     post_body TEXT,
                     prepared_event_id TEXT,
                     prepared_created_at INTEGER,
                     dispatched INTEGER NOT NULL,
                     post_event_id TEXT,
                     edit_count INTEGER NOT NULL,
                     last_send_at INTEGER,
                     ended INTEGER NOT NULL,
                     cap_noticed INTEGER NOT NULL
                 ) STRICT;
                 INSERT INTO progress_posts VALUES (
                     'ask-legacy', 'channel',
                     'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                     NULL, 1000, 'next', '[bot]: previous', NULL, NULL, 1,
                     NULL, 0, NULL, 0, 0
                 );",
            )
            .unwrap();

        let repository = SqliteRepository::from_connection(repository.connection).unwrap();
        let columns = repository
            .connection
            .prepare("SELECT name FROM pragma_table_info('progress_posts') ORDER BY cid")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(!columns.iter().any(|column| column == "dispatched"));
        let legacy = repository.progress_post("ask-legacy").unwrap().unwrap();
        assert!(legacy.ended);
        assert!(legacy.pending_body.is_none());
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
                ask_body: None,
                publish_reply_to_event_id: Some(event.event_id.clone()),
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
            Some(expected.clone())
        );
        assert_eq!(
            repository
                .session_by_occupant_logical_id("logical-agent-id")
                .unwrap(),
            Some(expected)
        );
        assert_eq!(repository.session_by_name("missing").unwrap(), None);
        assert_eq!(
            repository
                .session_by_occupant_logical_id("missing")
                .unwrap(),
            None
        );
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
                  ) STRICT;
                  INSERT INTO sessions(
                      id, bot_id, channel_id, session_name,
                      occupant_logical_id, renew_id
                  ) VALUES (
                      1, 'bot', 'ab12cd34-5678-90ab-cdef-0123456789ab',
                      'bot-channel', 'logical-agent-id', 'renew-id'
                  );
                  INSERT INTO turns(
                      sequence, session_id, event_id, ask_id,
                      reply_to_event_id, state
                  ) VALUES (
                      1, 1,
                      'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                      NULL,
                      'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
                      'queued'
                  );",
            )
            .unwrap();
        let mut repository = SqliteRepository::from_connection(connection).expect("migrated");
        let bot_id = BotId::new("bot").expect("bot id");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        repository
            .open_next_turn(&bot_id, channel_id, "ask-1")
            .unwrap();
        assert!(repository.claim_turn_for_publish("ask-1").unwrap());
        assert_eq!(
            repository
                .turn_by_ask_id("ask-1")
                .unwrap()
                .expect("turn")
                .publish_reply_to_event_id,
            Some(event_id('a'))
        );
    }

    #[test]
    fn repeated_migration_keeps_host_wake_publish_target_empty() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new("bot").expect("bot id");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        repository
            .save_session(&session(&bot_id, channel_id))
            .unwrap();
        repository
            .enqueue_turn(&NewTurn {
                bot_id: bot_id.clone(),
                channel_id: channel_id.to_owned(),
                event_id: event_id('a'),
                reply_to_event_id: Some(event_id('b')),
                ask_body: Some("## Watch event".to_owned()),
                publish_reply_to_event_id: None,
            })
            .unwrap();

        run_column_migrations(&repository.connection).unwrap();

        assert!(
            repository.turns_for_session(&bot_id, channel_id).unwrap()[0]
                .publish_reply_to_event_id
                .is_none()
        );
    }

    #[test]
    fn outbound_attempts_nullable_reply_to_keeps_existing_rows() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE outbound_attempts (
                     ask_id TEXT PRIMARY KEY NOT NULL,
                     body TEXT NOT NULL,
                     channel_id TEXT NOT NULL,
                     reply_to_event_id TEXT NOT NULL CHECK(length(reply_to_event_id) = 64),
                     mention TEXT NOT NULL,
                     outbound_event_id TEXT,
                     dispatched INTEGER NOT NULL DEFAULT 1
                 ) STRICT;
                 INSERT INTO outbound_attempts(
                     ask_id, body, channel_id, reply_to_event_id, mention,
                     outbound_event_id, dispatched
                 ) VALUES (
                     'ask-keep',
                     'hello',
                     'ab12cd34-5678-90ab-cdef-0123456789ab',
                     'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                     'mention',
                     'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                     1
                 );",
            )
            .unwrap();
        let repository = SqliteRepository::from_connection(connection).expect("migrated");
        let attempt = repository
            .outbound_attempt("ask-keep")
            .unwrap()
            .expect("kept");
        assert_eq!(attempt.body, "hello");
        assert_eq!(
            attempt.outbound_event_id.as_deref(),
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
        assert!(attempt.dispatched);
        let notnull: i64 = repository
            .connection
            .query_row(
                "SELECT \"notnull\" FROM pragma_table_info('outbound_attempts')
                 WHERE name = 'reply_to_event_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(notnull, 0);
    }

    #[test]
    fn watched_author_fires_once_per_cooldown_and_unwatched_author_does_not() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new("bot").expect("bot id");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        repository
            .save_session(&session(&bot_id, channel_id))
            .unwrap();
        repository
            .save_watch(&WatchRecord {
                watch_id: event_id('a'),
                created_at: 10,
                bot_id,
                channel_id: channel_id.to_owned(),
                author_pubkeys: vec!["b".repeat(64)],
                predicate_channel_id: None,
                predicate_kind: None,
                cooldown_secs: 1_800,
                expires_at: None,
                max_fires: Some(3),
            })
            .unwrap();
        let event = |id, author: String, created_at| IndexedRelayEvent {
            event_id: event_id(id),
            author_pubkey: author,
            created_at,
            kind: 9,
            content: "activity".to_owned(),
            tags_json: "[]".to_owned(),
            channel_id: Some("elsewhere".to_owned()),
            target_event_id: None,
        };

        assert_eq!(
            repository
                .record_matching_watch_fires(&event('c', "c".repeat(64), 11), 1_000)
                .unwrap(),
            []
        );
        assert_eq!(
            repository
                .record_matching_watch_fires(&event('c', "b".repeat(64), 9), 1_000)
                .unwrap(),
            []
        );
        let first = repository
            .record_matching_watch_fires(&event('d', "b".repeat(64), 11), 1_000)
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].channel_id, channel_id);
        assert_eq!(
            repository
                .record_matching_watch_fires(&event('e', "b".repeat(64), 12), 1_100)
                .unwrap(),
            []
        );
        assert_eq!(
            repository
                .record_matching_watch_fires(&event('d', "b".repeat(64), 11), 2_800)
                .unwrap(),
            []
        );
        assert_eq!(
            repository
                .record_matching_watch_fires(&event('f', "b".repeat(64), 13), 2_800)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn watch_scope_lifetime_and_cancel_control_author_subscription() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot_id = BotId::new("bot").expect("bot id");
        let channel_id = "ab12cd34-5678-90ab-cdef-0123456789ab";
        let author = "b".repeat(64);
        repository
            .save_session(&session(&bot_id, channel_id))
            .unwrap();
        repository
            .save_watch(&WatchRecord {
                watch_id: event_id('a'),
                created_at: 0,
                bot_id: bot_id.clone(),
                channel_id: channel_id.to_owned(),
                author_pubkeys: vec![author.clone()],
                predicate_channel_id: Some(channel_id.to_owned()),
                predicate_kind: Some(40_002),
                cooldown_secs: 60,
                expires_at: Some(2_000),
                max_fires: None,
            })
            .unwrap();
        assert_eq!(
            repository.watched_author_pubkeys(1_999).unwrap(),
            std::slice::from_ref(&author)
        );
        assert!(repository.watched_author_pubkeys(2_000).unwrap().is_empty());
        assert_eq!(
            repository
                .cancel_watches(&bot_id, channel_id, &author, &event_id('b'), 1)
                .unwrap(),
            1
        );
        assert!(repository.watched_author_pubkeys(1_000).unwrap().is_empty());

        repository
            .save_watch(&WatchRecord {
                watch_id: event_id('c'),
                created_at: 2,
                bot_id: bot_id.clone(),
                channel_id: channel_id.to_owned(),
                author_pubkeys: vec![author.clone()],
                predicate_channel_id: None,
                predicate_kind: None,
                cooldown_secs: 60,
                expires_at: None,
                max_fires: Some(1),
            })
            .unwrap();
        assert_eq!(
            repository
                .cancel_watches(&bot_id, channel_id, &author, &event_id('b'), 1)
                .unwrap(),
            0
        );
        assert_eq!(
            repository.watched_author_pubkeys(1_000).unwrap(),
            std::slice::from_ref(&author)
        );
    }

    #[test]
    fn host_initiated_post_ledger_is_per_channel_idempotent_and_rolling() {
        let mut repository =
            SqliteRepository::from_connection(Connection::open_in_memory().unwrap())
                .expect("repository");
        let bot = BotId::new("bot").expect("bot");
        let other = BotId::new("pr").expect("pr");
        let home = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        let eng = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff";
        let now = 2_000_000_000;
        repository
            .note_host_initiated_post("tell-1", &bot, home, now)
            .unwrap();
        repository
            .note_host_initiated_post("tell-1", &bot, home, now)
            .unwrap();
        repository
            .note_host_initiated_post("tell-2", &bot, eng, now)
            .unwrap();
        repository
            .note_host_initiated_post("tell-3", &other, home, now)
            .unwrap();
        repository
            .note_host_initiated_post("old", &bot, home, now - POST_CEILING_WINDOW_SECS)
            .unwrap();
        let since = now - POST_CEILING_WINDOW_SECS;
        assert_eq!(
            repository
                .count_host_initiated_posts(&bot, home, since)
                .unwrap(),
            1
        );
        assert_eq!(
            repository
                .count_host_initiated_posts(&bot, eng, since)
                .unwrap(),
            1
        );
        assert_eq!(
            repository
                .count_host_initiated_posts(&other, home, since)
                .unwrap(),
            1
        );
        repository
            .note_host_initiated_post("fresh", &bot, home, now)
            .unwrap();
        assert_eq!(
            repository
                .count_host_initiated_posts(&bot, home, i64::MIN)
                .unwrap(),
            2
        );
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
