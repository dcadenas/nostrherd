//! SQLite host-state adapter.

use std::path::Path;

use botserver_domain::{BotId, EventId};
use rusqlite::{params, Connection, OptionalExtension};

use crate::{HostRepository, NewTurn, SessionRecord, TurnRecord, TurnState};

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
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;

             CREATE TABLE IF NOT EXISTS processed_events (
                 event_id TEXT PRIMARY KEY NOT NULL
                     CHECK(length(event_id) = 64)
             ) STRICT;

             CREATE TABLE IF NOT EXISTS sessions (
                 id INTEGER PRIMARY KEY,
                 bot_id TEXT NOT NULL,
                 channel_id TEXT NOT NULL,
                 session_name TEXT NOT NULL UNIQUE,
                 occupant_logical_id TEXT,
                 renew_id TEXT,
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
                 CHECK(
                     (state = 'queued' AND ask_id IS NULL)
                     OR (state != 'queued' AND ask_id IS NOT NULL)
                 )
             ) STRICT;

             CREATE UNIQUE INDEX IF NOT EXISTS turns_one_open_per_session
                 ON turns(session_id) WHERE state = 'open';

             CREATE INDEX IF NOT EXISTS turns_session_order
                 ON turns(session_id, sequence);",
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
            state: TurnState::from_str(&state)
                .ok_or_else(|| invalid_value(6, "invalid turn state"))?,
        })
    }
}

impl HostRepository for SqliteRepository {
    type Error = rusqlite::Error;

    fn mark_event_processed(&mut self, event_id: &EventId) -> Result<bool, Self::Error> {
        let changed = self.connection.execute(
            "INSERT INTO processed_events(event_id) VALUES (?1)
             ON CONFLICT(event_id) DO NOTHING",
            [event_id.as_str()],
        )?;
        Ok(changed == 1)
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
                 bot_id, channel_id, session_name, occupant_logical_id, renew_id
             ) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(bot_id, channel_id) DO UPDATE SET
                 session_name = excluded.session_name,
                 occupant_logical_id = excluded.occupant_logical_id,
                 renew_id = excluded.renew_id",
            params![
                session.bot_id.as_str(),
                session.channel_id,
                session.session_name,
                session.occupant_logical_id,
                session.renew_id,
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
                "SELECT bot_id, channel_id, session_name, occupant_logical_id, renew_id
                 FROM sessions WHERE bot_id = ?1 AND channel_id = ?2",
                params![bot_id.as_str(), channel_id],
                |row| {
                    let stored_bot_id: String = row.get(0)?;
                    Ok(SessionRecord {
                        bot_id: parse_bot_id(&stored_bot_id, 0)?,
                        channel_id: row.get(1)?,
                        session_name: row.get(2)?,
                        occupant_logical_id: row.get(3)?,
                        renew_id: row.get(4)?,
                    })
                },
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
        transaction.execute(
            "UPDATE turns SET ask_id = ?1, state = 'open'
             WHERE sequence = ?2 AND state = 'queued'",
            params![ask_id, sequence],
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
        if matches!(state, TurnState::Queued | TurnState::Open) {
            return Ok(false);
        }
        let changed = self.connection.execute(
            "UPDATE turns SET state = ?1
             WHERE ask_id = ?2 AND state = 'open'",
            params![state.as_str(), ask_id],
        )?;
        Ok(changed == 1)
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

    fn sessions_with_open_turns(&self) -> Result<Vec<SessionRecord>, Self::Error> {
        let mut statement = self.connection.prepare(
            "SELECT DISTINCT s.bot_id, s.channel_id, s.session_name,
                    s.occupant_logical_id, s.renew_id
             FROM sessions AS s
             JOIN turns AS t ON t.session_id = s.id
             WHERE t.state = 'open'
             ORDER BY s.bot_id, s.channel_id",
        )?;
        let sessions = statement
            .query_map([], |row| {
                let bot_id: String = row.get(0)?;
                Ok(SessionRecord {
                    bot_id: parse_bot_id(&bot_id, 0)?,
                    channel_id: row.get(1)?,
                    session_name: row.get(2)?,
                    occupant_logical_id: row.get(3)?,
                    renew_id: row.get(4)?,
                })
            })?
            .collect();
        sessions
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
         VALUES (?1, ?2, ?3, 'queued')",
        params![
            session_id,
            turn.event_id.as_str(),
            turn.reply_to_event_id.as_ref().map(EventId::as_str),
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

        assert_eq!(
            repository.session(&bot_id, channel_id).unwrap(),
            Some(expected)
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
    fn open_sessions_can_be_enumerated_for_recovery() {
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
        repository
            .open_next_turn(&bot_id, channel_id, "ask-1")
            .unwrap();

        assert_eq!(
            repository.sessions_with_open_turns().unwrap(),
            vec![expected]
        );
    }
}
