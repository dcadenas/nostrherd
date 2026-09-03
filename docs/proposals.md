# Proposals

Design proposals only. Nothing here is accepted; promotion goes through
`docs/decision-log.md`. Both proposals share one new concept, the
host-initiated wake, described first.

## Shared concept: host-initiated wake

Today a Turn opens only on a user-written `{id}:` trigger (D8, D9), and
the ask is owned by waiter `botserver` (D5). Both proposals need the
host to open a turn **without** a user writing a trigger: a presence
watch fired, or a schedule came due.

Proposal: a second trigger origin, host-initiated, that flows through
the exact existing pipeline — same `kelpie ask` to `bot-<place>`, same
`--reply-to` coordinates, same stamp on final, same queued/open states
(D12), same publish path (D31, D37). The only new surface is a typed
body section so the occupant can tell a wake from a user request, e.g.:

```text
## Scheduled status
Every 5m you report the queue depth.
```

```text
## Watch event
sebastian just posted in this channel (first activity in 4h).
```

What this deliberately does not change: occupants never touch the relay
and never get the nsec (D31); the host never publishes presence or
typing (D18); one outbound post per turn still holds (D17); a wake
turn's final publishes as a normal stamped kind 9 (D37), or, for
bot-initiated posts that are not turn answers, the D38 tell path.

## P1. Presence proxy (author-activity watches)

Flow to support: "when <person> gets online, tell them Y", "when
sebastian first posts in #eng, poke bot-eng".

Reality check: Nostr/Buzz has no native presence protocol. Slack
(`presence_sub` → `presence_change`), Discord (privileged
`PRESENCE_UPDATE` intent), and Matrix (`m.presence` in incremental
sync) all reduce to the same shape: a scoped subscription over an
existing event stream, a persisted cursor, and delta delivery. botserver
already has that substrate — the host is the silent subscriber holding
the only relay connection with scoped filters and persisted `since`
cursors (D18, D24). On this stack, "online" can only mean observable
relay activity by an author; the proposal embraces that instead of
faking a presence protocol nobody broadcasts.

Design:

- A durable `watch` record in SQLite:
  `{watch_id, bot_id, session, author pubkey(s), predicate, cooldown,
  expires_at | max_fires, state}`. Predicate v1: any indexed activity
  by the author, optionally narrowed to a channel or kind (D16 kinds).
- Ingest extends the D24 filter model with author-scoped filters;
  subscriptions refresh when watches change (mechanism already exists
  for channel/active-turn changes). No new relay connections, no new
  privileges.
- On match, cooldown permitting: mark fired, then a host-initiated wake
  (typed section `## Watch event`) to the declaring session's occupant.
  Cooldown default (e.g. 30m) prevents "every keystroke pokes the bot".
- The occupant acts on the wake like any turn: answer, publish, done.
  If the user's goal is "tell them Y", that eventual post to a third
  party is the occupant's answer content — the host stamps and publishes
  as always; it does not become a DM send primitive in v1.

Open points for a decision: which session's occupant a DM-declared
watch wakes; whether watch management is by trigger phrase (`bot: watch
sebastian here`) or config file (v1: trigger phrase, since it needs no
new interface); retention of watch records.

## P2. Recurring schedules

Flow to support: "every 5 minutes, tell X the status of Y".

Layering is the whole idea: kelpie tells are deliberately one-shot
("not cron"), Telegram ships one-shot `schedule_date` and leaves
recurring to app-level job queues (JobQueue), and workflow engines own
missed-fire policy explicitly (Temporal Schedules: catchup window +
high-water mark; classic cron: persisted last-run cursor + explicit
skip/run-once policy). The cron belongs in the process with durable
state and a restart story — here, the host with SQLite (D3) — not in
the occupant's context window, which renew clears every 45m (D27), and
not in kelpie, whose stated semantics are one-shot delivery.

Design:

- A durable `schedule` record in SQLite:
  `{schedule_id, bot_id, session, every (v1: fixed interval),
  next_due_at (high-water mark), mode, expires_at | max_runs, state}`.
- Two payload modes:
  - **static**: host posts the stored text at due time through the D38
    tell-publish path (stamped, no `--reply-to`, no ⏳).
  - **interrogate**: host fires a host-initiated wake (`## Scheduled
    status`) asking the occupant for the status; the occupant's final
    publishes through the normal turn pipeline, so the posted text is
    fresh, not stored. This is the "tell X the status of Y" case, and
    it reuses everything: queueing when busy (D12), stamping (D37),
    one post per turn (D17).
- Firing is durable and idempotent in the D28 spirit: record dispatch
  before send; retry consults recorded state, never wall-clock alone.
  Missed-fire policy v1: skip, with a bounded catchup window
  (Temporal-style) — a schedule stuck behind a long turn skips ticks
  rather than stampeding. Host already runs a periodic tick
  (`SUBSCRIPTION_REFRESH`); the scheduler joins it or adds one.
- Termination v1 is dumb and durable: `expires_at` and/or `max_runs`.
  "Stop when X happens" predicates are explicitly out of scope until
  someone needs them.
- Management v1 by trigger phrase (`bot: every 5m report the queue`,
  `bot: stop the queue reports`), recognized as control phrases on
  triggers; the host cancels/creates schedule records.

Open points for a decision: drift/jitter policy (fire on due-or-later
wall clock vs anchored cadence); whether schedules survive bot
retirement (v1: they die with the session); control-phrase grammar;
whether interrogate-mode wakes queue behind an open turn (yes, D12) and
what the max backlog is before skipping (the catchup window answers
this).
