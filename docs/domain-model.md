# Domain model

Bounded context: **personal Nostr bot host**.

No types here talk to SQLite, Kelpie, or the relay. Those adapters live
in `botserver` / `botcli`.

## Aggregates

### Bot

Configured personality. Identity is `BotId` (stable slug, e.g. `bot`).
Holds: corpus repo path, inbound trigger, outbound prefix, occupant
kind. Does not hold live Kelpie ids (those are session runtime).

One actor in the host process maps 1:1 to one `Bot`.

### Session

A live conversation lane for one bot in one **place** (channel, DM,
group DM). Name is derived, e.g. `bot-foobar`, `bot-dm-<pubkey8>`.
Holds: place id, occupant Kelpie logical id when bound, renew id when
armed.

### Turn

Work owed for one triggering Nostr event. Holds: event id, Kelpie ask
id, state (`open`, `posted`, `failed`, `cancelled`). Completing a turn
is `botcli` publish **then** Kelpie final, not cancel.

## Values

- `EventId`, `Pubkey` — relay coordinates, opaque hex.
- `Place` — `Channel(uuid)` | `Dm(pubkey)` | `GroupDm(id)` (shape TBD).
- `TriggerMatch` — parsed from event content + tags against a bot's
  trigger. Q1 (follow-up bind) decides whether a non-prefix reply in an
  open thread is a match.

## Invariants

1. Two bots MUST NOT share a session name.
2. A session for place A MUST NOT be given history from place B
   (especially DMs into a channel session).
3. Processed `EventId`s are idempotent: a replay MUST NOT open a second
   turn.
4. `from=` on Kelpie envelopes for this host is only `botserver`.
