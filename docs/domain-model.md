# Domain model

Bounded context: **personal Nostr bot host**.

No types here talk to SQLite, Kelpie, or the relay. Those adapters live
in `botserver` (leftover `botcli` is not the occupant path).

## Aggregates

### Bot

Configured personality. Identity is `BotId` (stable slug, e.g. `bot`).
Holds: corpus repo path, inbound trigger, outbound prefix, occupant
kind. Does not hold live Kelpie ids (those are session runtime).

One actor in the host process maps 1:1 to one `Bot`.

### Session

A live conversation lane for one bot in one Buzz **channel UUID**
(including DMs). Name is derived, e.g. `bot-foobar`. Threads are not
sessions (D10).
Holds: place id, occupant Kelpie logical id when bound, renew id when
armed.

### Turn

Work owed for one triggering Nostr event. Holds: event id, Kelpie ask
id, `TurnState` (`queued`, `open`, `posted`, `failed`, `cancelled`).
Completing a turn is occupant `kelpie reply --final`, then host
publish, then `inbox.ack`, not cancel. Publish reservation is a claim,
not a state (D28). The host keeps a durable outbound attempt (body,
channel, trigger EventId, mention, accepted event id) so retry cannot
mint a second `[bot]:`.

## Values

- `EventId`, `Pubkey` — relay coordinates, opaque hex.
- `Place` — `Channel(uuid)` | `Dm(pubkey)` | `GroupDm(id)` (shape TBD).
- `TriggerMatch` — operator `p`-tag plus first token `bot:` after an
  optional mention (D8, D9). A non-prefix reply in an open thread is
  not a match.
- `TurnState` / `TurnTransition` — parsed tokens. Illegal changes are
  `None`.

## Invariants

Named tests live in `docs/invariants.md`.
