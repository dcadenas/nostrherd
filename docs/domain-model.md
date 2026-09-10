# Domain model

Bounded context: **personal Nostr bot host**.

No types here talk to SQLite, Kelpie, or the relay. Those adapters live
in `nostrherd`.

## Aggregates

### Bot

Configured personality. Identity is `BotId` (stable slug, e.g. `bot`).
Holds: corpus repo path, inbound trigger, outbound prefix, occupant kind,
requester public keys (empty means any relay member), and an optional private
operator Kelpie session.
Does not hold live Kelpie ids (those are session runtime).

One actor in the host process maps 1:1 to one `Bot`.

### Session

A live conversation lane for one bot in one opaque **channel ID**
(including DMs). Name is derived, e.g. `bot-foobar`. Threads are not
sessions (D10).
Holds: place id, occupant Kelpie logical id when bound, renew id when
armed, and the ask-context cursor (last stuffed event id /
created_at).

### Turn

Work owed for one triggering Nostr event. Holds: event id, Kelpie ask
id, `TurnState` (`queued`, `open`, `posted`, `failed`, `cancelled`).
Trigger turns persist the effective requester public key and request in
`ask_body` (D57); host wakes instead store their typed body. A non-null
publish reply target distinguishes trigger turns from host wakes.
Completing a turn is occupant `kelpie reply --final`, then host
publish, then `inbox.ack`, not cancel. An occupant tell is not a turn
(D38): it is a bot-initiated post on a known session channel. Publish reservation is a claim,
not a state (D28). The host keeps a durable outbound attempt (body,
channel, trigger EventId, mention, accepted event id) so retry cannot
mint a second stamp. While work on a trigger EventId is queued or open
the host marks it with `⏳` and removes the marker when that work ends
(D35).
An edit that re-queues the same EventId keeps the marker up. Relayed
progress (D42) is one host-created, host-edited stamped post per ask —
kept on final, Buzz-deleted (kind 9005) on cancel, excluded from
snapshots and ask Context — with its own durable progress row (D28).
A host-initiated Turn persists its typed ask body and has no relay reply target
or mention. Its final still uses the same stamped publish attempt and terminal
Turn transition (D45, D46). Unprompted publishes are not counted or rate
limited by the host; how often a bot speaks is its own judgement, written
in its corpus (D52).

### Watch

Durable host predicate declared by one bot session. Holds: author public keys,
optional channel and kind, cooldown, expiry or maximum fires, fire count, and
state. A separate immutable fire ledger maps one matched source event to one
synthetic wake Turn id. The host records that mapping before it asks an
occupant. Kelpie has no relay visibility, so this state does not duplicate its
timer ledger (D44, D45).

## Values

- `EventId`, `Pubkey` — relay coordinates, opaque hex.
- `Place` — `Channel(id)` | `Dm(pubkey)` | `GroupDm(id)` (shape TBD).
- `TriggerMatch` — first token `{bot-id}:` after an optional mention,
  plus either operator authorship or an operator `p`-tag from a requester
  allowlisted for that bot (D8, D9, D34, D57).
  A non-prefix reply in an open thread is not a match.
- `ChannelAudience` — current member-list roster when available, otherwise
  observed effective authors; uncertain and observed-only audiences are shared.
- `TurnState` / `TurnTransition` — parsed tokens. Illegal changes are
  `None`.

## Invariants

Named tests live in `docs/invariants.md`.
