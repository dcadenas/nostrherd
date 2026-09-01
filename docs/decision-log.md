# Decision log

Accepted decisions only. Proposals stay in `docs/open-questions.md`.

## D1. One process, actor per bot

Status: accepted

Multiple bots are configured. Each is an in-process actor (mailbox +
serialized turns), mapped to one corpus git repo. Not one OS process
per bot, and not one shared god-object.

## D2. One Kelpie waiter for the host

Status: accepted

`botserver` is a single adopted Herdr occupant and the waiter of every
ask. Session occupants are `bot-<place>` (or `<botid>-<place>`).
Receipts multiplex on ask id in SQLite. A waiter pane per bot is
deferred.

## D3. SQLite is host state, not the relay

Status: accepted

SQLite stores processed events, session bindings, in-flight turns,
renew ids. It does not store message bodies as the source of truth.
Corpus git stores personality. No nsecs in SQLite.

## D4. botcli is the occupant publish path

Status: accepted

Occupants MUST NOT call the relay SDK with a raw key. `botcli` stamps
the prefix, publishes with envchain, then `kelpie reply --final` as the
owing occupant.

## D5. Kelpie ask, not tell, for triggered work

Status: accepted

Triggered Nostr work is an ask so pending, reminders, and amnesia work.
The waiter is `botserver`. Successful out-of-band post completes with
final, never cancel.

## D6. Renew is wall-clock

Status: accepted

Bound occupant context with Kelpie `--every`. Durable channel context
is files the host writes. Token-count renew is later.

## D7. herdr-acp is out of this path for v1

Status: accepted

Session occupants are Kelpie-started in the corpus cwd. This repo does
not exec `herdr-acp` or `buzz-acp`.

## D8. Inject only on an explicit trigger

Status: accepted

Closes Q1. The host MUST open or poke a session only when the event is
a trigger (D9). If nobody ever writes that form, no occupant exists.
Existing occupants MUST NOT receive ordinary messages, thread follow-ups
without the prefix, or a bare mention of the operator.

The operator's own unprefixed reply in a thread is human mail.

## D9. Fixed trigger and stamp protocol

Status: accepted

Closes Q2.

Inbound: the event MUST `p`-tag the operator pubkey and the body MUST
have `bot:` as the first token after an optional leading mention.
Example: `@daniel bot: hello`.

Outbound: `botcli` MUST prefix the published body with `[bot]:`.
Occupants MUST NOT stamp it themselves. Own `[bot]:` posts MUST NOT
trigger a new turn (`ignore_self`).

## D10. Session grain follows Buzz: one occupant per channel

Status: accepted

Closes Q3. Buzz keys ACP sessions by channel UUID (`has_session_for`
in buzz-acp; one agent in five channels is five sessions). Threads are
NIP-10 reply targets inside that session, not separate occupants. DMs
are channels with their own UUID.

Session name is `<botid>-<channel-slug>` (slug from channel display +
stable id as needed to stay unique and ≤32 chars). Reply-to event id
travels on the Turn for `botcli`, not in the session name.

## D11. The operator may trigger their own bot

Status: accepted

`@daniel bot:` from the operator pubkey is a trigger. That is how you
talk to the bot without a second identity.

## D12. One in-flight turn per session; channels are independent

Status: accepted

A second trigger in the same channel while a Turn is open MUST queue
and become the next ask after that turn's final. A trigger in another
channel is another session and MUST NOT wait on the first.

## D13. Thread triggers stay on the channel session

Status: accepted

`@daniel bot:` inside a thread still addresses `bot-<channel>`. The
thread id is `botcli --reply-to` on that Turn. No extra occupant.

## D14. Edits replace an unposted turn only

Status: accepted

If the triggering event is edited before `botcli` publishes, the host
MUST replace the in-flight prompt body. After a successful post, an
edit is ignored unless a new trigger arrives.

## D15. Deletes abandon an unposted turn

Status: accepted

If the triggering event is deleted before publish, the host MUST
`kelpie cancel` that ask (work abandoned). After publish, leave the
`[bot]:` post up.

## D16. Only kind:9-style channel/DM text can trigger

Status: accepted

Reactions, huddles, canvas, kind:0, and file-only events without a
`bot:` text body MUST NOT open a Turn.

## D17. One outbound post per turn in v1

Status: accepted

Closes Q5. No `[bot]: working…` protocol. The occupant may take time;
people see one stamped reply when `botcli` runs.

## D18. Silent subscriber

Status: accepted

Closes the presence half of Q4. `botserver` MUST NOT publish presence
or typing as the operator. Buzz desktop remains the human session.
Unread badges on the desktop are later (rest of Q4).

## D19. Snapshots are per-channel files, refreshed on turn open

Status: accepted

The host writes last-N-days (and any extra sources) to a file for that
session. Occupant start and each new Turn point at that file. A channel
snapshot MUST NOT include other channels' DMs.

## D20. Recover the logical occupant; do not mint a twin

Status: accepted

If the pane is gone but the ask is open: Kelpie reminder, then
`adopt --logical-id`. Do not `kelpie start` a new logical agent
because the public name is free.
