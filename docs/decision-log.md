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

## D14. Edits collapse to one reply that matches the latest text

Status: accepted

User-visible: at most one `[bot]:` for that triggering event, and it
answers the **latest** body.

Turn transition, if `botcli` has not published: `kelpie cancel` the
open ask (abandoned: the question changed), then open a **new** Turn /
ask on the same `EventId` with the new body. `botcli` MUST NOT publish
if its ask is already cancelled (so a late occupant cannot post the
stale text).

If a `[bot]:` already landed, later edits of the trigger are ignored
unless a new `@daniel bot:` arrives.

## D15. Deletes abandon an unposted turn

Status: accepted

If the triggering event is deleted before publish, the host MUST
`kelpie cancel` that ask (work abandoned). After publish, leave the
`[bot]:` post up.

## D16. Only kind:9-style channel/DM text can trigger

Status: accepted

Kind 9 and Buzz stream-message-v2 kind 40002 are channel/DM text.
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

User-visible: still at most one eventual `[bot]:` for that Turn
(recovery MUST NOT double-post).

## D21. Issue contract baseline is a SHA, not a comment

Status: accepted

Each implementation issue records `Contract baseline: <sha>` and the
D-numbers it implements. Before coding, the agent MUST update from
`origin/master` (not an arbitrary topic-branch pull). If HEAD differs
from the baseline, it MUST edit the issue body (scope, acceptance,
deps) and the baseline SHA. A comment alone is not enough.
## D22. botcli is send, stdin body, JSON receipt

Status: accepted

`botcli` publishes to Nostr as the operator. It is not Kelpie. The
command is `send`. Body is `--stdin` or `--file` only. Default stdout
is a JSON receipt. Closing the Kelpie ask after a successful post is
an implementation side effect, not the occupant-facing verb.
## D23. Ingest enforces Buzz mutation contracts

Status: accepted

Message edits (kind 40003) and NIP-09 deletes (kind 5) MUST be signed by
the indexed target event's effective author. For events signed by the
configured Buzz relay, effective author follows Buzz: the `actor` tag,
then the first `p` tag as a legacy fallback. That attribution `p` tag is
not also an operator mention. Buzz moderator tombstones (kind 9005) are
accepted from a different author because the Buzz relay authorizes the
event author, channel owner/admin, or owning human before storing the
tombstone. This trust applies only to the configured Buzz relay.

Each edit or delete MUST identify exactly one valid target EventId with
an `e` tag. Events with zero or multiple valid targets are indexed but
MUST NOT mutate a Turn, matching Buzz relay validation.

## D24. Relay subscriptions are scoped and refreshed

Status: accepted

Ingest uses separate filters for operator `p`-tag discovery, known-channel
`h`-tag traffic, and active-turn mutation `e` tags. Every filter carries
an inclusive persisted `since` cursor. The cursor starts at the oldest
unprocessed actionable event; with none pending it overlaps the newest
indexed timestamp by Buzz's 15-minute accepted clock drift. The host
refreshes subscriptions when known channels or active EventIds change;
an empty set closes its corresponding subscription rather than widening
it. The consumer MUST acknowledge every emitted ingest action, including
actions it intentionally declines, so declined work cannot pin the replay
cursor indefinitely.

## D25. Occupant start is a bootstrap tell; triggers are always asks

Status: accepted

A new channel occupant is created with `kelpie start --tell` and a short
trusted bootstrap body. The triggering Nostr text is always a `kelpie
ask` owned by waiter `botserver` (D5), including on the first trigger.
Start and ask keep separate receipts so an accepted runtime does not
imply the trigger obligation exists.

## D26. A trigger with an empty request opens no Turn

Status: accepted

D9 requires `bot:` as the first token after an optional mention. If the
remainder is empty (`bot:` or `@daniel bot:` with no request text), the
host MUST acknowledge the event and MUST NOT open a Turn, start an
occupant, or send an ask. Silence here is not D8 untriggered traffic;
it is a classified trigger with nothing to answer.

## D27. Place snapshots are last 7 days; renew is every 45 minutes

Status: accepted

Closes the unspecified window and interval in D6/D19 for v1. The host
writes one file per channel session at
`<corpus>/.botserver/places/<session-name>.md` from indexed events with
that channel UUID only, in the window `[now - 7 days, now + 900s]` (the
upper slack is Buzz's accepted clock drift from D24). Corpus
`startup.md` points at `.botserver/places/<your public Kelpie name>.md`.
Occupant start and each new Turn refresh that file. After start, the
host tries to arm `kelpie renew --every 45m --on-timeout abort` on the
occupant's exact incarnation. Prepare writes `progress.md`. Resume reads
`startup.md` and the snapshot. A Kelpie failure leaving `renew_id` unset
does not block the ask; the next turn retries the arm. D19's MUST is the
file contents, not occupant filesystem isolation: occupants share the
corpus cwd (D7). Token-count renew remains later (Q6).

## D28. Publish reservation is a claim, not a TurnState

Status: accepted

`botcli` and the host share one SQLite claim on an `open` turn. `botcli`
MUST set the claim before relay publish. Host edit/delete cancel MUST
UPDATE only `queued` rows or `open` rows whose claim is clear. A claimed
turn is treated as already landing: later edits and deletes of that
EventId are ignored (D14 after publish). A failed publish MUST clear the
claim so retry can proceed. Do not add a `publishing` TurnState.
