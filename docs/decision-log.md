# Decision log

Accepted decisions only. Proposals stay in `docs/open-questions.md`.

## D1. One process, actor per bot

Status: accepted

Multiple bots are configured. Each is an in-process actor (mailbox +
serialized turns), mapped to one corpus git repo. Not one OS process
per bot, and not one shared god-object.

## D2. One Kelpie waiter for the host

Status: accepted

The host waiter is one pane-less socket LogicalAgent named `nostrherd`,
created with `waiter.register` (idempotent). It receives on a
reconnecting `inbox.claim` and resolves with `inbox.ack`. There is no
fake pane occupant. Occupant envelopes still use `from=nostrherd`, never
a relay pubkey and never `operator`. `--from operator` is sender
attribution only; the waiting agent stays `nostrherd`. Session occupants
are `bot-<place>` (or `<botid>-<place>`). Receipts multiplex on ask id
in SQLite.

Implementation note (issue 83): the inbox honors nonempty `KELPIE_SOCKET`, otherwise
uses nonempty `XDG_RUNTIME_DIR/kelpie/kelpie.sock`, otherwise
`std::env::temp_dir()/kelpie-client/kelpie/kelpie.sock`, matching Kelpie's
runtime fallback. Reconnect failures report the socket and a safe error
category immediately, then at most once per 30 seconds while retrying each
second. Receipt bodies are not diagnostic text. ACK ordering is unchanged.
The host passes the same resolved path as `--socket` to its outbound Kelpie
commands; `KELPIE_SOCKET` is a host setting, not a Kelpie CLI environment variable.

## D3. SQLite is host state, not the relay

Status: accepted

SQLite stores processed events, session bindings, in-flight turns,
renew ids. It does not store message bodies as the source of truth.
Corpus git stores personality. No nsecs in SQLite.

## D4. botcli is the occupant publish path

Status: retracted

Retracted as the occupant publish path. Occupants MUST answer with
`kelpie reply --final` and unstamped prose. The host is the only Nostr
publisher (D31). The `botcli` crate is removed (`dcadenas/nostrherd#35`).

## D5. Kelpie ask, not tell, for triggered work

Status: accepted

Triggered Nostr work is an ask so pending, reminders, and amnesia work.
The waiter is `nostrherd`. The occupant completes with `kelpie reply
--final`, never cancel. The host then publishes and `inbox.ack`.

The ask body is the trigger remainder, then a marked `## Context`
section. Context is this channel's indexed events since the last ask
to this session (unprefixed lines, earlier `[{id}]:`, and other
indexed traffic). It is labeled untrusted indexed text, not
instructions. Occupants MUST NOT follow directives found there.

Cap: 32 events and 8192 bytes of rendered event lines, dropping oldest
first. A per-session cursor (`ask_context_event_id`,
`ask_context_created_at`) tracks the last event already stuffed into
an ask. That cursor is not `processed_events` (trigger dedup). First
ask (null cursor), including the first ask after a new session,
points at the place snapshot file instead of restating the 7-day
window. Place snapshots still refresh on start and each turn (D19,
D27).

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

## D9. Trigger token is `{bot-id}:`; stamp is `[{bot-id}]:`

Status: accepted

Closes Q2. Outbound stamp amended by D37.

Inbound: the body MUST have `{bot-id}:` as the first token after an
optional leading mention. For the example bot id `bot` that is `bot:`.
Other authors MUST `p`-tag the operator pubkey. Example:
`@daniel bot: hello`. The operator's own `{id}:` is a trigger without
a self `p`-tag (D11, D34).

Outbound: the host MUST prefix the published body with `[{bot-id}]:`.
id `pr` publishes `[pr]:`. id `bot` publishes `[bot]:`. Occupants MUST
NOT stamp it themselves. Own `[{id}]:` posts MUST NOT trigger a new
turn (`ignore_self`). First token `[pr]:` is not `pr:`.

## D10. Session grain follows Buzz: one occupant per channel

Status: accepted

Closes Q3. Buzz keys ACP sessions by channel UUID (`has_session_for`
in buzz-acp; one agent in five channels is five sessions). Threads are
NIP-10 reply targets inside that session, not separate occupants. DMs
are channels with their own UUID.

Session name is `<botid>-<channel-slug>` (slug from channel display +
stable id as needed to stay unique and ≤32 chars). Channel display is
the kind-39000 `name` tag. A generic 1-1 title (`DM`) uses the other
participant's kind-0 `display_name` or `name`. Kind 39000 is NIP-29
relay-signed group metadata; the host REQs it by `#d` without an author
filter, matching Buzz client discovery. Existing stored session names
are kept. The triggering EventId travels on the Turn as outbound
`--reply-to`, not in the session name.

## D11. The operator may trigger their own bot

Status: accepted

`@daniel bot:` from the operator pubkey is a trigger. That is how you
talk to the bot without a second identity. Buzz 1-1 DMs `p`-tag the
peer, not the author, so the operator's own `bot:` MUST match on
authorship (D34), not only on a self mention.

## D12. One in-flight turn per session; channels are independent

Status: accepted

A second trigger in the same channel while a Turn is open MUST queue
and become the next ask after that turn's final. A trigger in another
channel is another session and MUST NOT wait on the first.

## D13. Thread triggers stay on the channel session

Status: accepted

`@daniel bot:` inside a thread still addresses `bot-<channel>`.
Outbound `--reply-to` is the triggering EventId (D31). Keep the
trigger's existing parent separately when snapshots need thread-root
context. No extra occupant.

## D14. Edits collapse to one reply that matches the latest text

Status: accepted

User-visible: at most one final `[bot]:` for that triggering event, and it
answers the **latest** body. A progress post (D42) is not a second answer
and is deleted when the ask is replaced.

Turn transition, if the host has not published: `kelpie cancel` the
open ask (abandoned: the question changed), then open a **new** Turn /
ask on the same `EventId` with the new body. The host MUST NOT publish
if that ask is already cancelled (I10; a late occupant final cannot
post the stale text).

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

Closes Q5. The occupant may take time; people see one final stamped
reply when the host publishes after occupant final. The host never
posts an invented working ping. Progress prose is relayed as one
host-edited progress post per ask (D42), never as a series of posts.

## D18. Silent subscriber

Status: accepted

Closes the presence half of Q4. `nostrherd` MUST NOT publish presence
or typing as the operator. Buzz desktop remains the human session.
Unread badges on the desktop are later (rest of Q4).

## D19. Snapshots are per-channel files, refreshed on turn open

Status: accepted

The host writes last-N-days (and any extra sources) to a file for that
session. Occupant start and each new Turn point at that file. A channel
snapshot MUST NOT include other channels' DMs.

## D20. Recover the logical occupant; do not mint a twin

Status: accepted

If the pane is gone while an ask is open or work is queued, recover that
logical agent. Do not `kelpie start` a new logical agent because the public
name is free. When a queued ask reports the recorded occupant unavailable,
cancel that failed ask's obligation, retire the exact unavailable incarnation,
continue that logical agent, and retry the ask once. Cancelling prevents stale
obligations from accumulating; retiring the stale binding prevents two Ready
incarnations from making later alias resolution ambiguous.

The stable-key retry requires the outcome-aware prompt idempotency fix tracked
by dcadenas/kelpie issue 37 (failed-prompt retry semantics). Deploy that Kelpie
release before treating queued occupant recovery as live end-to-end behavior.

A replacement pane has no occupant runtime, so `kelpie adopt` cannot
bind it. The host continues the recorded id with `kelpie start
--logical-id` on that pane. That is a new incarnation of the same
agent, not a twin. `adopt --logical-id` remains the path when a live
pane already exists.

Implementation notes (issue 82): record launch coordinates and a stable start
key before calling Kelpie, retaining each attempt's receipt and diagnostic.
An ambiguous attempt is reconciled by its pane and terminal, with matching
name, backend, corpus and any recorded logical id; a namesake is not evidence.
Adopt a live unsettled seat under that same logical id. An ended incarnation
may retry on a new pane under its recorded logical id when no other live or
unsettled incarnation exists. With no visible declaration, replay the stored
request on the same seat and key: Kelpie reserves start keys atomically with
identity creation and refuses their reuse after any outcome. Never vary the key
to bypass a refusal. Conflicting evidence keeps the attempt unsettled without
another allocation. A receipt's logical id disambiguates a subsequently reused
seat; ambiguous evidence without such an id is not a namesake-selection rule.
Launch completion and the session binding are one SQLite transaction. Keep the
first start error separately from the latest reconciliation diagnostic.

A `declared` incarnation whose recorded **start operation** has failed is also
recoverable under that logical id. A failed adoption alone is not evidence
that a pending or unknown native start has ended.

An original open ask stays open. Its recovery MUST NOT send a second ask;
Kelpie's reminder delivers the original question. Queued work has no prior
delivered ask, so it drains through the recovered occupant.

User-visible: still at most one eventual final `[bot]:` for that Turn
(recovery MUST NOT double-post).

## D21. Issue contract baseline is a SHA, not a comment

Status: accepted

Each implementation issue records `Contract baseline: <sha>` and the
D-numbers it implements. Before coding, the agent MUST update from
`origin/master` (not an arbitrary topic-branch pull). If HEAD differs
from the baseline, it MUST edit the issue body (scope, acceptance,
deps) and the baseline SHA. A comment alone is not enough.
## D22. botcli is send, stdin body, JSON receipt

Status: retracted

Retracted as the occupant publish path. Occupants MUST NOT use a send
tool to post. The `botcli` crate is removed (`dcadenas/nostrherd#35`).
Host publish is D31.

## D23. Live tests use the throwaway local relay

Status: accepted

Issues that need a real relay MUST use `skills/local-relay` and
`tools/local-relay`. Throwaway envchain namespaces `nostrherd-proof`
(operator) and `nostrherd-proof-peer` (peer). Never `nostr-personal`
or `buzz-acp`.

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

Ingest uses separate filters for operator `p`-tag discovery,
operator-authored kind 9/40002, known-channel `h`-tag traffic, and
active-turn mutation `e` tags. Every filter carries
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
ask` owned by waiter `nostrherd` (D5), including on the first trigger.
Start and ask keep separate receipts so an accepted runtime does not
imply the trigger obligation exists.

## D26. A trigger with an empty request opens no Turn

Status: accepted

D9 requires `{bot-id}:` as the first token after an optional mention.
If the remainder is empty (`bot:` or `@daniel bot:` with no request text
when the bot id is `bot`), the
host MUST acknowledge the event and MUST NOT open a Turn, start an
occupant, or send an ask. Silence here is not D8 untriggered traffic;
it is a classified trigger with nothing to answer.

## D27. Place snapshots are last 7 days; renew is every 45 minutes

Status: accepted

Closes the unspecified window and interval in D6/D19 for v1. The host
writes one file per channel session at
`<corpus>/.nostrherd/places/<session-name>.md` from indexed events with
that channel UUID only, in the window `[now - 7 days, now + 900s]` (the
upper slack is Buzz's accepted clock drift from D24). Corpus
`startup.md` points at `.nostrherd/places/<your public Kelpie name>.md`.
Occupant start and each new Turn refresh that file. The occupant
self-renews (`kelpie renew --every 45m --on-timeout abort` on its own
incarnation). The host MUST NOT arm occupant renew with `--sender-id`
of waiter `nostrherd` (D32). Prepare writes the session checkpoint (D56). Resume reads
`startup.md` and the snapshot. D19's MUST is the file contents, not
occupant filesystem isolation: occupants share the corpus cwd (D7).
Token-count renew remains later (Q6).

## D28. Publish reservation is a claim, not a TurnState

Status: accepted

The host claims an `open` turn before relay publish. Host edit/delete
cancel MUST UPDATE only `queued` rows or `open` rows whose claim is
clear. A claimed turn is treated as already landing: later edits and
deletes of that EventId are ignored (D14 after publish). A failed
publish MUST clear the claim so retry can proceed. Do not add a
`publishing` TurnState.

Publish is in-process over the host's own nostr connection (D43,
amending the original CLI wording here): the host prepares and signs
the stamped event, records `dispatched` with the prepared event id and
timestamp before calling send, and stores the accepted id after relay
accept. A crash after that record redelivers the same event and the
relay dedups it, closing the mixed-attempt case this decision
originally deferred to a buzz change. Transport failures (relay did
not accept: drop, timeout) may redeliver; explicit relay rejections
and build failures do not send again.

The same record-before-send rule applies to the progress post's
create and to each of its edits (D42).

## D29. envchain wraps the process; binaries only read env

Status: accepted

`envchain NAMESPACE CMD` injects secrets into CMD's environment.
The host MUST read `NOSTRHERD_PRIVATE_KEY` and `NOSTRHERD_RELAY_URL` from the
environment when present. Binaries MUST NOT take `--envchain` and MUST
NOT exec `envchain` themselves. Occupants MUST NOT receive the nsec.

Wrap the host: `envchain nostrherd-proof nostrherd …` (live tests) or
`envchain nostrherd nostrherd …` (operator). The namespace name is not
a secret. The nsec MUST NOT be standing pane-env.

Supersedes the `--envchain` flag shipped in #6.

## D30. Operator personal envchain namespace is `nostrherd`

Status: accepted

Personal host wrap uses envchain namespace `nostrherd` with
`NOSTRHERD_PRIVATE_KEY` and `NOSTRHERD_RELAY_URL`. Occupants do not wrap a
publish binary. That namespace is distinct from throwaway
`nostrherd-proof` / `nostrherd-proof-peer` (D23 live-test relay). Do
not point `NOSTRHERD_RELAY_URL` at a production relay.

## D31. Host publishes; occupant only kelpie final

Status: accepted

Retracts D4/D22 as the occupant path. The occupant is an ordinary
Kelpie peer of waiter `nostrherd`. It answers with `kelpie reply
--final` and unstamped prose.

The host is the only Nostr publisher. It stamps `[{bot-id}]:`, posts
from sqlite coordinates, then `inbox.ack`. Occupants never get the
operator nsec. That includes the progress post (D42), stamped the
same way.

Outbound `--reply-to` is the triggering EventId, including the first
call. Keep the trigger's existing parent separately when snapshots need
thread-root context.

The host MUST `--mention` the indexed event's effective author (not the
raw relay signer, not an arbitrary `p` tag), including operator-authored
triggers. `ignore_self` still blocks retrigger. The progress post (D42)
is the exception: it carries no `--mention`.

I10 is a host MUST: a late occupant final on a cancelled ask MUST NOT
publish.

The host is the shipping publish path. Issue 34 is the live E2E that the
occupant only `kelpie reply --final` and the host stamps. Issue 48 is
the per-bot stamp `[{id}]:`. Issue 35 removes the leftover `botcli`
crate and occupant send recipes.

## D32. Occupant self-renews

Status: accepted

Amends D27. Snapshot files stay. The occupant arms its own wall-clock
renew. The host MUST NOT arm occupant renew with `--sender-id` of
waiter `nostrherd`, so this inbox only sees channel asks the host
created. The host may still schedule the policy on the occupant's
incarnation. `renew_id` remains the stored policy id.

## D33. Inbox ACK after the host decides

Status: accepted

Keep the claimed inbox connection and the reply body. ACK only after
the host decides. Do not ACK in the drain thread before the body is
durable.

Classify by `reply_to` in the host's Turn ids:

- Empty or whitespace-only final: do not ACK if a later valid final
  should still be allowed.
- Progress: relay per D42 (create or edit, best-effort), then ACK.
  Progress on a non-open turn or a dispatched attempt: ACK, do not
  relay.
- Cancelled turn: ACK, do not publish.
- Already posted: ACK.
- Unknown `reply_to`: do not publish.

## D34. Operator-authored `bot:` does not need a self `p`-tag

Status: accepted

Amends D9, D11, and D24.

A Buzz 1-1 DM composer `p`-tags the other participant, not the author.
Typing `{id}: ping` or `@daniel {id}: ping` as the operator therefore
does not mention the operator pubkey. Other authors still MUST `p`-tag
the operator. Own `[{id}]:` posts still do not trigger (`ignore_self`).

Discovery MUST also fetch and subscribe to kind 9/40002 events
**authored by** the operator. The `#p` filter alone never sees those
DM lines.

## D35. Host in-flight reaction is ⏳

Status: accepted

The host adds a NIP-25 kind-7 `⏳` on the triggering EventId when a
turn becomes queued or open, and removes it with NIP-09 kind 5 when
work on that EventId ends: posted, failed, or a cancel that does not
re-queue the same EventId (both published by the host over its own
nostr connection per D43; removal finds the operator's kind-7 and
deletes it). An edit that replaces the turn keeps the marker up.
Occupants never publish a reaction. This is the code-side marker; the
prose side is the progress post (D42). Progress never adds or removes
the marker. It is not presence or typing (D18).

The marker is visually distinct from Buzz ACP `👀` / `💬`. Failures are
best-effort: a failed add or remove MUST NOT fail the turn. A stale
`⏳` on a fast-fail path is acceptable. v1 is one emoji; `💬` stays
out of scope.

## D37. Host stamp is `[{bot-id}]:`

Status: accepted

Amends D9 and D31.

Issue 43 left the outbound stamp as a global `[bot]:` on purpose.
Two bots in one channel made that wrong: `pr:` published as `[bot]:`.

The host stamp is `[{bot-id}]:`. `stamp_outbound` takes that bot's
prefix. Occupants still MUST NOT stamp. Own `[pr]:` / `[bot]:` posts
MUST NOT open a turn. Do not keep a global `[bot]:` constant as the
only stamp.

## D38. Occupant tell is a bot-initiated channel post

Status: accepted

A Kelpie tell from a known occupant is a bot-initiated kind 9, not a
trigger answer (D5). Identity is `sender_public_name` matching
`session_name`, or `sender_agent_id` matching `occupant_logical_id`.
When both are present they MUST agree. Disagreeing or missing identity
is unknown: ACK, do not post. The host reads those fields from
`inbox.delivery` when Kelpie includes them. Absent fields fail closed.

Hang point is that occupant's channel. Nested `<nostrherd to="…">` in
the tell body is host routing, not a Kelpie flag. Inner text is the
body; prose outside the tag is scratch. No tag, or a tag with no `to`,
posts to this session's channel. `to` is an exact channel UUID or an
exact slug this bot already knows. Unknown or ambiguous `to` does not
post. Tells do not `--reply-to` a trigger, do not add `⏳`, and MUST
NOT close a `{id}:` turn. Stamp stays `[{bot-id}]:` (D37). Occupants
still have no nsec (D31). `--due-in` is Kelpie holding the delivery;
the host publishes when the tell is delivered.

## D39. Occupant is its own Herdr workspace

Status: accepted

New occupants are allocated with
`herdr workspace create --cwd <corpus> --label <session_name> --no-focus`.
The occupant pane is `.result.root_pane`. Session name / public alias
is unchanged (D10, issue 41). Kelpie start still uses that pane id and
terminal id.

Do not `herdr tab create`. That adds a tab to whatever workspace the
host process is in.

Reuse by label is not reliable in this issue. Herdr `workspace create`
always mints a new workspace; there is no atomic get-or-create. Labels
are not unique, and a workspace that already has that label may already
host a live occupant on its root pane. Recovery of a gone pane therefore
creates another session-named workspace. It MUST NOT attach to the
host's workspace. Reclaim of leaked occupant workspaces is out of
scope here.

## D40. Occupant progress is relayed prose

Status: accepted; mechanism is D42

Daniel ruled 2026-09-03 that the progress gap is a feature: the ⏳
marker (D35) is the code-side signal, while occupant `--progress`
bodies are prose carrying reasoning and semantics the marker cannot
express. The host should relay progress prose to the channel.

Q7 is closed by D42.

## D41. Waiter transport split: correlated reply, uncorrelated tell

Status: accepted (alignment, recorded 2026-09-03)

The host waiter is pane-less (D2): its transport is the claimed socket
inbox, not a Herdr pane prompt. The two repos align as follows.
`kelpie reply` resolves the durable obligation and routes by the
waiter's `delivery_transport` (`socket_inbox`); it works today and is
the only correlated reporting path. `kelpie tell nostrherd` needs
alias resolution to dispatch on transport; until Kelpie ships
socket-waiter alias delivery for unsolicited tells, D38's interface is
blocked, not broken: the host side (register-once, reconnecting claim,
fail-closed sender identity) is complete and unchanged. `ask` to a
socket waiter is a Kelpie product decision; nostrherd MUST NOT build
on it.

Confirmed during the 2026-09-03 kelpied restart: waiter identity
survives the daemon and the host reclaims its inbox within seconds
without re-registering; no host restart is needed. Diagnostics bugs
named to Kelpie: lazy-adoption errors that mask an active waiter-owned
alias ("socket waiter … already holds public name nostrherd", then
"no Ready binding and matches 0 unbound live agents"), and `name-info`
reporting an actively-claimed waiter not-live.

## D42. Progress is one host-edited stamped post per ask

Status: accepted

Closes Q7 (the mechanism for D40). Amends D14, D17, D20, D28, D31,
D33, D35, D40. D38 is unchanged: a tell is not progress and progress
is not a tell.

An occupant MAY report progress on an open trigger ask with
`kelpie reply <ask-id> --progress` and unstamped prose from `--stdin`
or `--file`. Each body is the full current status, not a delta. The
host relays it as one kind 9 post per ask: created once, then edited
in place with Buzz kind 40003 over the host's own connection (D43
amends the original `buzz messages edit` wording). Never a series of
posts, and never invented by the host.

Shape: stamped `[{bot-id}]:` (D37), `--reply-to` the triggering
EventId (same thread as the final, D31), no `--mention`, no marker
tag. The ⏳ reaction (D35) stays the code-side signal and is untouched
by progress; this post is the prose side.

Timing: the host creates the post only once the ask has been open for
the initial hold (20 s) and a non-empty body exists; a final that
arrives first discards the pending body. Later bodies coalesce: only
the newest pending body is sent, at most one edit per 30 s, at most 20
edits per ask; past the cap the host ACKs and drops with one operator
notice. Bodies are trimmed and capped at 1024 bytes on a char boundary
with a trailing `…`. Flush runs on the host refresh tick; timing is
best-effort.

Lifecycle: on final the host publishes a new stamped post (D31) and
leaves the progress post up. A cancel that abandons or replaces the
ask (D14, D15, D28) issues a Buzz delete (kind 9005) on that ask's
progress post over the host's own connection (D43), best-effort; a
replacement ask starts with no progress post. Recovery (D20) continues
the same ask and therefore the same post. A failed turn keeps the
post. Queued turns have no ask id and therefore no progress. Progress
for a turn that is not open, or whose outbound attempt is already
dispatched, ACKs without relay.

Durability (D28): the host records the progress row (post id, prepared
event id, edit count, last accepted send time, pending body) before it ACKs the
delivery. Create is prepared and recorded before `send`; relay happens on the
refresh tick, never in the delivery handler. A crash after preparation without
a stored accepted id redelivers the same prepared event. Edit count and last
send time advance only after relay acceptance, so a failed edit leaves the
newest body pending without consuming the cap. Progress publish and edit
failures MUST NOT fail the turn and MUST NOT hold the ACK.

Implementation notes (issue 69): retryable progress create, edit, and delete
failures emit at most one operator notice per D42 edit interval. A cancelled
row keeps its delete pending until the relay accepts it; an explicit
non-retryable rejection ends that best-effort delete.

Indexing: the host indexes its own stamped kind 9 but does not fetch
its own kind 40003 edits or kind 5/9005 deletes (D24 filters), so a
progress post would otherwise appear in snapshots and ask Context
with a stale first body. Place snapshots (D19, D27) and ask Context
(D5) MUST exclude host progress post event ids. Own stamped posts
still do not trigger (D9).

Implementation notes (issue 60): the hold counts from the turn's open
time, stored as `turns.opened_at` when the ask opens; an open turn
recorded before that column existed counts from its first progress
delivery. The 1024-byte cap includes the trailing `…`, applied to the
unstamped body before the `[{bot-id}]:` stamp. The create is recorded
with its prepared event id before send (D43), so a prepared create
without an accepted id is redelivered under the same id on the next
tick and relay-deduped. A build failure or non-retryable create rejection
ends progress for that ask with one cause-specific notice.

Implementation notes (issue 64): progress create uses the shared D28/D43
prepare-record-send sequence. A recorded prepared event id is the durable
evidence that send may have been invoked; `progress_posts` no longer stores a
separate `dispatched` flag. Legacy rows whose old dispatched flag was set but
whose prepared id was absent are ended during migration and are never resent.

## D43. Host publishes over its own nostr connection

Status: accepted

Aligned 2026-09-03 between the operator's investigation session and
nostrherd-agent. The host stops shelling out to the `buzz` CLI for
writes (`buzz messages send|edit|delete`, `buzz reactions
add|remove`) and publishes through the nostr-sdk client it already
holds. A small Buzz-specific module in the domain crate owns the
event shapes — kind 9 + `h` + NIP-10 `e` root/reply + `p`; kind
40003 + `h` + `e`; kind 9005 + `h` + `e`; kind 7 + `e` — citing the
reference builders in buzz `crates/buzz-sdk/src/builders.rs`. `buzz`
remains the peer/human client in live test recipes.

Reasons: the Buzz relay accepts every kind the host publishes over
plain websocket; the shapes are tiny; the CLI's extras (@name
resolution, members-only guard, relay-side thread-root lookup, file
upload) are paths the host never uses; and in-process signing gives
the event id before send, so a retry republishes the same id and the
relay dedups — closing the mixed-attempt case D28 deferred. Publishes
ride the already NIP-42-authenticated connection instead of spawning
a connecting process per write.

Containment and guard: the OutboundPublisher/InFlightReaction seam
stays — BuzzPublisher becomes a nostr-client adapter, still the only
implementation behind existing tests. The copied shapes are pinned by
D23's relay contract; the live E2E publishes one event of each kind
(9, 40003, 9005, 7) so schema drift fails tests, not production.
Thread root comes from the trigger's own `e` tags (id-only markers
are valid); a markerless trigger falls back to a reply marker only.
Mention paths are unchanged: finals mention the trigger's effective
author (D31); tells and progress carry none.

Amends D28: `buzz messages send` is no longer the publish path; the
no-prebuilt-id paragraph and the mixed-attempt deferral are
superseded — record-before-send now includes the event id, and a
redelivered event is relay-deduped. Amends D35: the marker is
added/removed by the host client, not `buzz reactions`. Amends D42:
progress edits and deletes go through the same client, not `buzz
messages`. SPEC "Host publish" and "Inbox" are unchanged (they never
named the CLI). docs/testing.md and docs/operator-runbook.md recipes
flip buzz to peer/verification only; skills/local-relay is unchanged.

## D44. Recurring schedules live in Kelpie; the host owns restraint

Status: accepted

Amends an earlier proposal that placed the cron in the host.

A repeating schedule is a durable timer bound to a logical agent. That
is Kelpie's, not the host's: `kelpie tell --every`, with `schedules`
and `schedule-cancel`. The host MUST NOT keep its own schedule table,
firing loop, high-water mark, or missed-fire policy.

Reason: a second durable scheduler means a second implementation of
record-before-send, idempotent firing and crash recovery. Two of this
repository's own defects came from one rule written in two places, and
one of them published nothing for ninety-two minutes. One timer with
two consumers is the smaller surface.

An occupant arms its own schedule. Fixed text is a repeating
`kelpie tell nostrherd`, which D38's publish path already posts
stamped, with no `--reply-to` and no marker. A fresh status each time
is a repeating tell the occupant sends to itself, after which it tells
`nostrherd` with the result. Neither needs host code.

A schedule fires only while its target is addressable. Kelpie fails
closed and MUST NOT start, revive, or restart an agent to deliver one,
so an absent occupant misses firings rather than being resurrected.
This is deliberate: reviving on a timer is what turns a timer into a
workflow engine.

The host stores nothing about schedules. `inbox.delivery` carries no
schedule provenance, so a host mapping would be a second source of
truth that cannot be reconciled against Kelpie's.

Restraint is not the host's. How often a bot speaks is its own
judgement, written in its corpus (D52).

## D45. Author watches are host-evaluated; no model runs until a match

Status: accepted


"Online" means observable relay activity by an author. There is no
presence protocol on this stack and the host MUST NOT fake one.

A watch is evaluated in the host, against the relay stream it already
subscribes to with scoped filters and persisted cursors (D18, D24). No
occupant runs until a watch matches.

Reason: the alternative is an occupant polling its own snapshot on a
repeating schedule, which spends a language-model invocation per tick
whether or not anything happened, sees only its own channel, and
notices no sooner than its interval. Host evaluation costs nothing per
non-event, watches a person across every channel the host sees, and
fires immediately. That is what makes many watches affordable.

A watch record in host SQLite does not contradict D44. The test is
whether the other side already owns the mechanism: Kelpie owns a
durable timer, so a host schedule table would duplicate it; Kelpie
cannot see the relay, so a watch has no counterpart and the host is the
only place it can live.

A match fires a host-initiated wake: a turn opened without a person
writing a trigger, carrying a typed section so the occupant can tell a
wake from a request. That wake is shared with every other deferred
source and MUST be built once.

Firing is durable and cooldown-bounded. The host records the fire
before waking, so a crash cannot replay it, and a per-watch cooldown
keeps one person's burst from poking a bot repeatedly.

## D46. Watch v1 uses trigger phrases and retains its ledger

Status: accepted

Closes the implementation choices left open by D45.

Watch management uses exact trigger requests. Create is `watch
<pubkey[,pubkey...]>`, with optional `here`, `kind <number>`, `cooldown
<minutes>`, `expires <minutes>`, and `max <fires>` clauses. Kind is limited to
indexed channel message kinds 9 and 40002. Cancel is `cancel
watch <pubkey>` and cancels active watches for that author in the declaring
bot session. Public keys are explicit in v1; name resolution would add an
unrelated identity directory. A declaration with no explicit expiry or fire
limit defaults to one fire, and cooldown defaults to 30 minutes.

A watch always wakes the session in which it was declared, including a direct
message. An unscoped watch observes that author across subscribed channels;
`here` narrows matching to the declaring channel. The matched message remains
context, not a reply coordinate: the wake final is a top-level stamped post in
the declaring channel with no mention.

Expired, completed, and cancelled watch rows and all fire rows are retained as
the crash-safety and audit ledger. Only watches still inside their lifetime and
fire limit contribute author subscription filters. Fire insertion, fire-count
advance, and completion are one SQLite transaction. A deterministic wake Turn
id is stored with the fire before Kelpie is called, so replay cannot double-wake.

Watch activation uses the repository's existing relay cursor order,
`(created_at, event_id)`, with the declaration event as the cursor. This avoids
firing on history fetched when the new author subscription opens. It also means
an event whose signed timestamp sorts before the declaration is history rather
than new activity even if the host receives it later; this is the same durable
ordering used by ask Context and relay replay, not host arrival time.

Each declaration is an independent bounded watch. Repeating the create phrase
creates another watch; cancellation by author closes all older active watches
for that author in the declaring session. A replayed cancellation cannot close
a watch declared after it. Once a source event records a fire, later edits or
deletes do not cancel that wake.

Host-initiated wakes do not relay progress in v1. D42 requires progress to
reply to a triggering relay event, and a cross-channel watch has no such event
in the declaring channel. The final remains a top-level stamped post.

## D47. Host-initiated posts are dropped at the ceiling and in quiet hours

Status: retracted by D52

Amends D44's restraint paragraph.

A suppressed host-initiated post is dropped, never held or coalesced.
Holding would make the host a durable queue with its own firing loop,
which D44 forbids. A repeating tell's next firing carries fresh state;
a dropped wake is visible because the turn goes Failed. The operator is
told once per suppressed post on the same eprintln channel as D42
notices.

Quiet hours run on the host's local clock. The bot posts as the
operator's pubkey; the window protects the operator's day. Channels and
DMs carry no timezone.

Numbers live in `bots.toml` per bot. `post_ceiling` is the rolling
24-hour per-channel maximum (default 24, must be >= 1). `quiet_hours`
is an optional `HH:MM-HH:MM` window that may cross midnight; unset
means none. Phrase-managed overrides are not v1.

Quiet hours is evaluated before the ceiling: it is a time gate and
independent of volume. Scope is the unprompted direction only: occupant
tells (D38, including Kelpie `--due-in` / `--every` deliveries) and
host-initiated wake finals (a turn with `ask_body`, D45/D46). Trigger
finals and progress posts are exempt and never counted; progress keeps
the D42 cap. Reactions and deletes are untouched.

The ledger is SQLite `host_initiated_posts`, keyed by outbound attempt
id, recorded with INSERT OR IGNORE on relay-accepted publish so
redelivery does not double-count. Counts are per bot per channel in
    the last 86400 seconds. Enforcement is only at the publish path, and
only on a first publish: a retry of an already-prepared event is not
re-gated, because D43 redelivers that same event and a timed-out send
may already be on the relay.

## D48. Undispatched outbound is drained on the host tick

Status: accepted

A retryable publish failure does not ACK, which is right: Kelpie only
re-offers an un-ACKed delivery on reconnect. A connection that stays up
is the normal state, so the host MUST drain undispatched
`outbound_attempts` on the existing subscription-refresh tick.

The drain resends the same prepared event id so D43 dedup covers overlap
with a later reconnect. It does not ACK. After a successful drain send,
`outbound_event_id` is set; a later re-offer ACKs without sending twice.
After the retry bound the row is abandoned with an operator notice. An
open ask turn becomes `failed` so the queue can resume. Tells, scheduled
firings, and watch-wake finals share this path because they all persist
an attempt keyed by message id.

## D49. Occupant tells escape the routing marker, and refusals reach the occupant

Status: accepted

`parse_occupant_tell` treats a backslash immediately before `<nostrherd`
or `</nostrherd>` as prose, not a tag boundary. Published text unescapes
exactly those two sequences. A second real tag still refuses.

A tell the host will not publish still ACKs (the occupant is done with
that message) and MUST `kelpie tell` the occupant with the message id,
the reason, and the escape. The operator notice stays. HTML-escaping is
not an escape: the transport decodes it before the host parses.

## D50. A tell body is prose; the routing tag is retired

Status: accepted

Amends D38 and retires D49's escape. `parse_occupant_tell` publishes
the whole trimmed body and reads no markup. A tell reaches that
occupant's channel and no other.

Reason: the tag never routed a message. No corpus documented it, the
`bot-conduct` guidance never mentioned it, and no occupant was ever
told it existed, so its entire effect in production was one silently
dropped post when ordinary prose happened to contain the marker. The
escape added in D49 fixed that failure by adding a second rule to a
feature with no users.

Cross-channel posting stays a real want and comes back with its first
real caller, when the syntax can be chosen against a case instead of
guessed. Until then a bot speaks where it lives, which is what every
tell has done so far.

The refusal path from D49 stays: a tell the host will not publish still
ACKs and still tells the occupant why. The only refusal left is an
empty body.

## D51. The correlated path is Kelpie's; a self-published post never answers

Status: accepted

Amends D31, which made the host the only Nostr publisher. That was a
custody rule wearing a protocol rule's clothes. An occupant with a
shell on the operator's machine can already reach the operator's key
through the same wrapper the host uses, so the blanket ban never
bounded a hostile occupant, only an accidental one. What it did bound
was every ordinary use a bot might have for the relay: a reaction, a
profile read, a non-chat kind, history past the snapshot window.

An occupant whose corpus grants relay access MAY publish directly. Key
custody is unchanged in substance: use through a wrapper, never the
value, never printed, logged, or committed.

Two rules make the parallel path safe, and neither is enforceable by
the host.

A self-published body MUST begin `[{bot-id}]:`. A trigger matches only
when the first token is exactly `{bot-id}:`, so the stamp is what keeps
a bot from reading its own post back as a request. Without it a post
that merely starts with the trigger token loops.

A self-published post MUST NOT answer a trigger ask or carry progress.
The host holds the Turn keyed to the trigger EventId. An out-of-band
answer leaves that Turn open: the `⏳` never clears, the reminder
fires, and the next question queues behind a Turn that will never
close. The correlated path exists to reuse Kelpie's correlation,
reminders and recovery, which is the reason the host is in the middle
at all.

Restraint reached only the host's own publish path, never the parallel
one, which is what led to removing it from the host altogether (D52).

No `nostrherd compose` command and no signer proxy. Both were considered
and both invent a convention before a caller needs one, which is what
the retired routing tag (D50) cost once already. The formatting
contract is small enough to state: kind 9, the channel `h` tag, and
the stamp.

## D52. Restraint leaves the host; how often a bot speaks is its own judgement

Status: accepted

Retracts D47 and the restraint paragraph of D44. The per-bot,
per-channel ceiling on host-initiated posts, the quiet-hours window,
the `post_ceiling` and `quiet_hours` config, and the
`host_initiated_posts` ledger are removed. `crates/domain/restraint.rs`
is deleted.

D44 argued restraint had to ship with the first recurring capability
because an occupant is a language model that will sometimes decide to
post more than a person wants, with D42's edit cap as the precedent.
That reasoning held while the host was the only publisher. D51 ended
that: an occupant whose corpus grants relay access publishes for
itself, and a ceiling enforced only where the host publishes is one an
agent steps around without noticing it existed. A limit that binds one
path out of two is worse than none, because it reads as a guarantee.

D42's cap is not the same case and stays. It bounds edits to a single
post the host owns and is republishing, which no occupant can do for
itself.

How often a bot should speak, and when it should stay quiet, is
judgement about that bot rather than a property of the protocol. It
belongs in the corpus, next to its personality and its scope, where an
author can say it in a sentence and change it without a release.

The host keeps no counters and no clock. Nothing in `bots.toml`
configures speech. An existing database keeps its now-unused
`host_initiated_posts` table; nothing reads or writes it.

## D53. The host's two environment names carry its own prefix

Status: accepted

Amends D29's variable names. The host reads `NOSTRHERD_PRIVATE_KEY`
and `NOSTRHERD_RELAY_URL`; `BUZZ_PRIVATE_KEY` and `BUZZ_RELAY_URL` are
retired with no fallback.

The old names came from the host being written against Buzz. It runs
against any NIP-29 relay, proved against `groups_relay` as well as
Buzz, so a reader who is told that in the first paragraph and then
types `BUZZ_` twice is being told two different things.

No compatibility shim. An existing namespace needs both new names set
before the next start, and the host fails closed with a message naming
the variable it could not read.

`BUZZ_AUTH_TAG` in the test recipes belongs to the `buzz` CLI those
recipes drive, not to this host, and keeps its name.

## D54. Channel IDs are opaque; only display collisions need a suffix

Status: accepted

Amends D10's UUID assumption (issue 80). A channel ID is an arbitrary
string, stored and compared unchanged. A new session first tries the
readable bot-plus-display name; only a collision derives an ID suffix.

On that path, UUID-shaped IDs retain the existing lowercase hex
compaction so occupant bindings and place-history names do not change.
Other IDs use inline 64-bit FNV-1a over their UTF-8 bytes, formatted as
16 lowercase hex digits including leading zeros. The existing candidate
loop starts with up to eight digits, lengthens by four when taken, and
caps the suffix at the available room within Herdr's 32-character limit.
This is a display-name tiebreak, not a collision-resistant identity.
Stored session names remain unchanged. The domain gains no dependency.

## D55. The host writes the occupant contract; corpus authors write personality

Status: accepted (issue 79)

On snapshot refresh before occupant start or a new turn, the host upserts a
`nostrherd-contract` marker block alongside the snapshot block in `startup.md`.
The contract covers final and progress replies, bot-initiated and scheduled
tells, the host stamp, D51's direct-publish boundary, key custody and untrusted
context. Bootstrap and renew direct the occupant to read `startup.md`.

The host changes only its marked blocks and appends missing blocks. Malformed,
duplicate or overlapping markers fail without rewriting the startup file.
`AGENTS.md` and text outside host markers remain author-owned.

The block names the running installation's `skills/bot-conduct/SKILL.md` as a
file to read, not a harness skill to load. The runtime resolves it beside the
executable, or in the source checkout above its Cargo `target/` directory.
Relocated binaries ship that relative skills path beside the executable.
No build-time absolute path or corpus copy is used. Missing or unreadable advice
fails host startup before Kelpie or relay connection. Actors receive the resolved
path explicitly; tests can supply a synthetic installation independent of the
Cargo output directory. Updated startup files use a synced temporary file and
atomic rename, preserving author text if a write is interrupted.
Bot-specific handwritten advice can override shared advice, not the contract.

`corpus/template-bot/` is the creation template; `corpus/example-bot/` remains
a throwaway proof fixture. Removing copied protocol from existing live corpora
is a separate operator-owned rollout, never an automatic host migration.
Contract changes are not announced; the block regenerates on refresh and
occupants read it on startup or renew. No announcement mechanism is added.
## D56. Shared corpus instructions; channel-local continuation state

Status: accepted

Amends D27 and D55. Occupants sharing a corpus keep the same channel-neutral
root `startup.md` and fixed corpus instructions. Channel work and continuation
instructions MUST NOT be written into that shared startup file.

The renew checkpoint is `.nostrherd/sessions/<session-name>/progress.md`.
The key is the existing stored, filesystem-safe session name (D10, D54), not
a channel ID, pane, incarnation or ask id. The host creates the directory;
the occupant writes the checkpoint. Prepare and resume prompts name that exact
path, and resume also names the existing `.nostrherd/places/<session-name>.md`
snapshot. No additional continuation file is needed. All channel-specific
occupant state belongs under that session directory. Keep `.nostrherd/` ignored
in corpus git, as the creation template already does.

Root `progress.md` and other sessions' checkpoints MUST NOT be read or written
for continuation. Existing shared checkpoints are not migrated automatically:
their channel ownership is ambiguous. Existing renew policies receive the new
prompts when armed again; this change does not mutate running policies or
restart occupants.
## D57. Operator-only default and host-stamped requester identity

Status: accepted (issue 87)

Amends D5, D9, D11, D34 and D55. Each bot has an `allowed_requesters` list
of full hex public keys or npubs. Absent or empty means operator-only. The
operator remains authorized without a self mention. An additional requester
must be allowlisted for that bot and p-tag the operator. The indexed effective
author is the identity, including trusted-relay attribution, not the raw signer.
For a configured trusted relay, D23's legacy first-p-tag fallback is an author
assertion, not a mention: an operator assertion therefore receives `self:`.
This trusts that relay to assert authorship, just as edit/delete ownership does;
arbitrary event signers cannot supply such attribution. Without a configured
trusted relay key, only the event signer supplies the effective author.

The host prefixes the trigger request with `self: ` for the operator or
`[<full npub>]: ` for an allowlisted author. Kelpie's envelope is unchanged:
`from=nostrherd` remains the waiter. Request text and channel Context cannot
override the initial prefix. Watch wakes keep their typed body, without a
requester stamp.

Trigger turns store the author public key and request as JSON in `ask_body`;
`publish_reply_to_event_id` distinguishes them from typed host wakes. The
existing reply coordinates, dedup ledger and host-wake schema are unchanged.
Edits retain the original requester. Dispatch rechecks the current allowlist.
Legacy queued turns reconstruct identity from the indexed author and text from
the latest indexed body; missing identity or denied authorization cancels them
without occupant start. Already-open asks keep their existing lifecycle.

The allowlist enforces who may wake a bot. Non-self conduct is guidance, not a
sandbox: answer questions, do not write, do not read outside the bot's working
repositories, and do not disclose private information. Generated corpus conduct
distinguishes that remote requester from the operator working in the pane and
from a host-stamped `self:` request. Init implementation is separate.

## D58. `nostrherd init` scaffolds a corpus from templates in the binary

Status: accepted

The host scaffolds a bot corpus with `nostrherd init <dir> --id <id> --kind
<kind>`. The templates are the files under `corpus/template-bot/`, embedded
with `include_str!`, so a released binary scaffolds with no checkout and no
network, and the shipped templates cannot drift from the binary that writes
them. `corpus/template-bot/` stays the single source; there is no second set
of string literals and no separate example repository to keep in sync.

`init` refuses a destination that already holds entries, so an existing corpus
is never overwritten. It validates the id with `BotId` and rejects an empty
kind before touching the disk. It runs `git init` in the new directory and
reports failure rather than continuing silently.

Registration is amended by D59: `init` now writes the entry into the
conventional registry itself, and `--print-only` restores the stdout entry
and stderr-everything-else split this paragraph described.
Missing `--id` or `--kind` are prompted for on a terminal and
are a hard error without one, so scripted use fails loudly instead of blocking
on an invisible prompt.

`init` needs no database, credentials, Herdr or Kelpie. `--config` and
`--database` are optional at the parser; D59 makes them optional on the run
path too, where they became overrides for conventional paths.

The generated `AGENTS.md` tells the occupant to read `startup.md` on every
Kelpie ask. The host writes its contract there (D55), but nothing auto-loads
that file, while agent CLIs load `AGENTS.md` every turn and the host never
writes `AGENTS.md`. Without that pointer an occupant answers in prose and never
calls `kelpie reply`, so the reply is never published.

## D59. Registry and database live at conventional XDG paths; `init` registers

Status: accepted

The host resolves its registry and database by convention, so neither is a
flag the operator must remember:

- registry `$XDG_CONFIG_HOME/nostrherd/bots.toml`
- database `$XDG_DATA_HOME/nostrherd/nostrherd.sqlite`

An unset or empty XDG variable falls back to `$HOME/.config` and
`$HOME/.local/share`. This is exactly how Kelpie resolves its own paths, so
the tools agree on where a user's configuration lives. There is no
`cfg(target_os)` branch: macOS lands on `~/.config`, not on
`~/Library/Application Support`. Missing `HOME` with no XDG variable is a
hard error naming both. The host creates the database directory on first run;
SQLite will not.

`--config` and `--database` remain, as overrides for a second host or a
throwaway test. They are no longer required, and `MissingHostArgs` is gone.

`nostrherd init` resolves the same registry, checks the new bot against it,
and appends the entry itself. One command produces a registered, working bot.
It refuses an id already registered and a corpus another bot already uses,
both before writing any file, so a rejected bot leaves no half-made corpus.
The shared-corpus refusal is not stylistic: the host stamps the contract block
in `<corpus>/startup.md` with one bot id (D55), so two bots in one directory
overwrite each other's contract on every turn. Occupants of the *same* bot
still share one corpus (D7, D27); that is unaffected.

`--print-only` restores the stdout entry for scripting.

Supersedes D58's `>> bots.toml` registration and its "optional at the parser,
required for the run path" rule for `--config` and `--database`.

## D60. Setup guidance exposes the next step without claiming readiness

Status: accepted

`init` still needs no credentials or running services (D58). After registration
it names only missing host environment settings, never their values, then shows
the config-aware check/start commands and the operator's first trigger. Herdr
and `kelpied` are reminders, not claims of reachability. `--print-only` makes
registration a prerequisite to those commands and keeps all guidance on stderr.
The scaffold README holds the optional `allowed_requesters` instructions;
the short init output points there. Actionable setup errors name a next step.
