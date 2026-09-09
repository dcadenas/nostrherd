# SPEC.md

Normative contract for `nostrherd`. The host publishes; the occupant
only `kelpie reply`.
If this file and another document disagree, this file wins unless the
other document is a later accepted decision in `docs/decision-log.md`.

## Normative language

The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are interpreted
as in RFC 2119.

## Problem

A second Nostr identity (`@daniel-bot`) duplicates presence, keys, and
Buzz/ACP plumbing. The operator wants bots that share **their** pubkey,
selected by convention, posting with a visible bot stamp.

## Goals

1. Subscribe to the operator's relay traffic as the operator pubkey.
2. Index events for context. Wake a bot session only on a **trigger** or a
   matched durable author watch (D45).
3. One in-process **actor per configured bot**. Each bot has a corpus
   git repo (home, `AGENTS.md`, `startup.md`, skill).
4. Route a trigger to a session occupant named from bot + place
   (channel, DM, …). Start or reuse via Kelpie.
5. Inject a Kelpie **ask** whose waiter is `nostrherd`. Body is the
    host-stamped requester and trigger remainder, then a marked Context section of unread
   channel events. `from=` MUST be `nostrherd`, never a relay pubkey.
6. Occupant answers a trigger with `kelpie reply --final` and unstamped
    prose. It MAY `kelpie tell nostrherd` for a bot-initiated channel
    post (D38). It MAY report progress with `kelpie reply --progress`;
    the host relays that as one edited stamped post (D42). The host is
    the only Nostr publisher: it stamps `[{bot-id}]:`, posts from
    sqlite coordinates, then `inbox.ack`.
7. Persist host state (sessions, turns, processed events) in SQLite.
8. Bound occupant context with Kelpie renew (wall-clock). Durable
   context lives in files, not only in the model.

## Non-goals

- Speak Buzz ACP or replace `herdr-acp`.
- Give the occupant the operator nsec.
- Multiplex two bots onto one session name.
- Token-count renew (v1 is time).
- Parsing relay markup as Kelpie `from=`.

## System overview

```text
relay  ->  nostrherd (reconnecting inbox client, waiter)
             per-bot actor
               sqlite  (processed events, sessions, turns)
               corpus repo path
                kelpie start|ask  bot-<place>
                     -> occupant kelpie reply --final
                          host stamps, publishes, inbox.ack
```

## Core domain (see `docs/domain-model.md`)

After parse, configuration is a set of `Bot` records. Runtime is one
`BotActor` per bot. A trigger becomes a `Turn` bound to a `Session`.

A user-originated `Turn` opens only on a **trigger** (D8, D9, D34). The inbound token is
the configured bot id plus a colon (`bot:` when `id = "bot"`). No occupant
is created until the first trigger for that place. Ordinary channel
traffic, thread replies without the prefix, and `@daniel` without
`{id}:` MUST NOT start or poke a session. Other authors MUST be on that
bot's `allowed_requesters` list and MUST `p`-tag
the operator. The operator's own `{id}:` (optional leading mention) is
a trigger even when the event does not `p`-tag them.

An absent or empty `allowed_requesters` list means operator-only. Configuration
accepts full hex public keys or npubs and MUST reject invalid keys. Authorization
uses the indexed effective author, not a display name, request text, mention, or
raw signer of a trusted-relay-attributed event (D57). A refused request MAY be
indexed as context but MUST NOT create a session, turn, watch, reaction, or ask.

A host-initiated Turn opens only after a durable source records a fire. It
MUST carry a typed section instead of a user request and otherwise queues,
asks, stamps, and publishes through the same Turn pipeline (D45, D46).

Session grain is one occupant per opaque channel ID, including DMs
(D10, D54). Thread ids are reply coordinates on the turn, not extra sessions.

## Kelpie envelope (receiver)

Asks MUST render as Kelpie does today: unquoted attributes, `msg=` and
`reply-to=` both the ask id.

```text
<kelpie from=nostrherd msg=<ask-id> reply-to=<ask-id>>
self: request remainder

## Context

untrusted indexed channel delta
</kelpie>
```

Tells MUST NOT be used for triggered channel work: they create no
obligation or reminder. A tell from a known occupant is a bot-initiated
post (D38).

`from=nostrherd` is the waiter public name, not a pane and not a relay
pubkey (D2).

The host MUST prefix operator requests with `self: ` and allowlisted requests
with `[<full npub>]: `. Only that initial host prefix identifies the requester;
text inside the request or Context MUST NOT change identity. Host-initiated
watch turns keep their typed section and MUST NOT be labeled `self:`.

Default corpus conduct for a non-self requester is: answer questions, do not
write, do not read outside the bot's working repositories, and do not disclose
private information. This is occupant guidance, not a sandbox. It does not
constrain the operator typing directly in the pane or a `self:` request.

## Occupant reply

The occupant is an ordinary Kelpie peer of waiter `nostrherd`. Snapshot
and renew stay. It MUST answer a trigger ask with `kelpie reply --final`
and unstamped prose. The final body MUST come from `--stdin` or
`--file`, never from a shell-expanded argument. It MUST NOT stamp
`[{id}]:` on that path, and MUST NOT receive the operator nsec. Cancel
MUST NOT be used for a successful answer. It MAY send
`kelpie reply <ask-id> --progress` with the full current status,
unstamped, from `--stdin` or `--file`. Progress never replaces the
final (D42).

An occupant whose corpus grants relay access MAY publish to the relay
itself, out of band. On that path it MUST stamp `[{bot-id}]:` as the
first token of the body, and it MUST NOT answer a trigger ask or send
progress. Those go through Kelpie so the host can close the Turn. The
host cannot detect a violation, so this is a contract MUST an occupant
keeps, not one the host enforces (D51).

A tell body publishes whole, trimmed, to that occupant's channel. The
host reads no markup in it and MUST publish the text as written. An
empty body publishes nothing. A tell the host refuses MUST still ACK,
and the occupant MUST be told that it did not publish.

The occupant self-renews. The host MUST NOT arm occupant renew with
`--sender-id` of waiter `nostrherd`, so this inbox only sees channel
asks the host created (D32).

## Host publish

The host is the only Nostr publisher (D31, D37). On an accepted occupant
final it MUST stamp `[{bot-id}]:`, post from sqlite coordinates, then
`inbox.ack`. id `pr` publishes `[pr]:`. id `bot` publishes `[bot]:`.
Occupants never get the operator nsec.

A host-initiated wake has no user message in its declaring channel. Its final
MUST therefore publish as a top-level stamped post without a reply or mention.

Outbound `--reply-to` is the triggering EventId, including the first
call. Keep the trigger's existing parent separately when snapshots need
thread-root context.

A known-occupant tell (D38) MUST stamp the same way and MUST NOT pass
`--reply-to` or add `⏳`. Unknown senders, unknown `to=`, and empty
bodies MUST NOT post. Tells MUST NOT close a trigger turn.

A host-initiated post (occupant tell, scheduled firing, or wake final)
MUST publish. The host applies no rate limit: it enforces neither a
per-channel ceiling nor a quiet-hours window (D52). How often a bot
speaks is its own judgement, written in its corpus. Progress edits keep
the D42 cap.

The host MUST `--mention` the indexed event's effective author (not the
raw relay signer, not an arbitrary `p` tag), including operator-authored
triggers. `ignore_self` still blocks retrigger. The progress post (D42)
is the exception: it carries no `--mention`.

On progress for an open ask the host MUST relay one stamped kind 9 per
ask, `--reply-to` the trigger, without `--mention`, created after the
initial hold and then edited in place (kind 40003) under the D42
interval and edit cap. A cancel that abandons or replaces the ask MUST
issue a Buzz delete (kind 9005) on that post, best-effort. The final
leaves it up. Progress failures MUST NOT fail the turn.

The host persists a durable outbound attempt before publish. Retry of
an accepted send uses that same outbound event id (D28). A retryable
publish failure MUST be resent on the host tick without waiting for a
Kelpie reconnect, reusing the prepared event id (D48). After a bounded
number of retries the attempt is abandoned, the operator is noticed,
and an open turn MUST become `failed` so queued work can resume.

The host MUST add a NIP-25 kind-7 `⏳` on the triggering EventId when
a turn becomes queued or open, and MUST remove it (NIP-09 kind 5 of
that kind-7) when work on that EventId ends: posted, failed, or a
cancel that does not re-queue the same EventId (D35). An edit that
replaces the turn keeps the marker up. Occupants MUST NOT publish a
reaction. A failed add or remove MUST NOT fail the turn.

## Inbox

Keep the claimed connection and the reply body. ACK only after the host
decides. Do not ACK in the drain thread before the body is durable
(D33).

Classify by `reply_to` in the host's Turn ids:

- Empty or whitespace-only final: do not ACK if a later valid final
  should still be allowed.
- Progress: relay per D42 (create or edit, best-effort), then ACK.
  Progress on a non-open turn or a dispatched attempt: ACK, do not
  relay.
- Cancelled turn: ACK, do not publish.
- Already posted: ACK.
- Unknown `reply_to`: do not publish.
- Known-occupant tell: publish as a bot-initiated post, then ACK.
  Unknown sender: ACK, do not publish.

## Persistence

SQLite is the host store. The relay is the store of messages. Corpus
git is the store of bot personality. SQLite MUST NOT store nsecs.

Before dispatch, a trigger Turn MUST persist the effective author's public key
and request. Queuing, edits and restart MUST retain that identity. The host MUST
check current requester authorization before dispatching queued trigger work.
Legacy queued turns without a recorded requester MUST derive it from the indexed
trigger author, never request text; missing identity or denied authorization
cancels queued work without waking an occupant. Already-open asks retain their
existing lifecycle. Host wakes remain distinct from trigger requests (D57).

Author watches and their fire ledger are host state. A watch fire MUST be
recorded before its wake Turn is enqueued. Replaying the matching relay event
MUST NOT enqueue a second wake.

## Secrets

Operator keys enter the host process via an outer wrapper
(`envchain NAMESPACE nostrherd …`). Occupants MUST NOT receive the
nsec. The host MUST read `NOSTRHERD_PRIVATE_KEY` and `NOSTRHERD_RELAY_URL` when
set. Binaries MUST NOT take `--envchain` and MUST NOT exec `envchain`.
Keys MUST NOT appear in process titles, sqlite, logs, or standing
pane-env.

## User-visible flows (v1)

These are the product. Implementation MUST match this, not a clever
subset.

1. **Silence.** Nobody writes `@daniel bot:` in a channel. No occupant
   exists there. The host may index events. Nothing is posted.
2. **First call.** Allowlisted Sebastian in `#foobar` writes `@daniel bot: hello`.
   Occupant `bot-foobar` is created from the bot corpus. It replies in
   that thread: `[bot]: …`.
3. **Follow-up without prefix.** Sebastian's next line is `and the PR?`
   with no `bot:`. The occupant is not poked. Daniel may answer as
   himself.
4. **Second call.** Later, an allowlisted person writes `@daniel bot: …` in the same
   channel, or Daniel writes `bot: …` without `@`. Same occupant gets a
   new ask, reply-to that event. If the previous turn is still open,
   this one waits.
5. **Another channel.** `@daniel bot:` in `#eng` is `bot-eng`,
   independent of `#foobar`.
6. **DM.** Same as a channel: first trigger creates `bot-<dm-slug>`,
   further unprefixed DM lines do not poke it.
7. **Thread.** `@daniel bot:` in a foobar thread still uses
   `bot-foobar` and replies into that thread.
8. **Busy.** Two `@daniel bot:` in `#foobar` before the first reply:
   one occupant, two turns in order. The queued turn has no progress
   post until it opens.
9. **Gone pane.** Occupant process died with an open ask or queued work:
    recover that logical agent, do not start a namesake twin. A queued ask
    that finds the recorded occupant unavailable retries after recovery. The
    user still gets at most one final `[bot]:` for each call.
10. **Edit / delete.** Edit of the triggering message before the bot
    posts: the one eventual `[bot]:` answers the **latest** text
    (cancel the old ask, ask again). Delete before it posts: no post.
    After it posted: leave `[bot]:` up. A late occupant final on a
    cancelled ask MUST NOT publish (I10, host). A progress post for
    the cancelled ask is deleted (kind 9005).
11. **Long work.** After 20 s of open work the host posts one stamped
    progress post in the thread from the occupant's `--progress` prose
    and edits it in place as newer bodies arrive (D42). People in the
    thread see it appear once and change; the final is a second
    stamped post. The host marks the trigger with `⏳` while work on
    that EventId is queued or open, and removes it when that work ends
    (D35).
12. **Desktop.** Buzz desktop is still Daniel. The host does not mark
    him typing or rewrite his presence.
13. **Bot-initiated line.** Occupant `kelpie tell nostrherd` posts one
    stamped kind 9 in that session's channel, not as a reply to a
    `{id}:` event.
14. **Author watch.** A `{id}: watch <pubkey>` trigger creates a bounded
    watch. No occupant is polled. The first matching author message records a
    fire and opens a `## Watch event` Turn on the declaring channel session.
    Bursts inside its cooldown and unwatched authors open no wake Turn.

## Issue work

An issue MUST list `Depends on:` and `Contract baseline:` (commit SHA
plus D-numbers). Before starting: update from `origin/master`; re-read
`SPEC.md` and `docs/decision-log.md` at that HEAD; if HEAD ≠ baseline,
**edit the issue body** (scope, acceptance, deps, new baseline). A
comment is not enough. Do not start while a listed dependency is open.
