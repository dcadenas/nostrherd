# SPEC.md

Normative contract for `botserver`. The leftover `botcli` crate is not
the occupant publish path.
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
2. Index events for context. Wake a bot session only on a **trigger**.
3. One in-process **actor per configured bot**. Each bot has a corpus
   git repo (home, `AGENTS.md`, `startup.md`, skill).
4. Route a trigger to a session occupant named from bot + place
   (channel, DM, …). Start or reuse via Kelpie.
5. Inject a Kelpie **ask** whose waiter is `botserver`. Body carries
   escaped Nostr text. `from=` MUST be `botserver`, never a relay pubkey.
6. Occupant answers with `kelpie reply --final` and unstamped prose.
   The host is the only Nostr publisher: it stamps `[bot]:`, posts from
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
relay  ->  botserver (reconnecting inbox client, waiter)
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

A `Turn` opens only on a **trigger** (D8, D9). No occupant is created
until the first trigger for that place. Ordinary channel traffic, thread
replies without the prefix, and `@daniel` without `bot:` MUST NOT start
or poke a session.

Session grain is one occupant per Buzz channel UUID, including DMs
(D10). Thread ids are reply coordinates on the turn, not extra sessions.

## Kelpie envelope (receiver)

Asks MUST render as Kelpie does today: unquoted attributes, `msg=` and
`reply-to=` both the ask id.

```text
<kelpie from=botserver msg=<ask-id> reply-to=<ask-id>>
escaped nostr body
</kelpie>
```

Tells MUST NOT be used for triggered channel work: they create no
obligation or reminder.

`from=botserver` is the waiter public name, not a pane and not a relay
pubkey (D2).

## Occupant reply

The occupant is an ordinary Kelpie peer of waiter `botserver`. Snapshot
and renew stay. It MUST answer a trigger ask with `kelpie reply --final`
and unstamped prose. The final body MUST come from `--stdin` or
`--file`, never from a shell-expanded argument. It MUST NOT stamp
`[bot]:`, MUST NOT call the relay, and MUST NOT receive the operator
nsec. Cancel MUST NOT be used for a successful answer.

The occupant self-renews. The host MUST NOT arm occupant renew with
`--sender-id` of waiter `botserver`, so this inbox only sees channel
asks the host created (D32).

## Host publish

The host is the only Nostr publisher (D31). On an accepted occupant
final it MUST stamp `[bot]:`, post from sqlite coordinates, then
`inbox.ack`. Occupants never get the operator nsec.

Outbound `--reply-to` is the triggering EventId, including the first
call. Keep the trigger's existing parent separately when snapshots need
thread-root context.

The host MUST `--mention` the indexed event's effective author (not the
raw relay signer, not an arbitrary `p` tag), including operator-authored
triggers. `ignore_self` still blocks retrigger.

The leftover `botcli` crate is not the occupant path (D4/D22 retracted).
It remains until a later removal issue.

Crash-safe outbox (durable outbound attempt, same event id on retry) is
a later issue.

## Inbox

Keep the claimed connection and the reply body. ACK only after the host
decides. Do not ACK in the drain thread before the body is durable
(D33).

Classify by `reply_to` in the host's Turn ids:

- Empty or whitespace-only final: do not ACK if a later valid final
  should still be allowed.
- Progress: ACK, do not publish.
- Cancelled turn: ACK, do not publish.
- Already posted: ACK.
- Unknown `reply_to`: do not publish.

## Persistence

SQLite is the host store. The relay is the store of messages. Corpus
git is the store of bot personality. SQLite MUST NOT store nsecs.

## Secrets

Operator keys enter the host process via an outer wrapper
(`envchain NAMESPACE botserver …`). Occupants MUST NOT receive the
nsec. The host MUST read `BUZZ_PRIVATE_KEY` and `BUZZ_RELAY_URL` when
set. Binaries MUST NOT take `--envchain` and MUST NOT exec `envchain`.
Keys MUST NOT appear in process titles, sqlite, logs, or standing
pane-env.

## User-visible flows (v1)

These are the product. Implementation MUST match this, not a clever
subset.

1. **Silence.** Nobody writes `@daniel bot:` in a channel. No occupant
   exists there. The host may index events. Nothing is posted.
2. **First call.** Sebastian in `#foobar` writes `@daniel bot: hello`.
   Occupant `bot-foobar` is created from the bot corpus. It replies in
   that thread: `[bot]: …`.
3. **Follow-up without prefix.** Sebastian's next line is `and the PR?`
   with no `bot:`. The occupant is not poked. Daniel may answer as
   himself.
4. **Second call.** Later, anyone (including Daniel) writes
   `@daniel bot: …` in the same channel. Same occupant gets a new ask,
   reply-to that event. If the previous turn is still open, this one
   waits.
5. **Another channel.** `@daniel bot:` in `#eng` is `bot-eng`,
   independent of `#foobar`.
6. **DM.** Same as a channel: first trigger creates `bot-<dm-slug>`,
   further unprefixed DM lines do not poke it.
7. **Thread.** `@daniel bot:` in a foobar thread still uses
   `bot-foobar` and replies into that thread.
8. **Busy.** Two `@daniel bot:` in `#foobar` before the first reply:
   one occupant, two turns in order.
9. **Gone pane.** Occupant process died with an open ask: recover that
   logical agent, do not start a namesake twin. The user still gets at
   most one `[bot]:` for that call.
10. **Edit / delete.** Edit of the triggering message before the bot
    posts: the one eventual `[bot]:` answers the **latest** text
    (cancel the old ask, ask again). Delete before it posts: no post.
    After it posted: leave `[bot]:` up. A late occupant final on a
    cancelled ask MUST NOT publish (I10, host).
11. **Long work.** One stamped reply when done. No working ping in v1.
12. **Desktop.** Buzz desktop is still Daniel. The host does not mark
    him typing or rewrite his presence.

## Issue work

An issue MUST list `Depends on:` and `Contract baseline:` (commit SHA
plus D-numbers). Before starting: update from `origin/master`; re-read
`SPEC.md` and `docs/decision-log.md` at that HEAD; if HEAD ≠ baseline,
**edit the issue body** (scope, acceptance, deps, new baseline). A
comment is not enough. Do not start while a listed dependency is open.
