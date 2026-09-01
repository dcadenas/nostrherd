# SPEC.md

Normative contract for `botserver` / `botcli`.
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
6. Occupant publishes with `botcli`. On success `botcli` MUST
   `kelpie reply --final` as that occupant.
7. Persist host state (sessions, turns, processed events) in SQLite.
8. Bound occupant context with Kelpie renew (wall-clock). Durable
   context lives in files, not only in the model.

## Non-goals

- Speak Buzz ACP or replace `herdr-acp`.
- Give the occupant the operator nsec.
- Multiplex two bots onto one session name.
- Token-count renew (v1 is time).
- A pane-less waiter (Kelpie forbids it).
- Parsing relay markup as Kelpie `from=`.

## System overview

```text
relay  ->  botserver (host occupant, waiter)
             per-bot actor
               sqlite  (processed events, sessions, turns)
               corpus repo path
               kelpie start|ask  bot-<place>
                    -> occupant
                         botcli publish + kelpie reply --final
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

## botcli

`botcli` is a Nostr **send** tool for Herdr occupants, not a Kelpie
client. It MUST NOT expose ask/tell/reply as its user-facing verbs.
The occupant command is `send`.

The post body MUST come from `--stdin` or `--file`, never from a
shell-expanded argument. Occupants SHOULD use a quoted heredoc
(`<<'EOF'`). `--body` is forbidden for agent-generated text.

Host coordinates (channel, reply-to event, mention, in-flight Kelpie
ask id) are flags, not the body. `botcli` MUST stamp `[bot]:` onto the
body after reading it.

Stdout defaults to JSON: a receipt (`event_id`, and ask id if a Kelpie
ask was closed). That is the CLI result, not the channel message.
Errors are JSON on stderr. A human `--text` receipt MAY be added later;
it MUST NOT be the default.

After an accepted relay publish it MUST `kelpie reply --final` as the
owing occupant when an ask id was supplied. That is plumbing so the
host obligation closes. Cancel MUST NOT be used for a successful post.
`botcli` MUST NOT publish if that ask is already cancelled.

## Persistence

SQLite is the host store. The relay is the store of messages. Corpus
git is the store of bot personality. SQLite MUST NOT store nsecs.

## Secrets

Operator keys enter `botcli` via envchain (or equivalent exec). They
MUST NOT appear in process titles, sqlite, or logs.

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
    After it posted: leave `[bot]:` up. A late `botcli` on a cancelled
    ask MUST NOT publish.
11. **Long work.** One stamped reply when done. No working ping in v1.
12. **Desktop.** Buzz desktop is still Daniel. The host does not mark
    him typing or rewrite his presence.

## Issue work

An issue MUST list `Depends on:` and `Contract baseline:` (commit SHA
plus D-numbers). Before starting: update from `origin/master`; re-read
`SPEC.md` and `docs/decision-log.md` at that HEAD; if HEAD ≠ baseline,
**edit the issue body** (scope, acceptance, deps, new baseline). A
comment is not enough. Do not start while a listed dependency is open.
