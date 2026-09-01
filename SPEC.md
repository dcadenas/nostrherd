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

`botcli` MUST stamp the outbound prefix. It MUST take host coordinates
for the in-flight turn (channel, reply-to event, mention, ask id), not
only a destination pubkey. After an accepted relay publish it MUST
resolve the Kelpie ask as the owing occupant. Cancel MUST NOT be used
for a successful post.

## Persistence

SQLite is the host store. The relay is the store of messages. Corpus
git is the store of bot personality. SQLite MUST NOT store nsecs.

## Secrets

Operator keys enter `botcli` via envchain (or equivalent exec). They
MUST NOT appear in process titles, sqlite, or logs.
