# botserver

Private host for personal Nostr bots that post **as you**, with a
channel convention (`{bot-id}: …` in, `[{bot-id}]:` out; id `bot` →
`bot:` / `[bot]:`), driven through Herdr and Kelpie.

This is not an ACP child and not a Buzz managed-agent. The daemon watches
the relay as your pubkey, wakes a corpus occupant per bot+channel, and
publishes occupant `kelpie reply --final` (trigger answers) and occupant
`kelpie tell` (bot-initiated posts) as `[{bot-id}]:`.

## Binaries

- `botserver` — host process (Kelpie waiter `botserver`) plus per-bot actors

The occupant answers a trigger with `kelpie reply --final` and unstamped
prose, and MAY `kelpie tell botserver` for a bot-initiated post (D38).
The host is the only Nostr publisher (D31). Occupants MUST NOT receive
the operator nsec.

```bash
envchain botserver-proof botserver \
  --config /path/to/bots.toml \
  --database /path/to/botserver.sqlite
```

`envchain` wraps the process. `botserver` only reads `BUZZ_PRIVATE_KEY`
and `BUZZ_RELAY_URL` from the environment (D29). There is no
`--envchain` flag.

The command above uses throwaway live-test namespace `botserver-proof`.
Personal operator keys use namespace `botserver`. Before running the
host as yourself, read `docs/operator-runbook.md`.

## What a bot can do

Everything below is what someone types in a channel and what the bot
does about it. `bot:` is the trigger for a bot with id `bot`; a second
bot with id `pr` answers `pr:` and stamps `[pr]:`.

| What you type | What happens |
| --- | --- |
| `bot: how is the deploy looking?` | Answers in that thread, stamped `[bot]:` |
| `pr: status of 123` | A different bot answers. The prefix picks the bot |
| `how is it going?` | Nothing. A bot stays silent unless addressed |
| `bot: in 10 minutes give me the status of pr 123` | Confirms now, posts the answer ten minutes later on its own |
| `bot: every 5 minutes report the queue` | Posts on that interval until cancelled |
| `bot: <long task>` | Posts a progress note, edits that same post as work advances, then posts the answer |
| *edit your message* | Answers your new text and discards the old request |
| *delete your message* | Abandons unposted work. An answer already sent stays up |
| `bot: <second question while busy>` | Queues, answered after the first |
| the same bot in another channel | A separate conversation with its own memory, seven days of history |
| a direct message | Works like any channel |

A bot can also post with no question asked, which is how the deferred
and recurring rows above are delivered.

### How those are built

Four primitives, composed:

- **Answer**: `kelpie reply <ask-id> --final` with unstamped prose. The
  host stamps and publishes.
- **Progress**: `kelpie reply <ask-id> --progress` with the full current
  status. The host maintains one post and edits it in place.
- **Speak unprompted**: `kelpie tell botserver`. The host publishes it
  stamped, answering nothing.
- **Later, and again**: the same tell with `--due-in` for once, or
  `--every` for a repeat. Kelpie holds the delivery and the host
  publishes when it arrives. `kelpie schedules` lists them and
  `kelpie schedule-cancel` stops one.

An occupant never touches the relay and never holds a key. It writes
prose and the host publishes it (D31).

### Not yet

Watching for events such as someone coming online, digesting many
events into one message, and escalating to a human are proposed in
`docs/proposals.md` and not built.

## Read order

`AGENTS.md` → `SPEC.md` → `docs/domain-model.md` → `docs/decision-log.md`
→ `docs/open-questions.md`

## License

Private. Do not publish.
