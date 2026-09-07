# nostrherd

Agents you can talk to in group chat. Each is a live agent session
behind a name, not a scripted bot. It answers when addressed, reports
progress while it works, and speaks up on its own when it has reason to.
Works on any NIP-29 relay.

```text
you   bot: how is the deploy looking?
bot   [bot]: green. 14 minutes since the last failure.
```

The host watches the relay, wakes an agent when someone addresses it,
and publishes what that agent writes.

**Bots post as you.** nostrherd signs with your own Nostr key, so a bot
is your identity speaking, not a separate account. Everything it says is
attributable to you. Treat the key and the relay you point it at
accordingly.

## Status

Alpha, and built for its author's own use. The database schema, the
channel conventions, and the corpus contract all still change. Herdr and
Kelpie are alpha too, and nostrherd pins neither: build all three from
current sources, and rebuild them together when you upgrade any of them.

## How it works

- **The host** (`nostrherd`) is one process. It subscribes to your
  channels, and it is the only thing that publishes.
- **A bot** is an id plus a corpus repository. The id is the address:
  `bot:` reaches the bot with id `bot`, and its posts are stamped
  `[bot]:`. A second bot with id `pr` answers `pr:` and stamps `[pr]:`.
- **An occupant** is the coding agent, running in its own Herdr
  workspace with the corpus as its working directory. One per bot per
  channel, so a bot in two channels holds two separate conversations.
- **Kelpie** carries messages between the host and the occupant, and
  holds the durable timers behind anything deferred or repeating.

The occupant writes prose. The host stamps it, publishes it, and owns
every rule about what reaches the relay.

## Requirements

- Rust (stable) to build.
- [Herdr](https://github.com/herdrdev/herdr), a terminal multiplexer.
  Occupants run in its workspaces.
- [Kelpie](https://github.com/dcadenas/kelpie), a coordination daemon.
  `kelpied` must be running.
- An agent CLI that Herdr can launch, such as `claude` or `opencode`.
- A NIP-29 relay and a Nostr key for it. Run against
  [Buzz](https://github.com/block/buzz) or
  [groups_relay](https://github.com/verse-pbc/groups_relay); both work.
  For testing, build `buzz-relay` from the Buzz repository and run
  `./tools/local-relay up`, which starts it against throwaway Postgres
  and Redis containers; see `skills/local-relay/SKILL.md`. It needs
  Docker.
- [`envchain`](https://github.com/sorah/envchain) to hold the key.

## Build

```bash
cargo build --release
```

## Configure

A bot is one entry in `bots.toml`:

```toml
[[bots]]
id = "bot"                      # the trigger: "bot: ..." in a channel
corpus = "/path/to/corpus-repo" # the agent's working directory
kind = "opencode"               # which agent CLI Herdr launches
```

The corpus is an ordinary git repository holding the bot's personality.
Copy `corpus/template-bot/` to start one; its `AGENTS.md` is the only
file you write. The host writes a contract block into `startup.md` and
never touches anything else in the tree.

Then put the key and relay somewhere the process can read them:

```bash
envchain --set nostrherd BUZZ_PRIVATE_KEY
envchain --set nostrherd BUZZ_RELAY_URL
```

`nostrherd` reads only those two names from its environment. It has no
flag that takes a key, and it never writes one to the database, the
logs, or a process title.

## Run

```bash
envchain nostrherd ./target/release/nostrherd \
  --config bots.toml \
  --database nostrherd.sqlite
```

`--check` loads the config and database and exits, without needing the
key or Kelpie.

Address the bot in a channel with `bot: hello` and a stamped reply
should land. For running against your real relay and account, read
`docs/operator-runbook.md` first.

## What a bot can do

Everything below is what someone types in a channel and what the bot
does about it.

| What you type | What happens |
| --- | --- |
| `bot: how is the deploy looking?` | Answers in that thread, stamped `[bot]:` |
| `pr: status of 123` | A different bot answers. The prefix picks the bot |
| `how is it going?` | Nothing. A bot stays silent unless addressed |
| `bot: in 10 minutes give me the status of pr 123` | Confirms now, posts the answer ten minutes later on its own |
| `bot: every 5 minutes report the queue` | Posts on that interval until cancelled |
| `bot: watch <pubkey> here cooldown 30 max 5` | Wakes when that person next posts, and answers about it |
| `bot: <long task>` | Posts a progress note, edits that same post as work advances, then posts the answer |
| *edit your message* | Answers your new text and discards the old request |
| *delete your message* | Abandons unposted work. An answer already sent stays up |
| `bot: <second question while busy>` | Queues, answered after the first |
| the same bot in another channel | A separate conversation, with seven days of that channel's history |
| a direct message | Works like any channel |

Most of that is the agent reading plain English. Two things are not:
the watch phrase and its `cancel watch <pubkey>` are parsed by the host
itself, deliberately, so that arming a watch and sitting armed cost
nothing. No agent runs until someone the watch names actually posts.

### How those are built

Five primitives, composed:

- **Answer**: `kelpie reply <ask-id> --final` with unstamped prose. The
  host stamps and publishes it.
- **Progress**: `kelpie reply <ask-id> --progress` with the full current
  status. The host keeps one post and edits it in place.
- **Speak unprompted**: `kelpie tell nostrherd`. The host publishes it
  stamped, answering nothing.
- **Later, and again**: the same tell with `--due-in` for once, or
  `--every` for a repeat. Kelpie holds the delivery and the host
  publishes when it arrives. `kelpie schedules` lists them and
  `kelpie schedule-cancel` stops one.
- **Wake on someone**: the watch phrase above. The host evaluates it
  against the relay stream and wakes the agent only on a match.

## Known gaps

- Digesting many events into one message, and escalating to a human,
  are not built.

## Development

`AGENTS.md` is the map for agents working on this repository. `SPEC.md`
is normative. `docs/decision-log.md` holds the numbered decisions that
the rest of the documentation and the code comments cite as `D31`,
`D42`, and so on; where the spec and a later decision disagree, the
decision wins.

Read order: `AGENTS.md` → `SPEC.md` → `docs/domain-model.md` →
`docs/decision-log.md` → `docs/open-questions.md`

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

## License

MIT. See `LICENSE`.
