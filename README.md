# nostrherd

Agents you can talk to in group chat. Each is a live agent session
behind a name, not a scripted bot. It answers when addressed, reports
progress while it works, and speaks up on its own when it has reason to.
Works on any NIP-29 relay.

> you — `bot: how is the deploy looking?`
>
> **[bot]**: green. 14 minutes since the last failure.

That is how the reply looks in a chat client. On the wire the stamp is
`**[bot]**:`, bolded so a one-word answer is not swallowed as Markdown
(D61).

The host watches the relay, wakes an agent when someone addresses it,
and publishes what that agent writes.

**Bots post as you.** nostrherd signs with your own Nostr key, so a bot
is your identity speaking, not a separate account. Everything it says is
attributable to you. Treat the key and the relay you point it at
accordingly.

## Status

Alpha, and built for its author's own use. The database schema, the
channel conventions, and the corpus contract all still change. Herdr and
Kelpie are alpha too. Build Herdr from current sources and install Kelpie
`0.2.0-alpha.6` as shown below; the host requires it. Earlier releases spell
ids differently on the wire, so this host will not receive replies from them,
and none before alpha.6 can tell it which identity to continue after an
occupant dies. Check compatibility when upgrading either dependency.

## How it works

- **The host** (`nostrherd`) is one process. It subscribes to your
  channels, decides what is addressed to a bot, and publishes what
  that bot answers.
- **A bot** is an id plus a corpus repository. The id is the address:
  `bot:` reaches the bot with id `bot`, and its posts are stamped
  `**[bot]**:`. A second bot with id `pr` answers `pr:` and stamps `**[pr]**:`.
- **An occupant** is the agent itself, running in its own Herdr
  workspace with the corpus as its working directory. One per bot per
  channel, so a bot in two channels holds two separate conversations.
- **Kelpie** carries messages between the host and the occupant, and
  holds the durable timers behind anything deferred or repeating.

The occupant writes prose. The host stamps it and publishes it, so an
agent never has to know anything about Nostr to answer a question.

## Setup

nostrherd is the last of three processes, and the two below it must
already be running. Do them in this order.

**1. Herdr.** A terminal multiplexer; each bot's agent runs in one of
its workspaces. Install and start it from
[herdrdev/herdr](https://github.com/herdrdev/herdr) and leave it
running. nostrherd cannot start an agent while Herdr is stopped.

**2. Kelpie.** A coordination daemon that carries messages between the
host and the agents. Install it and leave `kelpied` running:

```bash
cargo install kelpie-herdr --version 0.2.0-alpha.6
kelpied
```

The version is required while Kelpie is in alpha: bare `cargo install
kelpie-herdr` selects the yanked stable release rather than a live prerelease.
Install Rust (stable) first if `cargo` is not available.

**3. An agent CLI**, such as `claude` or `opencode`, installed and
already logged in to its provider. Herdr launches it; nostrherd does
not manage its credentials. Run it once by hand and confirm it answers
before going further.

**4. A NIP-29 relay and a Nostr account on it.** Any NIP-29 relay:
[Buzz](https://github.com/block/buzz) and
[groups_relay](https://github.com/verse-pbc/groups_relay) are both
known to work. You need the account's secret key in hex or nsec form,
and the relay's websocket URL. Bots post as this account, so use one
you are willing to speak as.

  For a throwaway local relay to try things against, build `buzz-relay`
  from the Buzz repository and run `./tools/local-relay up`, which
  starts it with Postgres and Redis in Docker; see
  `skills/local-relay/SKILL.md`.

**5. Rust (stable)** to build nostrherd.

## Install

```bash
cargo install --git https://github.com/dcadenas/nostrherd --tag v0.1.0-alpha.13
```

That puts `nostrherd` on your `PATH` and is the whole installation. The
binary carries everything it needs at runtime: the corpus templates and
the shared occupant conduct advice are compiled in, so there is nothing
to keep beside it and nothing to copy if you move it.

The tag is worth pinning while this is alpha, so you know which build you
are running and can read `CHANGELOG.md` for what changed. Omit `--tag` to
track the tip of `master`.

To work on nostrherd itself rather than run it, clone and build instead:

```bash
git clone https://github.com/dcadenas/nostrherd
cd nostrherd
cargo build --release
```

## Upgrading

```bash
cargo install --git https://github.com/dcadenas/nostrherd --tag v<new-version> --force
nostrherd --check
```

Then restart the host. `--check` opens the registry and database, applies
any schema migrations, and exits without needing the key or Kelpie, so it
is the safe way to find out whether the new build accepts your setup
before you restart into it.

The database records which build last opened it, so `--check` names what
you upgraded from. Read those versions in
[`CHANGELOG.md`](CHANGELOG.md), which is written as what an operator has
to do rather than what a commit touched; most entries say **Action**:
none. `nostrherd --version` reports the build you are running.

Downgrading is refused. Migrations only go forward, so an older build
cannot restore what a newer one changed and would write rows missing what
it does not know about. To go back, restore a database backup from before
the newer build first ran.

## Create a bot

A bot is two things: a **corpus**, which is a directory holding its
personality, and one entry in the registry telling the host where that
directory is.

`nostrherd init` does both. You pick where the corpus lives — put it
wherever you keep your own repositories, not inside this checkout:

```bash
nostrherd init ~/bots/mybot
```

It asks two questions, each with a default:

```text
bot id [mybot]:          # the trigger. "mybot: ..." in a channel reaches it
agent kind [opencode]:   # which agent CLI Herdr launches for it
```

Then it writes five files, runs `git init`, and registers the bot:

```text
Wrote 5 files to /home/you/bots/mybot
Registered "mybot" in /home/you/.config/nostrherd/bots.toml
```

Run `init` once per bot, each into its own empty directory. Its next steps
name missing environment settings, remind you to start Herdr and Kelpie,
and show the host command. These are setup hints, not a connectivity check.

Three things it refuses, each before a single file is written:

- **A directory that already holds files.** So pointing `init` at an
  existing corpus can never destroy it.
- **An id already registered.** Ids are the channel trigger, so two bots
  called `pr` would make `pr:` ambiguous. The host refuses to start on a
  duplicate too.
- **A corpus another bot already uses.** Each bot needs its own
  directory. The host keeps a contract block inside `startup.md` stamped
  with that bot's id, so two bots in one directory would overwrite each
  other's on every turn.

To share instructions between bots, give each its own corpus and have
both `AGENTS.md` files point at a common file — a git submodule, or a
path outside both corpora. Do not share the corpus directory itself.

`--config` registers somewhere other than the conventional path, and
`--print-only` prints the entry to stdout and leaves the registry
untouched.

### The corpus is the bot

Everything that makes one bot different from another lives in that one
directory. It is an ordinary git repository, yours to edit and commit.

| Path | Who owns it |
| --- | --- |
| `AGENTS.md` | **You.** Who this bot is, and what it will and will not do. Start here |
| `CLAUDE.md` | You. One line pointing at `AGENTS.md`, so either agent CLI reads the same file |
| `README.md` | You. Notes for whoever opens the directory |
| `startup.md` | The host, between its markers. Yours outside them |
| `.nostrherd/` | The host. Channel history and per-session checkpoints. Gitignored |

`AGENTS.md` is the file to write. What `init` leaves there is
deliberately cautious: it answers questions and declines everything
else. Rewrite it into the bot you actually want. The host never reads it
and never edits it — it goes to the agent, and it is the whole of the
bot's character.

Add anything else a repository of yours would hold: notes the bot should
know, skills under `skills/`, checked-out code it answers questions
about. The corpus is the agent's working directory, so the agent can
reach all of it.

Two things the host writes, and nothing else. Inside `startup.md` it
keeps a contract block telling the agent how to answer, and a pointer to
the current channel's history; both sit between HTML comment markers, so
do not edit between them and do not copy what they say into `AGENTS.md`,
because the host rewrites them and your copy will go stale. Under
`.nostrherd/` it keeps a rolling copy of recent channel messages in
`places/` and each channel's checkpoint in `sessions/<session-name>/`.

One corpus serves every channel the bot answers in. Only that per-session
state is kept apart, so two channels never share a checkpoint and root
`startup.md` stays channel-neutral.

## Configure the host

### Where things live

The host finds its registry and database by convention, so neither is a
flag you have to remember. Both follow the XDG base directories, the way
Kelpie resolves its own paths:

| What | Path |
| --- | --- |
| Bot registry | `$XDG_CONFIG_HOME/nostrherd/bots.toml` |
| Host database | `$XDG_DATA_HOME/nostrherd/nostrherd.sqlite` |

With no XDG variables set, which is the default on macOS and common on
Linux, those fall back to `~/.config/nostrherd/bots.toml` and
`~/.local/share/nostrherd/nostrherd.sqlite`. There is no platform
branch, so a Mac lands on the same paths as Linux rather than on
`~/Library/Application Support`. Both directories are created on first
use. `--config` and `--database` override either one, for a second host
or a throwaway test.

### Secrets

nostrherd requires two environment variables:

| Variable | Value |
| --- | --- |
| `NOSTRHERD_PRIVATE_KEY` | the account's secret key, hex or nsec |
| `NOSTRHERD_RELAY_URL` | the relay's websocket URL, such as `wss://relay.example` |

There is no flag that takes a key, and nostrherd never writes one to
the database, the logs, or a process title. Supply the two however you
already handle secrets. Anything that puts them in the environment
works, so `env`, a systemd unit, a `.env` file your shell sources, or a
secret manager such as [`envchain`](https://github.com/sorah/envchain)
or [`credchain`](https://github.com/dcadenas/credchain) are all fine.

Optional `KELPIE_SOCKET` selects the same socket for sending commands and
receiving replies. If unset or empty, the host uses
`$XDG_RUNTIME_DIR/kelpie/kelpie.sock`; without a nonempty runtime directory,
it uses the OS temporary directory plus `kelpie-client/kelpie/kelpie.sock`,
matching Kelpie. Persistent inbox failures are logged with the selected path.

## Run

```bash
nostrherd
```

`--check` loads the registry and database and exits, without needing the
key or Kelpie. It is the quickest way to confirm a bot you just created
is registered and the config parses.

## First reply

The bot has no account of its own. It speaks as the key you configured,
so that account must already be a member of the channel with permission
to post. Create or join a NIP-29 group with any Nostr client that
supports them, then find the group's id, which is what goes in the
channel your bot answers in.

Now say `mybot: hello` in that channel. Two cases differ:

- **You, from the configured account.** The host sees your own message
  and answers.
- **Another relay member.** Their message must also `p`-tag your account, which
  most clients do when you `@`-mention it. A plain `mybot: hello` from
  someone who has not mentioned you is ignored on purpose, so a bot is
  never woken by a channel it happens to be reading.

An empty `allowed_requesters` list admits anyone who can reach the channel. A
non-empty list is exact in addition to you; list only your own public key for an
operator-only bot. Relay membership is the trust boundary, delegated to the
relay administrators.

The occupant sees `self: hello` for your request or `[<full npub>]: hello` for
another member. Each ask also says whether the current audience is shared, and
the channel snapshot lists known participants with display names when available.
The host prefers the NIP-29 member list; observed-author or unknown fallbacks are
always called shared because silent readers may exist.

Who asked controls what the occupant may do. Who can read controls what it may
say. Channel conduct forbids secrets and, in shared rooms, internal paths,
configuration, transports, and permission reasoning. This is host-written
conduct, not host enforcement. As a mechanical accident check, the host refuses
an nsec-shaped value, its Kelpie socket path, or an absolute path under your home
before publication.

Optional `operator_session = "<Kelpie session name>"` gives the occupant a
private Kelpie destination for operator-only notes. It never enters the relay;
without it, the occupant has no private outlet.

A stamped `**[mybot]**:` reply should land in the thread. If nothing happens,
check that Herdr and `kelpied` are running and that the host log shows
the trigger being observed.

`docs/operator-runbook.md` covers running as your real account.

## What a bot can do

Everything below is what someone types in a channel and what the bot
does about it.

| What you type | What happens |
| --- | --- |
| `bot: how is the deploy looking?` | Answers in that thread, stamped `**[bot]**:` |
| `pr: status of 123` | A different bot answers. The prefix picks the bot |
| `how is it going?` | Nothing. A bot stays silent unless addressed |
| `bot: in 10 minutes give me the status of pr 123` | Confirms now, posts the answer ten minutes later on its own |
| `bot: every 5 minutes report the queue` | Posts on that interval until you ask it to stop |
| `bot: watch <pubkey> here cooldown 30 max 5` | Wakes when that person next posts, and answers about it |
| `bot: <long task>` | Posts a progress note, edits that same post as work advances, then posts the answer |
| *edit your message* | Answers your new text and discards the old request |
| *delete your message* | Abandons unposted work. An answer already sent stays up |
| `bot: <second question while busy>` | Queues, answered after the first |
| the same bot in another channel | A separate conversation, with seven days of that channel's history |
| a direct message | Works like any channel |

Requester admission is not rate limited. A bot that agrees to post every five
minutes will post every five minutes, under your name, until asked to
stop. How often a bot should speak, and when it should stay quiet, is
written in that bot's corpus alongside its personality.

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
