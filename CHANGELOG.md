# Changelog

Notable changes per released version, newest first. Versions are the ones
`nostrherd --version` reports and `v<version>` tags in git.

Entries say what an operator has to do, not what a commit touched. `just
release` refuses a version with no section here.

## 0.1.0-alpha.7

- Forget occupant identities recorded before Kelpie renumbered its agents. A
  session stored a `UUIDv7` agent id, which the integer-id daemon refuses on
  sight, so the host retried a start that could never succeed — once a second,
  forever, while the channel stayed silent and the trigger sat queued. On first
  start this version clears those ids and their recorded start attempts, naming
  each session it clears.

  **Action**: none. Affected channels start a fresh occupant on their next
  trigger, and work queued behind the failure is delivered rather than lost.
  Those occupants lose their Kelpie conversation continuity, which was already
  unreachable.

- A repeated queued-resume failure is reported once, not on every retry. The
  identical line every second is what buried the one that named the cause.

## 0.1.0-alpha.6

- Pin Kelpie `0.2.0-alpha.5` in the README. crates.io `0.2.0-alpha.4` is the
  older UUID-id build; alpha.5 is the integer-id one this host now speaks to.

  **Action**: `cargo install kelpie-herdr --version 0.2.0-alpha.5`, then
  restart `kelpied` and the host.

## 0.1.0-alpha.5

- Send Kelpie ids as JSON numbers. Kelpie's ids are `serde(transparent)`
  newtypes over `NonZeroU64`, so the daemon accepts numbers only; the host was
  still sending `inbox.claim` and `inbox.ack` ids as strings. alpha.4 fixed
  reading and did not restore the inbox on its own.

  **Action**: required with the integer-id Kelpie. Upgrade past alpha.4, not to
  it. A non-numeric id is still sent unchanged, so an older Kelpie is
  unaffected.

## 0.1.0-alpha.4

- Read Kelpie ids sent as JSON numbers. Kelpie replaced UUID ids with integers
  and its JSON now emits them as numbers; the host read every id with a
  string-only accessor, so each delivery failed as a missing `message_id`.

  Incomplete on its own: see alpha.5 for the sending half. On this version the
  inbox still could not claim.

## 0.1.0-alpha.3

- Scaffolded corpora no longer restate the outbound stamp. `AGENTS.md` and
  `README.md` from `nostrherd init` used to hardcode `[{id}]:`, which went
  stale whenever the stamp changed and had to be hand-edited in every corpus.
  The stamp now appears only in the host-managed block of `startup.md`, which
  the host rewrites on start.

  **No action for existing corpora**, but a corpus scaffolded before this
  version still carries the old literal. Delete the stamp sentence from its
  `AGENTS.md` and `README.md`; nothing replaces it, because `startup.md`
  already says it.

## 0.1.0-alpha.2

- `nostrherd --version` reports the build. There was previously no way to tell
  a stale host from a current one, which is how a bot kept publishing an
  outdated stamp unnoticed.

## 0.1.0-alpha.1

First tagged release.

- The outbound stamp is bold: `**[{id}]**:` (D61). A plain `[{id}]: pong` is a
  CommonMark link reference definition, which renders as nothing, so every
  one-word reply arrived as an empty message in Markdown-rendering clients.
  Multi-word replies were unaffected, which is why it went unnoticed.

  **Action if upgrading from an untagged build**: restart the host, and remove
  the old stamp literal from your corpus's `AGENTS.md` and `README.md`.

- The registry and database resolve by convention (D59):
  `$XDG_CONFIG_HOME/nostrherd/bots.toml` and
  `$XDG_DATA_HOME/nostrherd/nostrherd.sqlite`, falling back to `~/.config` and
  `~/.local/share`. `--config` and `--database` became overrides rather than
  requirements. macOS resolves the same way as Linux, matching Kelpie.

- `nostrherd init <dir>` scaffolds a corpus and registers the bot (D58). It
  refuses a non-empty destination, an id already registered, and a corpus
  another bot already uses.

- Each channel keeps its own continuation state under
  `.nostrherd/sessions/<session>/` (D56), instead of every occupant of a bot
  sharing one `progress.md`.

- Requesters other than the operator must be allowlisted per bot, and the
  occupant is told who asked (D57).

- Requires Kelpie `0.2.0-alpha.4` or later, for the Herdr endpoint-generation
  negotiation. Install it with an explicit version: a bare `cargo install
  kelpie-herdr` resolves the yanked `0.1.0`.
