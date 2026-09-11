# Changelog

Notable changes per released version, newest first. Versions are the ones
`nostrherd --version` reports and `v<version>` tags in git.

Entries say what an operator has to do, not what a commit touched. `just
release` refuses a version with no section here.

## 0.1.0-alpha.17

- Bot replies now address the person who asked, by `@Label` (D71). The
  requester was already notified on every reply; what changes is that the
  prose names them, so the answer reads as addressed and the client
  renders a mention chip. General text that is not answering anyone in
  particular stays untagged.
- The Audience roster in each snapshot now prints both a participant's
  64-hex pubkey and its npub. Without that, an occupant handed a
  `[npub1…]:` requester stamp had no way to match it to a roster entry
  and could only guess a label.

  **Action**: none. Both land on the next occupant start.

## 0.1.0-alpha.16

- Occupant renew is a 45-minute occupancy budget, not 45 minutes of wall
  clock (D69). Kelpie `--every` counts working/blocked time; idle bots
  slide the due time forward, so the old wording made renew look like a
  wake that never fired. The interval and arming path are unchanged.
- Occupants can now read the watch grammar from the host-managed contract
  (D70). The capability already existed; it was only reachable if a
  reference post happened to sit in the 7-day snapshot.

  **Action**: none. The contract rewrites on the next occupant start.

## 0.1.0-alpha.15

- Progress posts are no longer cut at 1 KiB (D68). The cap was 1024 bytes, which
  D42 asserted without a reason, and it truncated ordinary status notes
  mid-sentence — a 1745-byte progress reply reached the operator ending in
  "7.2.3 driver v…", which reads as corruption rather than policy.

  The cap is now 64 KiB, matching what Buzz's own kind-9 builder accepts.
  Truncation is kept only as a backstop, because removing the cap outright would
  turn an over-long body from truncated into rejected, and a rejected publish
  loses the message entirely. D42's rate limits are unchanged and remain the real
  bound on traffic: one edit per 30 seconds, 20 edits per ask. Finals were never
  capped and still are not.

  **Action**: none.

## 0.1.0-alpha.14

- Occupants are told how to compose what they publish, not just how to behave.
  The conduct advice had conduct rules and no formatting guidance, so GitHub
  artifacts and links came out worse than they needed to: unlabelled code
  fences, prose describing where something lives instead of its URL, and
  `buzz://` links reformatted into uselessness — Buzz renders a returned link as
  a preview card only when it is posted verbatim. The advice now covers
  GitHub-flavored Markdown, language-tagged fences, verbatim `buzz://` links,
  real URLs, a findings-report shape, and not sending a reply that only
  acknowledges the request.

  **Action**: none. The advice ships in the binary (D63) and is rewritten into
  each corpus on the next occupant start, so upgrading and restarting is enough.

## 0.1.0-alpha.13

- Bot replies and bot-initiated posts now notify channel participants named with
  a readable `@Label` (D67). Until now the `@name` went out as plain text with a
  notification tag naming only the requester, so the person named was never told.
  Names resolve only from the current channel roster, matched longest-first
  against display name, kind-0 `name` and NIP-05 local part; a name held by more
  than one participant is left untagged rather than guessed, and tags are
  deduplicated and capped at 50.

  Note that a bot naming someone in passing will now notify them. The occupant
  conduct advice says to name people only when the notification is useful.

  **Action**: none.

## 0.1.0-alpha.12

- Bots now receive a current audience summary with every ask and a participant
  roster in each channel snapshot (D66). A NIP-29 member list wins when the relay
  supplies one; observed-author and unknown fallbacks are always identified as
  shared because silent readers may exist. Host-managed conduct separates what
  a requester may ask the bot to do from what is appropriate to say to everyone
  who can read the channel.
- Empty `allowed_requesters` now admits anyone who can reach the channel and
  mention the operator. A non-empty list remains exact in addition to the
  operator; list only the operator's public key to preserve operator-only use.
  Optional `operator_session` names a private Kelpie destination for notes that
  must not be published.
- Before channel publication, the host refuses an nsec-shaped value, its Kelpie
  socket path, or an absolute operator-home path. Refusal notices do not repeat
  the body and use `operator_session` when configured.

  **Action**: review every bot registry before restarting. Add the operator's
  public key to bots that must remain operator-only. Optionally configure
  `operator_session`, then run `nostrherd --check` and restart the host.

## 0.1.0-alpha.11

- Kelpie picks the identity a restarted occupant continues, instead of the host
  reading the claimant history and choosing the newest itself (D65). The host
  now calls `who <name> --resolve` and uses the answer. Same outcome in the
  normal case, one call instead of a policy the host had no business owning.

  **Action**: upgrade Kelpie first, then the host.

      cargo install kelpie-herdr --version 0.2.0-alpha.6
      cargo install --git https://github.com/dcadenas/nostrherd --tag v0.1.0-alpha.11 --force

  and restart `kelpied` and the host. This version requires Kelpie
  `0.2.0-alpha.6`: earlier releases have no `--resolve`, so every restart of a
  dead occupant would fail against them. Upgrading the host first leaves it
  unable to recover a channel until Kelpie catches up.

## 0.1.0-alpha.10

- The database records which build opened it, so an upgrade says what it came
  from and a downgrade is refused (D64). Migrations only go forward — alpha.8's
  drops a column — so an older build cannot restore what a newer one removed and
  would write rows missing whatever it does not know about. Nothing checked for
  that before.

  On an upgrade the host prints one line naming both versions. On a downgrade it
  refuses to start and says how to recover: reinstall the newer build, or restore
  a backup taken before it ran. `nostrherd --check` reports either one without
  connecting to Kelpie or the relay, so run it before restarting.

  **Action**: none. The guard binds from this version onward — a downgrade to
  alpha.9 or earlier is unprotected, because those builds have no such check.

- The `nostrherd connected` log line carries the version, so a log says which
  build wrote it.

## 0.1.0-alpha.9

- The binary is the whole install, so `cargo install` works and a git
  checkout is no longer part of running the host. The occupant conduct advice
  used to be read at runtime from beside the executable, which meant an
  installed binary had nothing to read and refused to start; the checkout was
  what supplied the file. It is compiled in now, and the host writes it to each
  corpus's gitignored `.nostrherd/bot-conduct.md` on start, where the contract
  block points every occupant.

  **Action**: install with

      cargo install --git https://github.com/dcadenas/nostrherd --tag v0.1.0-alpha.9 --force

  and restart the host. Nothing needs to be kept beside the binary any more,
  so a `skills/bot-conduct/` directory copied next to an older installation
  can be deleted. Upgrading from here is the same command with a new tag.

  Editing `skills/bot-conduct/SKILL.md` in a checkout no longer changes a
  running host: the advice ships in the build, so changing it means rebuilding.

## 0.1.0-alpha.8

- A session's name is its identity, and the host converges on one occupant per
  channel (D62). The host no longer records which Kelpie agent occupies a
  session. That pointer went stale twice in a week and stopped a channel dead
  both times: once when Kelpie renumbered agents to integers, and once when a
  name stayed claimed by the pane of an occupant that had died, so every
  replacement was refused as `agent_name_taken`.

  Whatever answers to the name is the occupant. When nothing answers, the host
  reclaims the pane still holding the name, continues the newest identity Kelpie
  recorded for it, and starts. Refusing to allocate a replacement is gone with
  the stored id: two holders of one name cannot exist, so a restart can no
  longer duplicate anything.

  **Action**: none. A channel stuck on a dead occupant recovers by itself on
  its next message, including work queued behind the failure. On first start
  this version drops the stored identities and their recorded start attempts.

- The backend's session token is kept and replayed, so a restarted occupant
  continues its conversation rather than rebuilding it from the corpus. The
  token is opaque: `kind` names any installed agent CLI, and the host stores
  and replays exactly what Herdr reported.

- A pane left holding a session's name is closed when it blocks a start. The
  host allocated a workspace per occupant and never released one, so every
  occupant it ever started leaked its pane.

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
