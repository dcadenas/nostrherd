# AGENTS.md

Map for agents working in this repository. Not the product spec.

## What this repo is

A DDD host: one process, one actor per configured bot, SQLite for host
state, `botcli` for occupant publish. Occupants live in **corpus repos**
(not this tree). Kelpie is the pane/obligation bus. The Nostr human is
never `kelpie from=`.

## Read order

1. `SPEC.md` (normative)
2. `docs/decision-log.md`
3. `docs/open-questions.md` (do not invent answers)
4. `docs/domain-model.md`
5. `docs/invariants.md`
6. `docs/testing.md` and `skills/local-relay/SKILL.md` for live tests

## Rules

- Domain crate has no SQLite, Kelpie, Herdr, or relay I/O.
- Do not put nsecs in SQLite, logs, or pane env. Wrap binaries with
  `envchain NAMESPACE cmd` (D29). Do not add `--envchain` flags.
- Do not implement a `blocks-v1` open question by guessing.
- Record accepted choices in `docs/decision-log.md`.

## Issues

Honor `Depends on:`. Before starting: `git fetch origin` and update to
`origin/master`; re-read `SPEC.md` and `docs/decision-log.md` at that
HEAD. If HEAD ≠ the issue's `Contract baseline:` SHA, **edit the issue
body** (scope, acceptance, deps, new baseline). Comments are not a
substitute.

## Live local relay

Any issue that touches relay, botcli, occupants, or SPEC flows MUST
use `skills/local-relay/SKILL.md` and `tools/local-relay`. Throwaway
keys only. Never `nostr-personal` or `buzz-acp`.

## Quality gates

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```
