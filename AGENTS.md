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

## Rules

- Domain crate has no SQLite, Kelpie, Herdr, or relay I/O.
- Do not put nsecs in SQLite, logs, or pane env. `botcli` uses envchain.
- Do not implement a `blocks-v1` open question by guessing.
- Record accepted choices in `docs/decision-log.md`.

## Quality gates

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```
