# AGENTS.md

Map for agents working in this repository. Not the product spec.

## What this repo is

A DDD host: one process, one actor per configured bot, SQLite for host
state. Occupants live in **corpus repos** (not this tree). The host
publishes. Occupants `kelpie reply --final` on a trigger turn and MAY
`kelpie tell nostrherd` for a bot-initiated post (D38). Kelpie is the
pane/obligation bus. The Nostr human is never `kelpie from=`.

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
  Before running the host as the operator (not local-relay proofs),
  read `docs/operator-runbook.md`.
- Do not implement a `blocks-v1` open question by guessing.
- Record accepted choices in `docs/decision-log.md`.

## Issues

Honor `Depends on:`. Before starting: `git fetch origin` and update to
`origin/master`; re-read `SPEC.md` and `docs/decision-log.md` at that
HEAD. If HEAD ≠ the issue's `Contract baseline:` SHA, **edit the issue
body** (scope, acceptance, deps, new baseline). Comments are not a
substitute.

## Live local relay

Any issue that touches relay, occupants, or SPEC flows MUST
use `skills/local-relay/SKILL.md` and `tools/local-relay`. Throwaway
keys only. Never `nostr-personal` or `buzz-acp`.

Before claiming new host, relay, or occupant behavior works, run it
against the local Docker relay (`groups_relay`) with a real host and a
real occupant. Unit tests and fakes cannot show what another live system
does at the moment you read it.

- `tools/local-relay up` starts the relay and the throwaway envchain
  namespaces.
- If the live host already owns the Kelpie waiter `nostrherd`, start an
  isolated daemon first: `kelpied --database PATH --socket PATH`. Run the
  scratch host with `KELPIE_SOCKET=PATH`. Never fight the live host for
  its waiter.
- Add the peer to the channel before triggering:
  `env -u BUZZ_AUTH_TAG envchain nostrherd-proof buzz channels add-member
  --channel CHANNEL --pubkey PEER --role member`.
- Trigger as the peer: `./tools/local-relay trigger --content TEXT`.
- Read the relay back:
  `env -u BUZZ_AUTH_TAG envchain nostrherd-proof buzz messages get
  --channel CHANNEL --limit 50`.
- Close every Herdr workspace the scratch host allocated, then run
  `tools/local-relay down`.
- For the repeatable whole-system proof, read the "Dispatch identity
  (issue 82)" section of `docs/testing.md` before running the
  `dispatch_proof` fixture.

## Quality gates

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```
