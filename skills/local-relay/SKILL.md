---
name: local-relay
description: >
  Run the throwaway Buzz relay and envchain namespaces for live
  nostrherd tests. Use when an issue needs a real relay, occupant
  kelpie reply, or @operator bot: triggers. Never use personal or
  sidecar keys.
---

# Local throwaway relay

Live tests in this repo use a **throwaway** nsec and a **local** Buzz
relay. They do not use `nostr-personal`, `buzz-acp`, or prod.

## When

Any issue whose acceptance mentions the relay, ingest, occupants, or
SPEC user-visible flows.

## Do not

- Print, log, or commit nsecs
- `cat` key files
- Dump `env` / `printenv`
- Point `NOSTRHERD_RELAY_URL` at a production relay
- Reuse envchain `nostr-personal` or `buzz-acp`

## Command

From the repo root:

```bash
./tools/local-relay status
./tools/local-relay up
./tools/local-relay smoke
./tools/local-relay trigger --content 'hello'
./tools/local-relay down
```

`up` starts postgres/redis, the relay on `ws://127.0.0.1:13001`, and
ensures two envchain namespaces:

- `nostrherd-proof` — operator (the identity the bot posts as)
- `nostrherd-proof-peer` — a second user who can `@` the operator

Keys live under `~/tmp-nostrherd-proof/` (mode 600). The script creates
them if missing. `envchain --list` shows **names only**.

## Operator-facing smoke

For the startup-contract proof without Docker or shared host resources, run
`./tools/local-relay contract-proof`. It uses nostr-sdk LocalRelay on an
OS-assigned loopback port and generates throwaway keys in memory. Occupant
start and reply transport are synthetic; this is not live Herdr/Kelpie/Buzz
proof. No credential namespace, corpus, or service outside the test is used.

```bash
./tools/local-relay up
./tools/local-relay smoke
```

`smoke` posts a throwaway operator message with `buzz messages send`
and checks the channel body is present. It does not stamp `**[{id}]**:`
and does not invoke a send crate.

## Trigger as the peer

An absent or empty `allowed_requesters` list admits the throwaway peer. A
non-empty list must contain its full public key or npub; listing only the
operator makes the bot operator-only (D66). The peer must still mention the
operator. A refused peer message is indexed without an occupant wake or reaction.

```bash
./tools/local-relay trigger --content 'hello from peer'
```

That posts `@<operator> bot: hello from peer` with a `p` tag. Use this
to exercise ingest once `nostrherd` is running. Occupant answers use
`kelpie reply --final`; the host stamps `**[{id}]**:`. Issue 34 live proof
is recorded in `docs/testing.md`.

## Secrets in commands

Wrap the **binary**, never put the nsec in pane-env or in flags:

```bash
envchain nostrherd-proof nostrherd --config … --database …
```

`nostrherd` registers a pane-less waiter named `nostrherd`. Do not start
a Herdr agent with that name as the host. Occupant panes still use
Herdr. If a leftover Ready alias `nostrherd` blocks `waiter.register`,
retire that incarnation.

The proof namespaces carry `NOSTRHERD_*` for the host and `BUZZ_*` for the Buzz
verification client, derived from the same throwaway keys. The binaries only
read the environment (D29). There is no `--envchain` flag.
