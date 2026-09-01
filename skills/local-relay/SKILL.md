---
name: local-relay
description: >
  Run the throwaway Buzz relay and envchain namespaces for live
  botserver tests. Use when an issue needs a real relay, botcli send,
  or @operator bot: triggers. Never use personal or sidecar keys.
---

# Local throwaway relay

Live tests in this repo use a **throwaway** nsec and a **local** Buzz
relay. They do not use `nostr-personal`, `buzz-acp`, or prod.

## When

Any issue whose acceptance mentions the relay, `botcli send`, ingest,
occupants, or SPEC user-visible flows.

## Do not

- Print, log, or commit nsecs
- `cat` key files
- Dump `env` / `printenv`
- Point `BUZZ_RELAY_URL` at a production relay
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

- `botserver-proof` — operator (the identity the bot posts as)
- `botserver-proof-peer` — a second user who can `@` the operator

Keys live under `~/tmp-botserver-proof/` (mode 600). The script creates
them if missing. `envchain --list` shows **names only**.

## Operator-facing smoke

```bash
./tools/local-relay up
./tools/local-relay smoke
```

`smoke` runs `botcli send --stdin` as the operator and checks the
channel body starts with `[bot]:`.

## Trigger as the peer

```bash
./tools/local-relay trigger --content 'hello from peer'
```

That posts `@<operator> bot: hello from peer` with a `p` tag. Use this
to exercise ingest once `botserver` is running.

## Secrets in commands

Wrap the **binary**, never put the nsec in pane-env or in flags:

```bash
envchain botserver-proof botcli send --stdin --channel "$CHANNEL" <<'EOF'
text
EOF

envchain botserver-proof botserver --config … --database …
```

`envchain NAMESPACE CMD` injects Buzz vars into CMD. The binaries only
read the environment (D29). Until #24, `botcli` still accepts a leftover
`--envchain` flag; do not add new call sites that use it.
