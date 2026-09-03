# Operator runbook: personal envchain namespace

Personal `botserver` uses envchain namespace `botserver`.
That is not the throwaway live-test namespaces `botserver-proof` and
`botserver-proof-peer` (D23 live-test relay, `skills/local-relay`).

`envchain` injects `BUZZ_PRIVATE_KEY` and `BUZZ_RELAY_URL` into the
wrapped process (D29, D30). The binary only reads those names from
the environment. There is no `--envchain` flag. Do not exec `envchain`
from the binary. Do not put the nsec in SQLite, logs, process titles,
or standing pane-env.

Do not point `BUZZ_RELAY_URL` at a production relay.

## Set the namespace

`--set` prompts. It does not print values. `envchain --list botserver`
shows **names only**.

```bash
envchain --set botserver BUZZ_PRIVATE_KEY
envchain --set botserver BUZZ_RELAY_URL
```

`BUZZ_RELAY_URL` MUST be a non-production relay you control (local
throwaway, or another non-prod URL). Live proofs in this repo use
`./tools/local-relay` and the proof namespaces, not `botserver`.

## Wrap the host

`botserver` registers a pane-less Kelpie waiter named `botserver`
(`waiter.register`, then a reconnecting `inbox.claim`). It does not
need `HERDR_PANE_ID`. Occupant panes are still Herdr sessions.

The host publishes over this same relay connection (D43); no `buzz`
process is needed on the publish path. `buzz` stays the peer and
verification client in live recipes.

```bash
envchain botserver botserver \
  --config /path/to/bots.toml \
  --database /path/to/botserver.sqlite
```

`bots.toml`:

```toml
[[bots]]
id = "bot"
corpus = "/path/to/corpus-repo"
kind = "opencode"
```

`--check` loads config and database, then exits. It needs neither the
envchain wrap nor Kelpie.

Occupants answer with `kelpie reply --final` and unstamped prose. They
MUST NOT wrap a publish binary and MUST NOT receive the nsec.

## Do not

- Reuse throwaway namespaces `botserver-proof` / `botserver-proof-peer`
  for personal keys
- Reuse other personal or sidecar envchain namespaces
- Pass `--envchain`
- Export `BUZZ_PRIVATE_KEY` into pane-env
- Print, log, or commit nsecs
- Dump `env` / `printenv`
- Run these wraps against a production relay
