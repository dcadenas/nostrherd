# Operator runbook: personal envchain namespace

Personal `botserver` / `botcli` use envchain namespace `botserver`.
That is not the throwaway live-test namespaces `botserver-proof` and
`botserver-proof-peer` (D23, `skills/local-relay`).

`envchain` injects `BUZZ_PRIVATE_KEY` and `BUZZ_RELAY_URL` into the
wrapped process (D29, D30). The binaries only read those names from
the environment. There is no `--envchain` flag. Do not exec `envchain`
from the binaries. Do not put the nsec in SQLite, logs, process titles,
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

`botserver` MUST run in a Herdr pane so `HERDR_PANE_ID` is set (D2).
A non-Herdr shell exits with `missing HERDR_PANE_ID`.

```bash
HERDR_PANE_ID="$PANE" envchain botserver botserver \
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

`--check` loads config and database, then exits. It still needs the
same envchain wrap.

## Wrap botcli

Occupants publish with the same namespace:

```bash
envchain botserver botcli send --stdin \
  --database /path/to/botserver.sqlite \
  --ask-id <kelpie-ask-id> \
  --channel <channel-uuid> \
  --reply-to <event-id> \
  --mention <pubkey> <<'EOF'
reply text
EOF
```

`--reply-to` is omitted on a first-call trigger with no inbound reply
marker. `--database` and `--ask-id` are omitted together when no Kelpie
ask should close.

A PATH wrapper that execs `envchain botserver botcli "$@"` is allowed
(SPEC). The namespace name is not a secret.

## Do not

- Reuse throwaway namespaces `botserver-proof` / `botserver-proof-peer`
  for personal keys
- Reuse other personal or sidecar envchain namespaces
- Pass `--envchain`
- Export `BUZZ_PRIVATE_KEY` into pane-env
- Print, log, or commit nsecs
- Dump `env` / `printenv`
- Run these wraps against a production relay
