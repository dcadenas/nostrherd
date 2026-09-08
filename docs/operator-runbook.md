# Operator runbook: personal envchain namespace

Personal `nostrherd` uses envchain namespace `nostrherd`.
That is not the throwaway live-test namespaces `nostrherd-proof` and
`nostrherd-proof-peer` (D23 live-test relay, `skills/local-relay`).

`envchain` injects `NOSTRHERD_PRIVATE_KEY` and `NOSTRHERD_RELAY_URL` into the
wrapped process (D29, D30). The binary only reads those names from
the environment. There is no `--envchain` flag. Do not exec `envchain`
from the binary. Do not put the nsec in SQLite, logs, process titles,
or standing pane-env.

Do not point `NOSTRHERD_RELAY_URL` at a production relay.

## Set the namespace

`--set` prompts. It does not print values. `envchain --list nostrherd`
shows **names only**.

```bash
envchain --set nostrherd NOSTRHERD_PRIVATE_KEY
envchain --set nostrherd NOSTRHERD_RELAY_URL
```

`NOSTRHERD_RELAY_URL` MUST be a non-production relay you control (local
throwaway, or another non-prod URL). Live proofs in this repo use
`./tools/local-relay` and the proof namespaces, not `nostrherd`.

## Wrap the host

`nostrherd` registers a pane-less Kelpie waiter named `nostrherd`
(`waiter.register`, then a reconnecting `inbox.claim`). It does not
need `HERDR_PANE_ID`. Occupant panes are still Herdr sessions.

The host publishes over this same relay connection (D43); no `buzz`
process is needed on the publish path. `buzz` stays the peer and
verification client in live recipes.

```bash
envchain nostrherd nostrherd \
  --config /path/to/bots.toml \
  --database /path/to/nostrherd.sqlite
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

- Reuse throwaway namespaces `nostrherd-proof` / `nostrherd-proof-peer`
  for personal keys
- Reuse other personal or sidecar envchain namespaces
- Pass `--envchain`
- Export `NOSTRHERD_PRIVATE_KEY` into pane-env
- Print, log, or commit nsecs
- Dump `env` / `printenv`
- Run these wraps against a production relay
# Existing Corpus Contract Migration

After installing the host-written contract feature, the operator can migrate
each live corpus separately. This is not performed by the host or by its tests.

1. Keep `skills/bot-conduct/SKILL.md` with the running installation as described
   in the README. Back up the corpus's author-owned files.
2. At an operator-chosen restart, confirm `startup.md` contains one populated
   `nostrherd-contract` block and one snapshot block, with a readable advice path.
3. Remove the old hand-copied protocol section from `AGENTS.md` by hand, keeping
   personality and bot-specific advice. Review the corpus diff before committing.
4. Confirm a new occupant reads `startup.md` and a test trigger gets one stamped
   reply. An occupant already running is not notified of contract changes.

The host never edits `AGENTS.md` or removes an author's later protocol copy.
Incomplete, duplicate or overlapping host markers cause an error rather than
silently deleting author text. Repair those markers by hand before retrying.
