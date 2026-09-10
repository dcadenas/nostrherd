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

## Unsettled Occupant Starts

The host retains start intent, its first error/receipt, and the latest
reconciliation diagnostic in `occupant_starts`. A transport failure before
declaration is retried with the same key and seat; a known live seat is adopted
under the recorded logical identity. Neither path needs a database reset.

If retries remain unsettled, inspect the matching host session, the latest
attempt, `kelpie --json report`, and the exact recorded Herdr pane/terminal.
Restore reachability to the same Kelpie store first. A reserved-key refusal
names a prior operation; it is not permission to invent a new key or delete
host intent. Conflicting identities, an ongoing retirement, or a known native
start that cannot settle require reconciliation of that Kelpie/Herdr binding.
Preserve the recorded logical id when recovering it. Do not clear the host's
SQLite rows to make a namesake replacement possible.

A rejected start alone does not prove the pane is empty (`agent_pane_busy`
can mean a different occupant is there). The host leaves workspace reclamation
to the operator, per D39; inspect ownership before closing any pane.

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
allowed_requesters = [] # anyone who can reach and mention you
# operator_session = "your-private-kelpie-session"
```

An absent or empty requester list admits anyone who can reach the channel and
p-tag the operator. A non-empty list is exact in addition to the operator; list
only the operator's full hex key or npub for operator-only behavior. Invalid
keys reject configuration. Refused requesters create no turn or reaction.
Queued work is rechecked against the current list before dispatch; already-open
asks retain their lifecycle. The occupant sees `self:` for the operator and a
full npub prefix for another requester (D66).

Set `operator_session` to a live Kelpie name when the occupant needs a private
route for operator-only notes. The host-managed contract names it. Output to the
channel is screened for an nsec-shaped value, the selected Kelpie socket path,
and absolute paths under the operator home; refusals are reported without
echoing the body.

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

## Existing Corpus Contract Migration

After installing the host-written contract feature, the operator can migrate
each live corpus separately. This is not performed by the host or by its tests.

1. Back up the corpus's author-owned files. Nothing needs to be kept beside the
   binary: the advice ships inside it (D63).
2. At an operator-chosen restart, confirm `startup.md` contains one populated
   `nostrherd-contract` block and one snapshot block, and that the corpus has a
   `.nostrherd/bot-conduct.md` the host wrote.
3. Remove old hand-copied protocol sections from both `AGENTS.md` and text
   outside host markers in `startup.md` by hand. Keep personality, bot-specific
   advice and the generated marker blocks. Review the corpus diff before committing.
4. Confirm a new occupant reads `startup.md` and a test trigger gets one stamped
   reply. An occupant already running is not notified of contract changes.

The host never edits `AGENTS.md` or removes an author's later protocol copy.
Incomplete, duplicate or overlapping host markers cause an error rather than
silently deleting author text. Repair those markers by hand before retrying.
