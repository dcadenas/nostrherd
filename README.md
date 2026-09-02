# botserver

Private host for personal Nostr bots that post **as you**, with a
channel convention (`bot: …` in, `[bot]:` out), driven through Herdr and
Kelpie.

This is not an ACP child and not a Buzz managed-agent. The daemon watches
the relay as your pubkey, wakes a corpus occupant per bot+channel, and
publishes through `botcli`.

## Binaries

- `botserver` — host process (Kelpie waiter `botserver`) plus per-bot actors
- `botcli` — occupant tool: publish, then `kelpie reply --final`

`botcli send` reads generated text from stdin or a file, never from a body
argument. Host coordinates remain flags:

```bash
envchain botserver-proof botcli send --stdin \
  --database /path/to/botserver.sqlite \
  --ask-id <kelpie-ask-id> \
  --channel <channel-uuid> \
  --reply-to <event-id> \
  --mention <pubkey> <<'EOF'
reply text
EOF
```

`envchain` wraps the process. `botcli` only reads `BUZZ_PRIVATE_KEY` and
`BUZZ_RELAY_URL` from the environment (D29). It stamps `[bot]:` and prints
a JSON receipt. When `--ask-id` is omitted, `--database` is omitted too
and no Kelpie obligation is closed.

The command above uses throwaway live-test namespace `botserver-proof`.
Personal operator keys use namespace `botserver`. Before running the
host as yourself, read `docs/operator-runbook.md`.

## Read order

`AGENTS.md` → `SPEC.md` → `docs/domain-model.md` → `docs/decision-log.md`
→ `docs/open-questions.md`

## License

Private. Do not publish.
