# botserver

Private host for personal Nostr bots that post **as you**, with a
channel convention (`{bot-id}: …` in, `[{bot-id}]:` out; id `bot` →
`bot:` / `[bot]:`), driven through Herdr and Kelpie.

This is not an ACP child and not a Buzz managed-agent. The daemon watches
the relay as your pubkey, wakes a corpus occupant per bot+channel, and
publishes the occupant's `kelpie reply --final` as `[{bot-id}]:`.

## Binaries

- `botserver` — host process (Kelpie waiter `botserver`) plus per-bot actors

The occupant answers with `kelpie reply --final` and unstamped prose.
The host is the only Nostr publisher (D31). Occupants MUST NOT receive
the operator nsec.

```bash
envchain botserver-proof botserver \
  --config /path/to/bots.toml \
  --database /path/to/botserver.sqlite
```

`envchain` wraps the process. `botserver` only reads `BUZZ_PRIVATE_KEY`
and `BUZZ_RELAY_URL` from the environment (D29). There is no
`--envchain` flag.

The command above uses throwaway live-test namespace `botserver-proof`.
Personal operator keys use namespace `botserver`. Before running the
host as yourself, read `docs/operator-runbook.md`.

## Read order

`AGENTS.md` → `SPEC.md` → `docs/domain-model.md` → `docs/decision-log.md`
→ `docs/open-questions.md`

## License

Private. Do not publish.
