# botserver

Private host for personal Nostr bots that post **as you**, with a
channel convention (`bot: …` in, `[bot]:` out), driven through Herdr and
Kelpie.

This is not an ACP child and not a Buzz managed-agent. The daemon watches
the relay as your pubkey, wakes a corpus occupant per bot+channel, and
publishes through `botcli`.

## Binaries

- `botserver` — host occupant (Kelpie waiter `botserver`) plus per-bot actors
- `botcli` — occupant tool: publish, then `kelpie reply --final`

## Read order

`AGENTS.md` → `SPEC.md` → `docs/domain-model.md` → `docs/decision-log.md`
→ `docs/open-questions.md`

## License

Private. Do not publish.
