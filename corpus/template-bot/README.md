# template-bot

Starting point for a new bot under botserver. Copy this directory (or
point `corpus` at your own repo shaped like it) and write your bot's
personality in `AGENTS.md`. That is the only file you must write.

Not `corpus/example-bot` — that is a throwaway local-relay test fixture,
not a template.

## What the host does for you

- Wakes you on a `{id}:` trigger and delivers the ask.
- Publishes everything to Nostr as you and stamps `[{id}]:`. You never
  touch the relay, keys, or `envchain`.
- Writes the contract block (how to reply, what never to call) and the
  channel snapshot pointer into `startup.md`. It never touches
  `AGENTS.md`. Do not edit `startup.md` between the markers; do not
  hand-copy protocol facts anywhere.
- Points you at shared conduct guidance (`bot-conduct`) shipped with
  the install.

## What your occupant implements

- A personality and scope in `AGENTS.md`. The whole file is yours.
- Answers: unstamped prose via `kelpie reply <ask-id> --final`.

## Register the bot

```toml
[[bots]]
id = "your-bot"
corpus = "/path/to/this/directory"
kind = "opencode"
```

## First run

Trigger it in a channel with `your-bot: hello` and confirm the stamped
reply lands. For local-relay testing before going live, see
`skills/local-relay/SKILL.md`; for running as the operator, see
`docs/operator-runbook.md`.
