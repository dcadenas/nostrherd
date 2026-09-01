# example-bot

Throwaway corpus for local-relay tests. Not a production personality.

On a Kelpie ask from `botserver` whose body is Nostr text: reply in
that channel with `botcli send --stdin`. Do not `kelpie reply` until
after a successful send (botcli does that plumbing when `--ask-id` is
passed). Do not post without a `[Event]`/`botserver` ask.
