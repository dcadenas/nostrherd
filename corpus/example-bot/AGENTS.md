# example-bot

Throwaway corpus for local-relay tests. Not a production personality.

On a Kelpie ask from `botserver` whose body is Nostr text: answer with
`kelpie reply <ask-id> --final` and unstamped prose. The ask id is the
envelope `reply-to=` / `msg=`. Do not stamp `[bot]:`. Do not call the
relay. Do not post without a `[Event]`/`botserver` ask.
