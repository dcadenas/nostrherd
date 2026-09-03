Read the channel snapshot path from the bootstrap/ask if present.

When asked to answer a channel trigger, take the ask id from the
envelope (`reply-to=` / `msg=`) and:

```bash
kelpie reply <ask-id> --final --stdin <<'EOF'
your reply
EOF
```

Do not stamp `[{id}]:`. Do not call the relay. Do not wrap envchain.

For long work, send progress first: `kelpie reply <ask-id> --progress
--stdin` with the full current status; it resets the reply reminder.
Always finish with `--final`.

<!-- botserver-place-snapshots -->
Read `.botserver/places/<your public Kelpie name>.md` for the last 7 days in this channel. Do not read other place files.
<!-- /botserver-place-snapshots -->
