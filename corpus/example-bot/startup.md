Read the channel snapshot path from the bootstrap/ask if present.

When asked to answer a channel trigger, use:

```bash
kelpie reply --final --stdin <<'EOF'
your reply
EOF
```

Do not stamp `[bot]:`. Do not call the relay. Do not wrap envchain.

<!-- botserver-place-snapshots -->
Read `.botserver/places/<your public Kelpie name>.md` for the last 7 days in this channel. Do not read other place files.
<!-- /botserver-place-snapshots -->
