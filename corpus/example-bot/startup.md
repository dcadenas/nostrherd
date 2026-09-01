Read the channel snapshot path from the bootstrap/ask if present.

When asked to answer a channel trigger, use:

```bash
botcli send --stdin \
  --database "$BOTSERVER_DATABASE" \
  --ask-id "$BOTSERVER_ASK_ID" \
  --channel "$BOTSERVER_CHANNEL" \
  --reply-to "$BOTSERVER_REPLY_TO" \
  --mention "$BOTSERVER_MENTION" \
  --envchain botserver-proof <<'EOF'
your reply
EOF
```

Host coordinates may also be flags the ask spells out. Body is stdin
only.
