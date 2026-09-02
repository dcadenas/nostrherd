# Testing

## Unit

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

`crates/botserver/src/spec_flows.rs` is in-process acceptance of SPEC
flows 1–12. It is not a live relay proof.

`TriggerMatch` and `TurnTransition` are parsed types in
`crates/domain`. Illegal trigger text and illegal turn changes are
`None`, not stringly-typed later.

Invariants and their tests: `docs/invariants.md`.

## SPEC flow matrix

| Flow | Unit proof | Live proof |
| --- | --- | --- |
| 1 Silence | `flow_01_silence_indexes_without_an_occupant` | landed (`dcadenas/botserver#18`) |
| 2 First call | `flow_02_first_call_starts_bot_foobar_and_asks` | landed (`dcadenas/botserver#18`) |
| 3 Follow-up without prefix | `flow_03_follow_up_without_prefix_does_not_poke` | landed (`dcadenas/botserver#19`) |
| 4 Second call | `flow_04_second_call_reuses_the_same_occupant` | landed (`dcadenas/botserver#19`) |
| 5 Other channel | `flow_05_another_channel_is_an_independent_occupant` | landed (`dcadenas/botserver#19`) |
| 6 DM | `flow_06_dm_is_its_own_channel_session` | landed (`dcadenas/botserver#20`) |
| 7 Thread | `flow_07_thread_stays_on_the_channel_occupant` | landed (`dcadenas/botserver#19`) |
| 8 Busy | `flow_08_busy_queues_the_second_turn` | landed (`dcadenas/botserver#19`) |
| 9 Gone pane | `flow_09_gone_pane_continues_the_logical_agent` | landed (`dcadenas/botserver#20`) |
| 10 Edit / delete | `flow_10_edit_answers_latest_text_and_delete_abandons`, `flow_10_claimed_turn_keeps_the_landing_reply`, `flow_10_posted_turn_is_left_up_after_delete`, `spec_flow_10_cancelled_turn_never_reaches_buzz` | landed (`dcadenas/botserver#20`) |
| 11 Long work | `flow_11_one_ask_while_the_occupant_works`, `spec_flow_11_one_stamped_reply_then_final` | landed (`dcadenas/botserver#20`) |
| 12 Desktop | `flow_12_host_does_not_publish_presence_or_typing` | landed (`dcadenas/botserver#20`) |

Live columns are issues 18–20. Occupant start/ask from the running host
(`dcadenas/botserver#17`) uses the local relay; it is not the flow 2 live
proof. Issue 18 is the live proof of flows 1–2: silence until a trigger,
then a `[bot]:` body via `botcli`. Issue 19 is the live proof of flows
3–5 and 7–8.

## Live local relay

Follow `skills/local-relay/SKILL.md`. Issues 17–19 require it.

```bash
./tools/local-relay up
./tools/local-relay smoke
./tools/local-relay trigger --content 'hello'
./tools/local-relay down
```

### Flows 1–2 (issue 18)

Use throwaway envchain `botserver-proof` / `botserver-proof-peer`. Do not
print nsecs, pubkeys, or event ids. Wrap live Buzz calls with
`env -u BUZZ_AUTH_TAG`. A first-call trigger has no inbound reply marker,
so `botcli` is invoked without `--reply-to` (thread replies are flow 7).

```bash
ROOT=$(pwd)
PROOF=$HOME/tmp-botserver-proof-is18
mkdir -p "$PROOF"
./tools/local-relay up
cargo build -p botserver -p botcli

cat > "$PROOF/bots.toml" <<EOF
[[bots]]
id = "bot"
corpus = "$ROOT/corpus/example-bot"
kind = "opencode"
EOF

# Fresh channel and sqlite. Do not print the channel id.
env -u BUZZ_AUTH_TAG envchain botserver-proof buzz channels create \
  --name botserver-is18 --type stream --visibility open > "$PROOF/channel.json"
python3 -c 'import json,sys; d=json.load(open(sys.argv[1]));
open(sys.argv[2],"w").write(d.get("channel_id") or d.get("id") or "")' \
  "$PROOF/channel.json" "$PROOF/channel.id"
CHANNEL=$(tr -d '\n' < "$PROOF/channel.id")
OPERATOR_PUB=$(tr -d ' \n' < "$HOME/tmp-botserver-proof/operator.pub")

# Host waiter is pane-less (D2 / issue 27). Do not start a Herdr agent named botserver.
rm -f "$PROOF/host.sqlite"
envchain botserver-proof "$ROOT/target/debug/botserver" \
  --config "$PROOF/bots.toml" --database "$PROOF/host.sqlite"

# Flow 1: ordinary channel text (no bot: prefix, no operator mention).
# Expect: no session/turn for this channel, no body starting with [bot]:
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL" --content 'ordinary hello from the channel'
sqlite3 "$PROOF/host.sqlite" \
  "SELECT count(*) FROM sessions WHERE channel_id='$CHANNEL';"
sqlite3 "$PROOF/host.sqlite" \
  "SELECT count(*) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL';"

# Flow 2: peer trigger, then botcli as the occupant pane so kelpie reply --final
# closes the ask. Expect: one open turn, JSON receipt, turn posted, ask resolved,
# one [bot]: body.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL" --mention "$OPERATOR_PUB" --content 'bot: hello'
ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL' AND t.state='open';")
OCCUPANT_PANE=$(kelpie --json report --live | python3 -c 'import json,sys
d=json.load(sys.stdin)
found=[]
def walk(obj):
    if isinstance(obj, dict):
        incs=obj.get("incarnations")
        if incs and obj.get("public_name") and str(obj.get("public_name")).startswith("bot-"):
            pane=(incs[0] or {}).get("observed_pane_id")
            if pane:
                found.append(pane)
        for v in obj.values():
            walk(v)
    elif isinstance(obj, list):
        for v in obj:
            walk(v)
walk(d.get("result") or d)
print(found[-1] if found else "")
')
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_AUTH_TAG envchain botserver-proof \
  "$ROOT/target/debug/botcli" send --stdin \
  --database "$PROOF/host.sqlite" \
  --ask-id "$ASK_ID" \
  --channel "$CHANNEL" <<'EOF'
hello from example-bot
EOF
```

### Flows 3–5, 7–8 (issue 19)

Same throwaway namespaces and `env -u BUZZ_AUTH_TAG` as flows 1–2. Two
fresh channels. Do not print channel ids, pubkeys, or event ids. Host
waiter is pane-less. If a leftover Ready alias named `botserver` blocks
`waiter.register`, retire that incarnation. Occupant recover still uses
`kelpie start --logical-id`. Queued-turn drain runs on the host refresh
tick (every 30s).

```bash
ROOT=$(pwd)
PROOF=$HOME/tmp-botserver-proof-is19
mkdir -p "$PROOF"
./tools/local-relay up
cargo build -p botserver -p botcli

cat > "$PROOF/bots.toml" <<EOF
[[bots]]
id = "bot"
corpus = "$ROOT/corpus/example-bot"
kind = "opencode"
EOF

env -u BUZZ_AUTH_TAG envchain botserver-proof buzz channels create \
  --name botserver-is19-a --type stream --visibility open > "$PROOF/channel-a.json"
env -u BUZZ_AUTH_TAG envchain botserver-proof buzz channels create \
  --name botserver-is19-b --type stream --visibility open > "$PROOF/channel-b.json"
python3 -c 'import json,sys
d=json.load(open(sys.argv[1])); open(sys.argv[2],"w").write(d.get("channel_id") or d.get("id") or "")' \
  "$PROOF/channel-a.json" "$PROOF/channel-a.id"
python3 -c 'import json,sys
d=json.load(open(sys.argv[1])); open(sys.argv[2],"w").write(d.get("channel_id") or d.get("id") or "")' \
  "$PROOF/channel-b.json" "$PROOF/channel-b.id"
CHANNEL_A=$(tr -d '\n' < "$PROOF/channel-a.id")
CHANNEL_B=$(tr -d '\n' < "$PROOF/channel-b.id")
OPERATOR_PUB=$(tr -d ' \n' < "$HOME/tmp-botserver-proof/operator.pub")

rm -f "$PROOF/host.sqlite"
envchain botserver-proof "$ROOT/target/debug/botserver" \
  --config "$PROOF/bots.toml" --database "$PROOF/host.sqlite"

# Flow 3: trigger, then unprefixed follow-up (mention without bot:).
# Expect: still one turn, no extra ask.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL_A" --mention "$OPERATOR_PUB" --content 'bot: hello' \
  > "$PROOF/a-trigger1.json"
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL_A" --mention "$OPERATOR_PUB" --content 'and the PR?'
sqlite3 "$PROOF/host.sqlite" \
  "SELECT count(*) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A';"

ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' AND t.state='open';")
OCCUPANT_PANE=$(kelpie --json report --live | python3 -c 'import json,sys
d=json.load(sys.stdin)
found=[]
def walk(obj):
    if isinstance(obj, dict):
        incs=obj.get("incarnations")
        if incs and obj.get("public_name") and str(obj.get("public_name")).startswith("bot-"):
            pane=(incs[0] or {}).get("observed_pane_id")
            if pane:
                found.append(pane)
        for v in obj.values():
            walk(v)
    elif isinstance(obj, list):
        for v in obj:
            walk(v)
walk(d.get("result") or d)
print(found[-1] if found else "")
')
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_AUTH_TAG envchain botserver-proof \
  "$ROOT/target/debug/botcli" send --stdin \
  --database "$PROOF/host.sqlite" --ask-id "$ASK_ID" --channel "$CHANNEL_A" <<'EOF'
hello from example-bot
EOF

# Flow 4: second bot: in the same channel, --reply-to the first trigger.
# Expect: one session, second turn open (leave it open for flow 5).
python3 -c 'import json,sys
d=json.load(open(sys.argv[1])); open(sys.argv[2],"w").write(d.get("event_id") or d.get("id") or "")' \
  "$PROOF/a-trigger1.json" "$PROOF/a-trigger1.id"
TRIGGER1=$(tr -d '\n' < "$PROOF/a-trigger1.id")
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL_A" --mention "$OPERATOR_PUB" --reply-to "$TRIGGER1" \
  --content 'bot: later'
sqlite3 "$PROOF/host.sqlite" \
  "SELECT count(*) FROM sessions WHERE channel_id='$CHANNEL_A';"
sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.state, length(t.reply_to_event_id) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' ORDER BY t.sequence;"

# Flow 5: bot: on the other channel while A's second turn is still open.
# Expect: two sessions, two names, A still open, B open (does not wait on A).
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL_B" --mention "$OPERATOR_PUB" --content 'bot: status'
sqlite3 "$PROOF/host.sqlite" "SELECT count(*) FROM sessions;"
sqlite3 "$PROOF/host.sqlite" "SELECT count(DISTINCT session_name) FROM sessions;"
sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.state FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' ORDER BY t.sequence;"
sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.state FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_B';"

ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' AND t.state='open';")
REPLY_TO=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.reply_to_event_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' AND t.state='open';")
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_AUTH_TAG envchain botserver-proof \
  "$ROOT/target/debug/botcli" send --stdin \
  --database "$PROOF/host.sqlite" --ask-id "$ASK_ID" --channel "$CHANNEL_A" \
  --reply-to "$REPLY_TO" <<'EOF'
later from example-bot
EOF

# Flow 7: parent message, then bot: --reply-to that event.
# Expect: still one A session, open turn.reply_to_event_id equals the parent (0/1).
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL_A" --content 'parent for thread' > "$PROOF/a-parent.json"
python3 -c 'import json,sys
d=json.load(open(sys.argv[1])); open(sys.argv[2],"w").write(d.get("event_id") or d.get("id") or "")' \
  "$PROOF/a-parent.json" "$PROOF/a-parent.id"
PARENT=$(tr -d '\n' < "$PROOF/a-parent.id")
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL_A" --mention "$OPERATOR_PUB" --reply-to "$PARENT" \
  --content 'bot: in thread'
sqlite3 "$PROOF/host.sqlite" \
  "SELECT count(*) FROM sessions WHERE channel_id='$CHANNEL_A';"
sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.state, t.reply_to_event_id = '$PARENT' FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' AND t.state='open';"
ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' AND t.state='open';")
REPLY_TO=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.reply_to_event_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' AND t.state='open';")
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_AUTH_TAG envchain botserver-proof \
  "$ROOT/target/debug/botcli" send --stdin \
  --database "$PROOF/host.sqlite" --ask-id "$ASK_ID" --channel "$CHANNEL_A" \
  --reply-to "$REPLY_TO" <<'EOF'
thread reply from example-bot
EOF

# Flow 8: two bot: triggers before a reply.
# Expect: one open and one queued on A; after botcli of the open turn, poll until
# the queued turn becomes open (host resume tick is every 30s).
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL_A" --mention "$OPERATOR_PUB" --content 'bot: first'
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL_A" --mention "$OPERATOR_PUB" --content 'bot: second'
sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.state FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' ORDER BY t.sequence;"
ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' AND t.state='open';")
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_AUTH_TAG envchain botserver-proof \
  "$ROOT/target/debug/botcli" send --stdin \
  --database "$PROOF/host.sqlite" --ask-id "$ASK_ID" --channel "$CHANNEL_A" <<'EOF'
busy first
EOF
for _ in $(seq 1 40); do
  queued=$(sqlite3 "$PROOF/host.sqlite" \
    "SELECT count(*) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' AND t.state='queued';")
  opened=$(sqlite3 "$PROOF/host.sqlite" \
    "SELECT count(*) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' AND t.state='open';")
  if [ "$queued" = 0 ] && [ "$opened" = 1 ]; then
    break
  fi
  sleep 1
done
sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.state FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' ORDER BY t.sequence;"
```

### Flows 6, 9-12 (issue 20)

Same throwaway namespaces and `env -u BUZZ_AUTH_TAG` as flows 1–2. Do not
print channel ids, pubkeys, or event ids. Host waiter is pane-less. After
a trigger is `open`, wait one host poll (~1s) before edit/delete so
mutation fetch includes that EventId. After `herdr pane close` of an
occupant, `kelpie recover` so whoami reports the alias unbound; the 30s
resume tick then continues `--logical-id`.

```bash
ROOT=$(pwd)
PROOF=$HOME/tmp-botserver-proof-is20
mkdir -p "$PROOF"
./tools/local-relay up
cargo build -p botserver -p botcli

cat > "$PROOF/bots.toml" <<EOF
[[bots]]
id = "bot"
corpus = "$ROOT/corpus/example-bot"
kind = "opencode"
EOF

OPERATOR_PUB=$(tr -d ' \n' < "$HOME/tmp-botserver-proof/operator.pub")
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz dms open \
  --pubkey "$OPERATOR_PUB" > "$PROOF/dm.json"
python3 -c 'import json,sys
d=json.load(open(sys.argv[1])); open(sys.argv[2],"w").write(d.get("dm_id") or d.get("id") or "")' \
  "$PROOF/dm.json" "$PROOF/dm.id"
for name in recover edit delete longwork posted; do
  env -u BUZZ_AUTH_TAG envchain botserver-proof buzz channels create \
    --name "botserver-is20-$name" --type stream --visibility open \
    > "$PROOF/${name}-channel.json"
  python3 -c 'import json,sys
d=json.load(open(sys.argv[1])); open(sys.argv[2],"w").write(d.get("channel_id") or d.get("id") or "")' \
    "$PROOF/${name}-channel.json" "$PROOF/${name}-channel.id"
done
DM=$(tr -d '\n' < "$PROOF/dm.id")
RECOVER=$(tr -d '\n' < "$PROOF/recover-channel.id")
EDIT=$(tr -d '\n' < "$PROOF/edit-channel.id")
DELETE=$(tr -d '\n' < "$PROOF/delete-channel.id")
LONGWORK=$(tr -d '\n' < "$PROOF/longwork-channel.id")
POSTED=$(tr -d '\n' < "$PROOF/posted-channel.id")

env -u BUZZ_AUTH_TAG envchain botserver-proof buzz users presence \
  --pubkeys "$OPERATOR_PUB" > "$PROOF/presence-before.json"

rm -f "$PROOF/host.sqlite"
envchain botserver-proof "$ROOT/target/debug/botserver" \
  --config "$PROOF/bots.toml" --database "$PROOF/host.sqlite"

occupant_pane() {
  kelpie --json report --live | python3 -c 'import json,sys
d=json.load(sys.stdin)
want=sys.argv[1]
found=[]
def walk(obj):
    if isinstance(obj, dict):
        incs=obj.get("incarnations")
        if incs and obj.get("public_name")==want:
            pane=(incs[0] or {}).get("observed_pane_id")
            if pane:
                found.append(pane)
        for v in obj.values():
            walk(v)
    elif isinstance(obj, list):
        for v in obj:
            walk(v)
walk(d.get("result") or d)
print(found[-1] if found else "")
' "$1"
}

bot_stamped() {
  env -u BUZZ_AUTH_TAG envchain botserver-proof buzz messages get \
    --channel "$1" --limit 50 | python3 -c 'import json,sys
items=json.load(sys.stdin)
print(sum(1 for it in items if str(it.get("content","")).startswith("[bot]:")))'
}

# Flow 6: DM trigger, then an unprefixed DM line that still mentions so the
# host sees it. Expect: one session, one turn.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$DM" --mention "$OPERATOR_PUB" --content 'bot: ping'
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$DM" --mention "$OPERATOR_PUB" --content 'unprefixed dm line'
sqlite3 "$PROOF/host.sqlite" "SELECT count(*) FROM sessions WHERE channel_id='$DM';"
sqlite3 "$PROOF/host.sqlite" \
  "SELECT count(*) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$DM';"
ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$DM' AND t.state='open';")
SNAME=$(sqlite3 "$PROOF/host.sqlite" "SELECT session_name FROM sessions WHERE channel_id='$DM';")
OCCUPANT_PANE=$(occupant_pane "$SNAME")
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_AUTH_TAG envchain botserver-proof \
  "$ROOT/target/debug/botcli" send --stdin \
  --database "$PROOF/host.sqlite" --ask-id "$ASK_ID" --channel "$DM" <<'EOF'
dm hello from example-bot
EOF

# Flow 9: close the occupant pane with the ask still open, recover, wait
# for resume. Expect: same logical id, new pane, still one ask; one [bot]:.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$RECOVER" --mention "$OPERATOR_PUB" --content 'bot: recover me'
ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$RECOVER' AND t.state='open';")
LID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT occupant_logical_id FROM sessions WHERE channel_id='$RECOVER';")
SNAME=$(sqlite3 "$PROOF/host.sqlite" "SELECT session_name FROM sessions WHERE channel_id='$RECOVER';")
OCCUPANT_PANE=$(occupant_pane "$SNAME")
herdr pane close "$OCCUPANT_PANE"
kelpie recover
for _ in $(seq 1 50); do
  if kelpie --json whoami "$SNAME" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
sqlite3 "$PROOF/host.sqlite" \
  "SELECT count(*) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$RECOVER';"
sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.state, occupant_logical_id = '$LID' FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$RECOVER';"
OCCUPANT_PANE=$(occupant_pane "$SNAME")
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_AUTH_TAG envchain botserver-proof \
  "$ROOT/target/debug/botcli" send --stdin \
  --database "$PROOF/host.sqlite" --ask-id "$ASK_ID" --channel "$RECOVER" <<'EOF'
recovered hello
EOF
bot_stamped "$RECOVER"

# Flow 10 edit: wait until open, then edit the trigger to bot: latest.
# Expect: cancelled then open; one [bot]: answering latest.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$EDIT" --mention "$OPERATOR_PUB" --content 'bot: hello' \
  > "$PROOF/edit-trigger.json"
python3 -c 'import json,sys
d=json.load(open(sys.argv[1])); open(sys.argv[2],"w").write(d.get("event_id") or d.get("id") or "")' \
  "$PROOF/edit-trigger.json" "$PROOF/edit-trigger.id"
EDIT_EVENT=$(tr -d '\n' < "$PROOF/edit-trigger.id")
sleep 2
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages edit \
  --event "$EDIT_EVENT" --content 'bot: latest'
sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.state FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$EDIT' ORDER BY t.sequence;"
ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$EDIT' AND t.state='open';")
SNAME=$(sqlite3 "$PROOF/host.sqlite" "SELECT session_name FROM sessions WHERE channel_id='$EDIT';")
OCCUPANT_PANE=$(occupant_pane "$SNAME")
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_AUTH_TAG envchain botserver-proof \
  "$ROOT/target/debug/botcli" send --stdin \
  --database "$PROOF/host.sqlite" --ask-id "$ASK_ID" --channel "$EDIT" <<'EOF'
latest from example-bot
EOF
bot_stamped "$EDIT"

# Flow 10 delete before publish: no [bot]:.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$DELETE" --mention "$OPERATOR_PUB" --content 'bot: delete me' \
  > "$PROOF/delete-trigger.json"
python3 -c 'import json,sys
d=json.load(open(sys.argv[1])); open(sys.argv[2],"w").write(d.get("event_id") or d.get("id") or "")' \
  "$PROOF/delete-trigger.json" "$PROOF/delete-trigger.id"
DELETE_EVENT=$(tr -d '\n' < "$PROOF/delete-trigger.id")
sleep 2
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages delete --event "$DELETE_EVENT"
sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.state FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$DELETE';"
bot_stamped "$DELETE"

# Flow 10 posted reply stays up after delete of the trigger.
# After publish the turn is no longer active, so mutation fetch does not
# ingest that delete (D24). The witness is the stamped body still on the
# channel, not sqlite seeing the delete event.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$POSTED" --mention "$OPERATOR_PUB" --content 'bot: stay up' \
  > "$PROOF/posted-trigger.json"
python3 -c 'import json,sys
d=json.load(open(sys.argv[1])); open(sys.argv[2],"w").write(d.get("event_id") or d.get("id") or "")' \
  "$PROOF/posted-trigger.json" "$PROOF/posted-trigger.id"
ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$POSTED' AND t.state='open';")
SNAME=$(sqlite3 "$PROOF/host.sqlite" "SELECT session_name FROM sessions WHERE channel_id='$POSTED';")
OCCUPANT_PANE=$(occupant_pane "$SNAME")
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_AUTH_TAG envchain botserver-proof \
  "$ROOT/target/debug/botcli" send --stdin \
  --database "$PROOF/host.sqlite" --ask-id "$ASK_ID" --channel "$POSTED" <<'EOF'
stay up from example-bot
EOF
POSTED_EVENT=$(tr -d '\n' < "$PROOF/posted-trigger.id")
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages delete --event "$POSTED_EVENT"
sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.state FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$POSTED';"
bot_stamped "$POSTED"

# Flow 11: one ask while the occupant works; one stamped reply; no working ping.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$LONGWORK" --mention "$OPERATOR_PUB" --content 'bot: long job'
sqlite3 "$PROOF/host.sqlite" \
  "SELECT count(*) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$LONGWORK';"
sleep 5
ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$LONGWORK' AND t.state='open';")
SNAME=$(sqlite3 "$PROOF/host.sqlite" "SELECT session_name FROM sessions WHERE channel_id='$LONGWORK';")
OCCUPANT_PANE=$(occupant_pane "$SNAME")
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_AUTH_TAG envchain botserver-proof \
  "$ROOT/target/debug/botcli" send --stdin \
  --database "$PROOF/host.sqlite" --ask-id "$ASK_ID" --channel "$LONGWORK" <<'EOF'
long job done
EOF
bot_stamped "$LONGWORK"

# Flow 12: host does not publish presence or typing as the operator.
# Expect: presence-after equals presence-before. Typing is not published:
# the host subscriber has no signing keys and never calls send.
env -u BUZZ_AUTH_TAG envchain botserver-proof buzz users presence \
  --pubkeys "$OPERATOR_PUB" > "$PROOF/presence-after.json"
python3 -c 'import json,sys
b=json.load(open(sys.argv[1])); a=json.load(open(sys.argv[2]))
raise SystemExit(0 if a==b else 1)' \
  "$PROOF/presence-before.json" "$PROOF/presence-after.json"
```

### Socket waiter (issue 27)

Host waiter is pane-less. First `bot:` still yields one `[bot]:`. Occupant
envelopes use `from=botserver`. The ask stays open until `inbox.ack`.
Killing the host before ACK leaves the obligation open. Do not print
pubkeys or event ids. Live proof landed: no waiter pane, one stamped
reply, obligation closed only after ACK, drop-host left it open, delete
cancelled the unposted turn.

```bash
ROOT=$(pwd)
PROOF=$HOME/tmp-botserver-proof-is27
mkdir -p "$PROOF"
./tools/local-relay up
cargo build -p botserver -p botcli

cat > "$PROOF/bots.toml" <<EOF
[[bots]]
id = "bot"
corpus = "$ROOT/corpus/example-bot"
kind = "opencode"
EOF

env -u BUZZ_AUTH_TAG envchain botserver-proof buzz channels create \
  --name botserver-is27 --type stream --visibility open > "$PROOF/channel.json"
python3 -c 'import json,sys; d=json.load(open(sys.argv[1]));
open(sys.argv[2],"w").write(d.get("channel_id") or d.get("id") or "")' \
  "$PROOF/channel.json" "$PROOF/channel.id"
CHANNEL=$(tr -d '\n' < "$PROOF/channel.id")
OPERATOR_PUB=$(tr -d ' \n' < "$HOME/tmp-botserver-proof/operator.pub")

rm -f "$PROOF/host.sqlite"
env -u HERDR_PANE_ID envchain botserver-proof "$ROOT/target/debug/botserver" \
  --config "$PROOF/bots.toml" --database "$PROOF/host.sqlite" &
HOST_PID=$!

occupant_pane() {
  kelpie --json report --live | python3 -c 'import json,sys
d=json.load(sys.stdin)
want=sys.argv[1]
found=[]
def walk(obj):
    if isinstance(obj, dict):
        incs=obj.get("incarnations")
        if incs and obj.get("public_name")==want:
            pane=(incs[0] or {}).get("observed_pane_id")
            if pane:
                found.append(pane)
        for v in obj.values():
            walk(v)
    elif isinstance(obj, list):
        for v in obj:
            walk(v)
walk(d.get("result") or d)
print(found[-1] if found else "")
' "$1"
}

# Expect: report lists waiter botserver with no observed pane.
kelpie --json report --live

env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL" --mention "$OPERATOR_PUB" --content 'bot: hello'
ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL' AND t.state='open';")
SNAME=$(sqlite3 "$PROOF/host.sqlite" "SELECT session_name FROM sessions WHERE channel_id='$CHANNEL';")
OCCUPANT_PANE=$(occupant_pane "$SNAME")
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_AUTH_TAG envchain botserver-proof \
  "$ROOT/target/debug/botcli" send --stdin \
  --database "$PROOF/host.sqlite" \
  --ask-id "$ASK_ID" \
  --channel "$CHANNEL" <<'EOF'
hello from example-bot
EOF
# Expect: one [bot]: body. kelpie pending "$SNAME" is empty after ACK.
kelpie --json pending "$SNAME"

# Drop-host: second trigger, kill host, occupant replies, pending stays open.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL" --mention "$OPERATOR_PUB" --content 'bot: drop-host'
ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL' AND t.state='open';")
kill "$HOST_PID"
OCCUPANT_PANE=$(occupant_pane "$SNAME")
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_AUTH_TAG envchain botserver-proof \
  "$ROOT/target/debug/botcli" send --stdin \
  --database "$PROOF/host.sqlite" \
  --ask-id "$ASK_ID" \
  --channel "$CHANNEL" <<'EOF'
after host drop
EOF
# Expect: kelpie pending "$SNAME" still lists the ask.
kelpie --json pending "$SNAME"
env -u HERDR_PANE_ID envchain botserver-proof "$ROOT/target/debug/botserver" \
  --config "$PROOF/bots.toml" --database "$PROOF/host.sqlite" &
HOST_PID=$!
# Expect: pending empties after inbox.ack on reconnect.
kelpie --json pending "$SNAME"

# Delete before publish: trigger, delete, unposted turn cancelled.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL" --mention "$OPERATOR_PUB" --content 'bot: delete me' \
  > "$PROOF/delete-trigger.json"
python3 -c 'import json,sys
d=json.load(open(sys.argv[1])); open(sys.argv[2],"w").write(d.get("event_id") or d.get("id") or "")' \
  "$PROOF/delete-trigger.json" "$PROOF/delete-trigger.id"
sleep 2
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages delete \
  --event "$(tr -d '\n' < "$PROOF/delete-trigger.id")" > "$PROOF/delete.json"
sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.state FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL' ORDER BY t.sequence;"
kill "$HOST_PID"
```

Wrap binaries with envchain. Do not pass `--envchain` (D29):

```bash
envchain botserver-proof botcli send --stdin --channel "$CHANNEL" <<'EOF'
text
EOF
```

Throwaway namespaces only: `botserver-proof` and
`botserver-proof-peer`. Never `nostr-personal` or `buzz-acp`.

Any issue whose done-when includes the relay MUST run the harness and
say so in the issue body.
