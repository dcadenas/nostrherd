# Testing

## Unit

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

`crates/botserver/src/spec_flows.rs` is in-process acceptance of SPEC
flows 1–12. It is not a live relay proof.

`crates/botserver/src/outbox.rs` and `inbox.rs` prove host publish on
kelpie final, ACK-after-decide, crash-safe retry of the same outbound
event, and best-effort `⏳` add/remove on the trigger. That is not a
live relay proof.

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
then a `[bot]:` body. Issue 19 is the live proof of flows 3–5 and 7–8.
Issue 34 is the live E2E that the occupant only `kelpie reply --final`
and the host stamps `[bot]:` (D31). Issues 18–20 and 27 were live-proved
with leftover `botcli`. Occupant steps below match the current path
(`kelpie reply --final`); do not invoke a send crate.

## Live local relay

Follow `skills/local-relay/SKILL.md`. Issues 17–20, 27, and 34 require it.

```bash
./tools/local-relay up
./tools/local-relay smoke
./tools/local-relay trigger --content 'hello'
./tools/local-relay down
```

### Flows 1–2 (issue 18)

Use throwaway envchain `botserver-proof` / `botserver-proof-peer`. Do not
print nsecs, pubkeys, or event ids. Wrap live Buzz calls with
`env -u BUZZ_AUTH_TAG`. A first-call trigger has no inbound reply marker
(thread replies are flow 7). Occupant answers with `kelpie reply --final`.

```bash
ROOT=$(pwd)
PROOF=$HOME/tmp-botserver-proof-is18
mkdir -p "$PROOF"
./tools/local-relay up
cargo build -p botserver

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

# Flow 2: peer trigger, then occupant kelpie reply --final.
# Expect: one open turn, host stamps [bot]:, ask resolved.
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
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
  kelpie reply "$ASK_ID" --final --stdin <<'EOF'
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
cargo build -p botserver

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
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
  kelpie reply "$ASK_ID" --final --stdin <<'EOF'
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
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
  kelpie reply "$ASK_ID" --final --stdin <<'EOF'
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
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
  kelpie reply "$ASK_ID" --final --stdin <<'EOF'
thread reply from example-bot
EOF

# Flow 8: two bot: triggers before a reply.
# Expect: one open and one queued on A; after occupant final of the open turn, poll until
# the queued turn becomes open (host resume tick is every 30s).
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL_A" --mention "$OPERATOR_PUB" --content 'bot: first'
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL_A" --mention "$OPERATOR_PUB" --content 'bot: second'
sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.state FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' ORDER BY t.sequence;"
ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL_A' AND t.state='open';")
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
  kelpie reply "$ASK_ID" --final --stdin <<'EOF'
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
cargo build -p botserver

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
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
  kelpie reply "$ASK_ID" --final --stdin <<'EOF'
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
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
  kelpie reply "$ASK_ID" --final --stdin <<'EOF'
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
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
  kelpie reply "$ASK_ID" --final --stdin <<'EOF'
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
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
  kelpie reply "$ASK_ID" --final --stdin <<'EOF'
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
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
  kelpie reply "$ASK_ID" --final --stdin <<'EOF'
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
cargo build -p botserver

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
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
  kelpie reply "$ASK_ID" --final --stdin <<'EOF'
hello from example-bot
EOF
# Expect: one [bot]: body. kelpie pending "$SNAME" is empty after ACK.
# Expect: renew armed (1). D27: a failed arm does not block the ask.
sqlite3 "$PROOF/host.sqlite" \
  "SELECT renew_id IS NOT NULL FROM sessions WHERE channel_id='$CHANNEL';"
kelpie --json pending "$SNAME"

# Drop-host: second trigger, kill host, occupant replies, pending stays open.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$CHANNEL" --mention "$OPERATOR_PUB" --content 'bot: drop-host'
ASK_ID=$(sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL' AND t.state='open';")
kill "$HOST_PID"
wait "$HOST_PID" 2>/dev/null || true
OCCUPANT_PANE=$(occupant_pane "$SNAME")
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
  kelpie reply "$ASK_ID" --final --stdin <<'EOF'
after host drop
EOF
# Expect: kelpie pending "$SNAME" still lists the ask.
kelpie --json pending "$SNAME"
env -u HERDR_PANE_ID envchain botserver-proof "$ROOT/target/debug/botserver" \
  --config "$PROOF/bots.toml" --database "$PROOF/host.sqlite" &
HOST_PID=$!
for _ in $(seq 1 30); do
  n=$(kelpie --json pending "$SNAME" | python3 -c 'import json,sys
d=json.load(sys.stdin)
r=d.get("result")
print(len(r) if isinstance(r, list) else 99)')
  if [ "$n" = "0" ]; then break; fi
  sleep 0.3
done
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
sleep 2
sqlite3 "$PROOF/host.sqlite" \
  "SELECT t.state FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$CHANNEL' ORDER BY t.sequence;"
kill "$HOST_PID"
wait "$HOST_PID" 2>/dev/null || true
```

### Occupant kelpie-final (issue 34)

Host publish on occupant `kelpie reply --final`. Do not invoke `botcli`.
Do not wrap that reply with envchain. Add the peer as a channel member
before the trigger so host `--mention` of that author is accepted. Do
not print nsecs, pubkeys, or event ids. Host waiter is pane-less. If a
leftover socket waiter named `botserver` blocks `waiter.register`,
`kelpie waiter-retire --logical-id` that waiter.

Live proof landed: one `[bot]:` whose `e` tag is the trigger and whose
`p` tag is the peer; unprefixed follow-up did not post; edit of an
unposted trigger yielded one `[bot]:` for the latest text; delete before
publish yielded no `[bot]:`, and a late final did not post. Poll until
the host opens a turn before reading `ask_id`. Check relay tags, not
only sqlite. Wait for cancelled-then-open before answering an edit.

```bash
ROOT=$(pwd)
PROOF=$HOME/tmp-botserver-proof-is34
KEYS=$HOME/tmp-botserver-proof
mkdir -p "$PROOF"
./tools/local-relay up
cargo build -p botserver

cat > "$PROOF/bots.toml" <<EOF
[[bots]]
id = "bot"
corpus = "$ROOT/corpus/example-bot"
kind = "opencode"
EOF

OPERATOR_PUB=$(tr -d ' \n' < "$KEYS/operator.pub")
PEER_PUB=$(tr -d ' \n' < "$KEYS/peer.pub")

create_channel() {
  local name=$1 out=$2
  env -u BUZZ_AUTH_TAG envchain botserver-proof buzz channels create \
    --name "$name" --type stream --visibility open > "$PROOF/${out}.json"
  python3 -c 'import json,sys; d=json.load(open(sys.argv[1]));
open(sys.argv[2],"w").write(d.get("channel_id") or d.get("id") or "")' \
    "$PROOF/${out}.json" "$PROOF/${out}.id"
  ch=$(tr -d '\n' < "$PROOF/${out}.id")
  env -u BUZZ_AUTH_TAG envchain botserver-proof buzz channels add-member \
    --channel "$ch" --pubkey "$PEER_PUB" --role member >/dev/null
}

create_channel botserver-is34-first first
create_channel botserver-is34-edit edit
create_channel botserver-is34-delete delete
FIRST=$(tr -d '\n' < "$PROOF/first.id")
EDIT=$(tr -d '\n' < "$PROOF/edit.id")
DELETE=$(tr -d '\n' < "$PROOF/delete.id")

rm -f "$PROOF/host.sqlite"
env -u HERDR_PANE_ID envchain botserver-proof "$ROOT/target/debug/botserver" \
  --config "$PROOF/bots.toml" --database "$PROOF/host.sqlite" \
  >"$PROOF/host.log" 2>&1 &
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

bot_stamped() {
  env -u BUZZ_AUTH_TAG envchain botserver-proof buzz messages get \
    --channel "$1" --limit 50 | python3 -c 'import json,sys
items=json.load(sys.stdin)
print(sum(1 for it in items if str(it.get("content","")).startswith("[bot]:")))'
}

wait_sql() {
  local sql=$1 want=$2
  local got=
  for _ in $(seq 1 80); do
    got=$(sqlite3 "$PROOF/host.sqlite" "$sql" 2>/dev/null || true)
    if [ "$got" = "$want" ]; then
      return 0
    fi
    sleep 0.5
  done
  echo "sqlite wanted $want got ${got:-empty}" >&2
  return 1
}

wait_open_ask() {
  local channel=$1
  local ask=
  for _ in $(seq 1 80); do
    ask=$(sqlite3 "$PROOF/host.sqlite" \
      "SELECT t.ask_id FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$channel' AND t.state='open';" 2>/dev/null || true)
    if [ -n "$ask" ]; then
      printf '%s' "$ask"
      return 0
    fi
    sleep 0.5
  done
  echo "no open ask" >&2
  return 1
}

wait_pane() {
  local name=$1 pane=
  for _ in $(seq 1 80); do
    pane=$(occupant_pane "$name")
    if [ -n "$pane" ]; then
      printf '%s' "$pane"
      return 0
    fi
    sleep 0.5
  done
  echo "no occupant pane" >&2
  return 1
}

reply_final() {
  local pane=$1 ask=$2 body=$3
  rm -f "$ROOT/target/debug/botcli"
  HERDR_PANE_ID="$pane" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
    kelpie reply "$ask" --final --stdin <<EOF
$body
EOF
}

check_posted() {
  local channel=$1 trigger_file=$2 peer_file=$3 needle=$4
  env -u BUZZ_AUTH_TAG envchain botserver-proof buzz messages get \
    --channel "$channel" --limit 50 | python3 -c 'import json,sys
items=json.load(sys.stdin)
trigger=open(sys.argv[1]).read().strip()
peer=open(sys.argv[2]).read().strip()
needle=sys.argv[3]
bots=[it for it in items if str(it.get("content","")).startswith("[bot]:")]
print("posted_count", len(bots))
if len(bots)!=1:
    raise SystemExit(1)
it=bots[0]
content=str(it.get("content",""))
tags=it.get("tags") or []
e_ok=any(isinstance(t,list) and t and t[0]=="e" and len(t)>1 and t[1]==trigger for t in tags)
p_ok=any(isinstance(t,list) and t and t[0]=="p" and len(t)>1 and t[1]==peer for t in tags)
body_ok=needle in content
print("e_tag_is_trigger", int(e_ok))
print("p_tag_is_peer", int(p_ok))
print("stamped_body", int(body_ok))
if not (e_ok and p_ok and body_ok):
    raise SystemExit(1)
' "$trigger_file" "$peer_file" "$needle"
}

# First call: peer p-tags the operator with bot: hello.
# Occupant answers with kelpie reply --final only (no envchain, no botcli).
# Expect: one [bot]: body, e tag is the trigger, p tag is the peer (0/1).
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$FIRST" --mention "$OPERATOR_PUB" --content 'bot: hello' \
  > "$PROOF/first-trigger.json"
python3 -c 'import json,sys; d=json.load(open(sys.argv[1]));
open(sys.argv[2],"w").write(d.get("event_id") or d.get("id") or "")' \
  "$PROOF/first-trigger.json" "$PROOF/first-trigger.id"
TRIGGER=$(tr -d '\n' < "$PROOF/first-trigger.id")
wait_sql "SELECT count(*) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$FIRST' AND t.state='open';" 1
ASK_ID=$(wait_open_ask "$FIRST")
SNAME=$(sqlite3 "$PROOF/host.sqlite" "SELECT session_name FROM sessions WHERE channel_id='$FIRST';")
OCCUPANT_PANE=$(wait_pane "$SNAME")
reply_final "$OCCUPANT_PANE" "$ASK_ID" 'hello from example-bot'
for _ in $(seq 1 40); do
  [ "$(bot_stamped "$FIRST")" = 1 ] && break
  sleep 0.5
done
sqlite3 "$PROOF/host.sqlite" \
  "SELECT a.reply_to_event_id = '$TRIGGER', a.mention = '$PEER_PUB',
          a.body NOT LIKE '[bot]:%' FROM outbound_attempts a
   JOIN turns t ON t.ask_id=a.ask_id JOIN sessions s ON s.id=t.session_id
   WHERE s.channel_id='$FIRST';"
check_posted "$FIRST" "$PROOF/first-trigger.id" "$KEYS/peer.pub" 'hello from example-bot'

# Unprefixed follow-up does not post.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$FIRST" --mention "$OPERATOR_PUB" --content 'and the PR?' \
  >/dev/null
sleep 3
bot_stamped "$FIRST"
sqlite3 "$PROOF/host.sqlite" \
  "SELECT count(*) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$FIRST';"

# Edit of an unposted trigger: one [bot]: for the latest text.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$EDIT" --mention "$OPERATOR_PUB" --content 'bot: hello' \
  > "$PROOF/edit-trigger.json"
python3 -c 'import json,sys; d=json.load(open(sys.argv[1]));
open(sys.argv[2],"w").write(d.get("event_id") or d.get("id") or "")' \
  "$PROOF/edit-trigger.json" "$PROOF/edit-trigger.id"
EDIT_EVENT=$(tr -d '\n' < "$PROOF/edit-trigger.id")
wait_sql "SELECT count(*) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$EDIT' AND t.state='open';" 1
sleep 2
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages edit \
  --event "$EDIT_EVENT" --content 'bot: latest' >/dev/null
wait_sql "SELECT count(*) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$EDIT' AND t.state='cancelled';" 1
wait_sql "SELECT count(*) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$EDIT' AND t.state='open';" 1
ASK_ID=$(wait_open_ask "$EDIT")
SNAME=$(sqlite3 "$PROOF/host.sqlite" "SELECT session_name FROM sessions WHERE channel_id='$EDIT';")
OCCUPANT_PANE=$(wait_pane "$SNAME")
reply_final "$OCCUPANT_PANE" "$ASK_ID" 'latest from example-bot'
for _ in $(seq 1 40); do
  [ "$(bot_stamped "$EDIT")" = 1 ] && break
  sleep 0.5
done
env -u BUZZ_AUTH_TAG envchain botserver-proof buzz messages get \
  --channel "$EDIT" --limit 50 | python3 -c 'import json,sys
items=json.load(sys.stdin)
bots=[it for it in items if str(it.get("content","")).startswith("[bot]:")]
print("edit_posted_count", len(bots))
if len(bots)!=1 or "latest from example-bot" not in str(bots[0].get("content","")):
    raise SystemExit(1)
'

# Delete before publish: no [bot]:. Late final does not post.
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages send \
  --channel "$DELETE" --mention "$OPERATOR_PUB" --content 'bot: delete me' \
  > "$PROOF/delete-trigger.json"
python3 -c 'import json,sys; d=json.load(open(sys.argv[1]));
open(sys.argv[2],"w").write(d.get("event_id") or d.get("id") or "")' \
  "$PROOF/delete-trigger.json" "$PROOF/delete-trigger.id"
wait_sql "SELECT count(*) FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$DELETE' AND t.state='open';" 1
ASK_ID=$(wait_open_ask "$DELETE")
SNAME=$(sqlite3 "$PROOF/host.sqlite" "SELECT session_name FROM sessions WHERE channel_id='$DELETE';")
OCCUPANT_PANE=$(wait_pane "$SNAME")
sleep 2
env -u BUZZ_AUTH_TAG envchain botserver-proof-peer buzz messages delete \
  --event "$(tr -d '\n' < "$PROOF/delete-trigger.id")" >/dev/null
wait_sql "SELECT t.state FROM turns t JOIN sessions s ON s.id=t.session_id WHERE s.channel_id='$DELETE';" cancelled
HERDR_PANE_ID="$OCCUPANT_PANE" env -u BUZZ_PRIVATE_KEY -u BUZZ_RELAY_URL \
  kelpie reply "$ASK_ID" --final --stdin <<'EOF' || true
late final after delete
EOF
sleep 3
bot_stamped "$DELETE"
kill "$HOST_PID"
wait "$HOST_PID" 2>/dev/null || true
```

Wrap the host with envchain. Do not pass `--envchain` (D29):

```bash
envchain botserver-proof botserver --config "$PROOF/bots.toml" --database "$PROOF/host.sqlite"
```

Throwaway namespaces only: `botserver-proof` and
`botserver-proof-peer`. Never `nostr-personal` or `buzz-acp`.

Any issue whose done-when includes the relay MUST run the harness and
say so in the issue body.
