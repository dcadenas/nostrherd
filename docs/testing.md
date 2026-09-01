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
| 1 Silence | `flow_01_silence_indexes_without_an_occupant` | not landed (`dcadenas/botserver#18`) |
| 2 First call | `flow_02_first_call_starts_bot_foobar_and_asks` | not landed (`dcadenas/botserver#18`) |
| 3 Follow-up without prefix | `flow_03_follow_up_without_prefix_does_not_poke` | not landed (`dcadenas/botserver#19`) |
| 4 Second call | `flow_04_second_call_reuses_the_same_occupant` | not landed (`dcadenas/botserver#19`) |
| 5 Other channel | `flow_05_another_channel_is_an_independent_occupant` | not landed (`dcadenas/botserver#19`) |
| 6 DM | `flow_06_dm_is_its_own_channel_session` | not landed (`dcadenas/botserver#20`) |
| 7 Thread | `flow_07_thread_stays_on_the_channel_occupant` | not landed (`dcadenas/botserver#19`) |
| 8 Busy | `flow_08_busy_queues_the_second_turn` | not landed (`dcadenas/botserver#19`) |
| 9 Gone pane | `flow_09_gone_pane_continues_the_logical_agent` | not landed (`dcadenas/botserver#20`) |
| 10 Edit / delete | `flow_10_edit_answers_latest_text_and_delete_abandons`, `flow_10_claimed_turn_keeps_the_landing_reply`, `flow_10_posted_turn_is_left_up_after_delete`, `spec_flow_10_cancelled_turn_never_reaches_buzz` | not landed (`dcadenas/botserver#20`) |
| 11 Long work | `flow_11_one_ask_while_the_occupant_works`, `spec_flow_11_one_stamped_reply_then_final` | not landed (`dcadenas/botserver#20`) |
| 12 Desktop | `flow_12_host_does_not_publish_presence_or_typing` | not landed (`dcadenas/botserver#20`) |

Live columns are issues 18–20. This issue does not run the relay.

## Live local relay

Follow `skills/local-relay/SKILL.md`. This issue does not require it.

```bash
./tools/local-relay up
./tools/local-relay smoke
./tools/local-relay trigger --content 'hello'
./tools/local-relay down
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
