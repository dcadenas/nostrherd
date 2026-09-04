# Invariants

Each MUST names the test that currently proves it. SPEC still wins if
this file and SPEC disagree.

| # | Invariant | Test |
| --- | --- | --- |
| I1 | Two bots MUST NOT share a session name | `session_name_tries_more_of_uuid_after_a_prefix_collision`, `session_names_disambiguate_when_bot_and_display_collide`, `session_names_include_the_bot_id` |
| I2 | A session for place A MUST NOT be given history from place B | `snapshot_omits_other_channels_including_dms`, `later_ask_includes_unread_same_channel_delta` |
| I3 | Replaying a processed `EventId` MUST NOT open a second turn | `processed_events_are_idempotent` |
| I4 | Kelpie `from=` for this host is only `botserver` | `names_socket_waiter_botserver`, `ask_is_owned_by_waiter_and_passes_body_on_stdin` |
| I5 | A trigger is first token `{bot-id}:` after an optional mention, plus either an operator `p`-tag or operator authorship | `trigger_requires_operator_p_tag_unless_operator_authored`, `trigger_allows_one_leading_mention`, `trigger_rejects_non_prefix_and_inexact_tokens`, `operator_authored_bot_colon_without_self_p_tag_emits`, `peer_authored_bot_colon_without_operator_p_tag_is_indexed_only`, `inbound_trigger_token_is_the_configured_bot_id`, `inbound_tokens_route_to_the_matching_bot`, `inbound_tokens_queue_separate_sessions_for_each_bot`, `action_bot_id_follows_the_active_turn_for_edits`, `actor_for_bot_selects_the_matching_actor`, `bot_uses_id_as_inbound_trigger_and_rejects_empty_kind` |
| I6 | An empty request after `bot:` MUST NOT open a Turn | `empty_trigger_request_is_not_asked` |
| I7 | Turn states are parsed tokens; there is no `publishing` state | `turn_state_parses_known_tokens_only` |
| I8 | Only queued→open/cancelled and open→posted/failed/cancelled are legal | `turn_transition_parses_legal_changes_only`, `turns_queue_in_order_and_state_changes_are_terminal` |
| I9 | A claimed open turn MUST NOT be cancelled | `claimed_open_turn_cannot_be_cancelled`, `flow_10_claimed_turn_keeps_the_landing_reply` |
| I10 | The host MUST NOT publish on a cancelled ask | `cancelled_turn_acks_without_publish`, `spec_flow_10_cancelled_turn_never_reaches_buzz` |
| I11 | Binaries MUST read `BUZZ_PRIVATE_KEY` and `BUZZ_RELAY_URL` when set, and MUST NOT take `--envchain` | `operator_env_reads_buzz_private_key_and_relay_url`, `runtime_requires_operator_key_from_env`, `runtime_requires_relay_url_from_env`, `botserver_parser_rejects_envchain` |
| I12 | SQLite MUST NOT store nsecs | `sqlite_does_not_persist_an_nsec`, `sqlite_schema_has_no_nsec_columns` |
| I13 | Host MUST add `⏳` on queued/open and remove it when work on that EventId ends; an edit that re-queues the same EventId keeps the marker; occupants MUST NOT react | `trigger_adds_in_flight_reaction`, `queued_trigger_adds_its_own_in_flight_reaction`, `delete_removes_in_flight_reaction`, `occupant_final_removes_in_flight_reaction`, `late_final_on_edited_trigger_keeps_in_flight_reaction`, `posted_turn_removes_in_flight_reaction`, `failed_turn_removes_in_flight_reaction`, `progress_does_not_remove_in_flight_reaction` |
| I14 | Host stamp is `[{bot-id}]:`; occupants MUST NOT stamp; own stamped posts MUST NOT retrigger | `stamp_outbound_prefixes_once`, `bot_uses_id_as_inbound_trigger_and_rejects_empty_kind`, `pr_bot_final_publishes_pr_stamp_once`, `final_with_text_publishes_then_acks`, `stamped_self_posts_do_not_emit_triggers` |
| I15 | Known-occupant tell posts a stamped kind 9 with no trigger `--reply-to` and MUST NOT close a turn; unknown sender or `to=` MUST NOT post | `known_occupant_tell_posts_without_trigger_reply_to`, `occupant_tell_tag_routes_and_drops_scratch`, `unknown_tell_destination_does_not_post`, `unknown_named_sender_tell_does_not_post`, `disagreeing_tell_identity_does_not_post`, `missing_reply_to_acks_without_publish`, `occupant_tell_without_tag_posts_the_whole_body` |
| I16 | Occupant panes MUST be allocated with `herdr workspace create`, never `tab create` | `allocate_uses_workspace_create_not_tab_create` |
| I17 | One progress post per ask (D42): host-created after the hold and host-edited under the interval and cap, no `--mention`, kept on final, Buzz-deleted (kind 9005) on cancel, excluded from snapshots and ask Context, never relayed from the delivery handler | `progress_acks_without_publish`, `final_after_progress_discards_the_pending_body`, `progress_posts_once_after_hold_without_mention`, `later_bodies_coalesce_into_one_edit_after_the_interval`, `edit_cap_drops_later_bodies_with_one_notice`, `progress_body_is_trimmed_and_capped_at_1024_bytes`, `final_leaves_the_progress_post_up_and_drops_a_pending_edit`, `cancel_ends_progress_and_buzz_deletes_the_post`, `crash_after_create_dispatch_redelivers_the_same_id`, `flush_skips_relay_while_the_final_attempt_is_dispatched`, `unaccepted_create_on_a_closed_turn_is_ended_and_leaves_the_scan`, `cancelled_progress_post_deleted`, `edit_replacement_deletes_the_old_progress_post`, `snapshot_progress_post_excluded`, `ask_context_excludes_progress_post`, `flow_11_progress_is_one_edited_post_then_a_final` |

SPEC user-visible flows are the product matrix in `docs/testing.md`.

Secrets (D29): wrap binaries with `envchain NAMESPACE cmd`. Do not add
`--envchain` flags.
