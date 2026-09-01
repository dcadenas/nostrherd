# Testing

## Unit

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

`crates/botserver/src/spec_flows.rs` is in-process acceptance of SPEC
flows 1–12. It is not a live relay proof.

## Live local relay

Follow `skills/local-relay/SKILL.md`.

```bash
./tools/local-relay up
./tools/local-relay smoke
./tools/local-relay trigger --content 'hello'
./tools/local-relay down
```

Any issue whose done-when includes the relay MUST run the harness and
say so in the issue body.
