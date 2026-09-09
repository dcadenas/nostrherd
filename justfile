# nostrherd tasks. Run `just` to list them.
#
# The release recipe exists because cutting one by hand meant remembering four
# separate things, and the manifest drifted from the docs anyway. Everything
# here is safe to run repeatedly.

set shell := ["bash", "-uc"]

_default:
    @just --list --unsorted

# Fast type check while editing.
check:
    cargo check --all-targets

# Format in place.
fmt:
    cargo fmt

# Everything CI enforces. Run before pushing.
gates: verify
    cargo fmt --check
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --workspace

# The version in Cargo.toml, which is the single source of truth.
version:
    @grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/'

# Consistency checks that no compiler or test catches.
verify: _verify-license _verify-stamp _verify-kelpie-pin _verify-version-unreleased
    @echo "verify: ok"

# A released version must identify exactly one build. Once `vX` is tagged,
# further commits carrying X make `--version` a lie: two different binaries
# answer the same. This happened once, and `--version` was the feature it hid.
_verify-version-unreleased:
    #!/usr/bin/env bash
    set -euo pipefail
    v=$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
    tag="v${v}"
    if ! git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
        exit 0
    fi
    tagged=$(git rev-list -n1 "${tag}")
    head=$(git rev-parse HEAD)
    if [[ "${tagged}" == "${head}" ]]; then
        exit 0
    fi
    # Only what goes into the binary counts. A justfile or docs change since the
    # tag leaves every build reporting ${v} identical, so it needs no bump.
    changed=$(git diff --name-only "${tag}..HEAD" -- crates Cargo.toml Cargo.lock)
    if [[ -n "${changed}" ]]; then
        echo "verify: ${tag} is already released at ${tagged:0:7}, but these changed since:" >&2
        echo "${changed}" | sed 's/^/          /' >&2
        echo "        Two builds would report ${v}. Cut the next one with:" >&2
        echo "          just release <next-version>" >&2
        exit 1
    fi

# The manifest's license must match the LICENSE file. These disagreed once:
# the manifest said UNLICENSED while LICENSE and the README said MIT, which
# is the opposite of what a reader of the manifest would conclude.
_verify-license:
    #!/usr/bin/env bash
    set -euo pipefail
    declared=$(grep -m1 '^license = ' Cargo.toml | sed 's/license = "\(.*\)"/\1/')
    if ! head -1 LICENSE | grep -qi "${declared}"; then
        echo "verify: Cargo.toml says license=${declared}, LICENSE says: $(head -1 LICENSE)" >&2
        exit 1
    fi

# The outbound stamp is bold (D61). An unbolded `[id]:` on its own line is a
# CommonMark link reference definition and renders as nothing, so a one-word
# reply arrives blank. Guard the docs; the code is covered by tests.
_verify-stamp:
    #!/usr/bin/env bash
    set -euo pipefail
    # `bot id [mybot]:` in the README is init's prompt default, not a stamp.
    if rg -nP '(?<!\*)\[(bot|pr|mybot|\{id\}|\{bot-id\}|\{bot_id\}|\{\{BOT_ID\}\})\]:' \
         README.md SPEC.md docs/invariants.md docs/domain-model.md corpus/ skills/ 2>/dev/null \
         | rg -v 'bot id \[mybot\]:'; then
        echo "verify: unbolded stamp above; the stamp is \`**[id]**:\` (D61)" >&2
        exit 1
    fi

# The README pins a Kelpie prerelease because a bare `cargo install
# kelpie-herdr` resolves the yanked 0.1.0. Check the pin is still installable.
_verify-kelpie-pin:
    #!/usr/bin/env bash
    set -euo pipefail
    pinned=$(rg -o 'kelpie-herdr --version \S+' README.md | head -1 | awk '{print $3}')
    if [[ -z "${pinned}" ]]; then
        echo "verify: README no longer pins a kelpie-herdr version" >&2
        exit 1
    fi
    yanked=$(curl -sS -A "nostrherd-verify" \
        "https://crates.io/api/v1/crates/kelpie-herdr/${pinned}" \
        | python3 -c 'import json,sys; print(json.load(sys.stdin).get("version",{}).get("yanked","missing"))')
    if [[ "${yanked}" != "False" ]]; then
        echo "verify: README pins kelpie-herdr ${pinned}, which is ${yanked} on crates.io" >&2
        exit 1
    fi

# Cut a release: `just release 0.1.0-alpha.2`
#
# Refuses a dirty tree or a wrong branch rather than producing a half-release.
# Tags, because a manifest version with no tag leaves nothing to check out.
release new_version:
    #!/usr/bin/env bash
    set -euo pipefail
    branch=$(git rev-parse --abbrev-ref HEAD)
    if [[ "${branch}" != "master" ]]; then
        echo "release: on ${branch}, expected master" >&2
        exit 1
    fi
    if [[ -n "$(git status --porcelain)" ]]; then
        echo "release: working tree is dirty; commit or stash first" >&2
        git status --short >&2
        exit 1
    fi
    if git rev-parse -q --verify "refs/tags/v{{new_version}}" >/dev/null; then
        echo "release: tag v{{new_version}} already exists" >&2
        exit 1
    fi
    sed -i '0,/^version = ".*"/s//version = "{{new_version}}"/' Cargo.toml
    cargo update --workspace --quiet
    just gates
    git add Cargo.toml Cargo.lock
    git commit -m "Release {{new_version}}"
    git tag -a "v{{new_version}}" -m "{{new_version}}"
    # Plain echo: `@` is just's line-suppression syntax and is not valid inside
    # a shebang recipe body, where the whole recipe is one shell script.
    echo
    echo "Committed and tagged v{{new_version}}. Nothing is pushed."
    echo "Push with: just release-push {{new_version}}"

# Push a release commit and its tag. Separate so the tag is reviewable first.
release-push version:
    git push origin master
    git push origin "v{{version}}"

# Scaffold a throwaway bot into a temp dir and check it registers and loads.
smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    root=$(mktemp -d)
    trap 'rm -rf "${root}"' EXIT
    cargo build --release --quiet
    run() { env -u XDG_CONFIG_HOME -u XDG_DATA_HOME HOME="${root}/home" \
        ./target/release/nostrherd "$@"; }
    mkdir -p "${root}/home"
    run init "${root}/mybot" --id mybot --kind opencode
    run --check
    grep -q 'id = "mybot"' "${root}/home/.config/nostrherd/bots.toml"
    echo "smoke: init registered the bot and --check loaded it"
