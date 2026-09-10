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
verify: _verify-license _verify-stamp _verify-kelpie-pin _verify-version-unreleased _verify-changelog
    @echo "verify: ok"

# Every released version needs an entry someone can read to decide whether to
# upgrade and what it costs them. A version with no section is a release nobody
# outside this machine can act on.
_verify-changelog:
    #!/usr/bin/env bash
    set -euo pipefail
    v=$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
    if ! grep -q "^## ${v}\$" CHANGELOG.md; then
        echo "verify: CHANGELOG.md has no '## ${v}' section" >&2
        exit 1
    fi

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
    # Only what ships counts. A justfile or docs change since the tag leaves
    # every build reporting ${v} identical, so it needs no bump.
    # `corpus/template-bot` and `skills/bot-conduct` are both `include_str!`d
    # into the binary, so both are part of the artifact even though neither
    # lives under crates/.
    changed=$(git diff --name-only "${tag}..HEAD" -- \
        crates Cargo.toml Cargo.lock corpus/template-bot skills/bot-conduct)
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
    # Checked before the bump so a missing entry leaves the tree untouched
    # rather than half-released.
    if ! grep -q "^## {{new_version}}\$" CHANGELOG.md; then
        echo "release: add a '## {{new_version}}' section to CHANGELOG.md first." >&2
        echo "         Say what an operator has to do, not what changed in git." >&2
        exit 1
    fi
    sed -i '0,/^version = ".*"/s//version = "{{new_version}}"/' Cargo.toml
    cargo update --workspace --quiet
    just gates
    # A release is exactly when an upgrade path gets exercised for the first
    # time, by someone who already has a database. Prove both here.
    just smoke
    just upgrade-smoke
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
#
# Runs a *relocated* copy of the binary, with no checkout and no `skills/`
# beside it. That is the shape `cargo install` produces, and it is what the
# README tells people to do; building in place would never exercise it.
smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    root=$(mktemp -d)
    trap 'rm -rf "${root}"' EXIT
    cargo build --release --quiet
    mkdir -p "${root}/home" "${root}/bin"
    cp ./target/release/nostrherd "${root}/bin/nostrherd"
    run() { env -u XDG_CONFIG_HOME -u XDG_DATA_HOME HOME="${root}/home" \
        "${root}/bin/nostrherd" "$@"; }
    run init "${root}/mybot" --id mybot --kind opencode
    run --check
    grep -q 'id = "mybot"' "${root}/home/.config/nostrherd/bots.toml"
    # The occupant advice must travel inside the binary. It used to be read
    # from beside the executable, so an installed host had nothing to read and
    # refused to start; the corpus copy is written from this compiled-in text.
    # -a because grep otherwise decides for itself whether to search a binary,
    # and answers "no match" for one it declines to read.
    grep -aqF 'Conduct for nostrherd occupants' "${root}/bin/nostrherd" \
        || { echo "smoke: the binary does not carry the conduct advice" >&2; exit 1; }
    echo "smoke: a relocated binary registered the bot, loaded it, and carries the conduct advice"

# Start on a database an older version wrote, which `smoke` never does.
#
# `smoke` only ever installs from nothing, so every upgrade path was untested.
# Both outages so far were upgrades, and both were a stored occupant identity
# the host could no longer use. Seed that shape and require the host to reach a
# working state on its own.
upgrade-smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    root=$(mktemp -d)
    trap 'rm -rf "${root}"' EXIT
    cargo build --release --quiet
    run() { env -u XDG_CONFIG_HOME -u XDG_DATA_HOME HOME="${root}/home" \
        ./target/release/nostrherd "$@"; }
    db="${root}/home/.local/share/nostrherd/nostrherd.sqlite"
    mkdir -p "${root}/home"
    run init "${root}/mybot" --id mybot --kind opencode
    run --check

    # Rebuild the pre-D62 shape the migration has to survive: a stored identity
    # column, a session holding a dead UUIDv7 id, and a recorded start attempt
    # keyed to a seat that no longer exists.
    sqlite3 "${db}" "
        ALTER TABLE sessions ADD COLUMN occupant_logical_id TEXT;
        CREATE TABLE occupant_starts (
            sequence INTEGER PRIMARY KEY,
            session_name TEXT NOT NULL,
            attempt_key TEXT NOT NULL UNIQUE,
            attempt_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        ) STRICT;
        INSERT INTO sessions(bot_id, channel_id, session_name,
                             occupant_logical_id, renew_id)
        VALUES ('mybot', 'a-channel', 'mybot-a-channel',
                '01a068f7-fc0e-7172-8dfc-4fc1a54ec66c',
                '01a06902-cf56-7462-ad91-6a7e6f0bb7fe');
        INSERT INTO occupant_starts(session_name, attempt_key, attempt_json,
                                    created_at, updated_at)
        VALUES ('mybot-a-channel', 'stale-key', '{}', 0, 0);"

    run --check 2>"${root}/notice" || { cat "${root}/notice" >&2; exit 1; }

    # Nothing that could name a dead identity may survive, and the session
    # itself must: forgetting the occupant is not forgetting the channel.
    left=$(sqlite3 "${db}" "
        SELECT (SELECT count(*) FROM pragma_table_info('sessions')
                WHERE name = 'occupant_logical_id')
             + (SELECT count(*) FROM sqlite_master
                WHERE type = 'table' AND name = 'occupant_starts');")
    if [[ "${left}" != "0" ]]; then
        echo "upgrade-smoke: a stored occupant identity survived the upgrade" >&2
        sqlite3 -header "${db}" "SELECT * FROM sessions;" >&2
        exit 1
    fi
    sqlite3 "${db}" "SELECT session_name FROM sessions;" | grep -qx 'mybot-a-channel' \
        || { echo "upgrade-smoke: the session itself was lost" >&2; exit 1; }
    echo "upgrade-smoke: a pre-D62 stored identity was dropped, the session kept"

    # The database records which build shaped it (D64), so an upgrade is
    # reported and a downgrade is refused rather than silently losing whatever
    # the older build does not know how to write.
    version=$(just version)
    stamped=$(sqlite3 "${db}" "SELECT value FROM host_meta WHERE key = 'host_version';")
    if [[ "${stamped}" != "${version}" ]]; then
        echo "upgrade-smoke: database says ${stamped:-<nothing>}, binary is ${version}" >&2
        exit 1
    fi

    # Claim a far-future build opened it, which no release can ever be behind.
    sqlite3 "${db}" "UPDATE host_meta SET value = '99.0.0' WHERE key = 'host_version';"
    if run --check 2>"${root}/refused"; then
        echo "upgrade-smoke: started against a database from a newer build" >&2
        exit 1
    fi
    grep -q 'Migrations only go forward' "${root}/refused" \
        || { echo "upgrade-smoke: refused, but not for the reason it should:" >&2
             cat "${root}/refused" >&2; exit 1; }
    # A refused start must not claim the database, or the retry would sail past.
    still=$(sqlite3 "${db}" "SELECT value FROM host_meta WHERE key = 'host_version';")
    if [[ "${still}" != "99.0.0" ]]; then
        echo "upgrade-smoke: a refused downgrade rewrote the stamp to ${still}" >&2
        exit 1
    fi
    echo "upgrade-smoke: the host stamps its version and refuses to run behind it"
