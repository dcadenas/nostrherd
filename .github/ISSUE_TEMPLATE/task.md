---
name: Task
about: Implementation task. Re-read HEAD before starting.
---

**Depends on:** (issue numbers, or "none")

**Contract baseline:** (`<sha>` plus D-numbers)

## Before starting

1. `git fetch origin` and update to `origin/master`
2. Re-read `SPEC.md` and `docs/decision-log.md` at that HEAD
3. If HEAD ≠ baseline, **edit this issue body** (scope, acceptance,
   deps, new baseline SHA). A comment is not enough
4. Do not start if a `Depends on:` issue is still open

## Done when

- Quality gates in `AGENTS.md` pass
- This issue's tests match SPEC at the HEAD you ship
