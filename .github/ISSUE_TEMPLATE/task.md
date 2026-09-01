---
name: Task
about: Implementation task. Re-read HEAD before starting.
---

**Depends on:** (issue numbers, or "none")

## Contract at last edit

Cite SPEC / decision ids this issue implements.

## Before starting

1. `git pull`
2. Re-read `SPEC.md` and `docs/decision-log.md` at HEAD
3. If HEAD changed the contract, comment the delta here and adjust
   scope before writing code
4. Do not start if a `Depends on:` issue is still open

## Done when

- Quality gates in `AGENTS.md` pass
- This issue's tests match SPEC at the HEAD you ship
