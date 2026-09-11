# Open questions

Status is `blocks-v1` or `later`. Do not implement a `blocks-v1` item
by guessing.

| ID | Question | Impact | Status |
| --- | --- | --- | --- |
| Q1 | After the first trigger in a thread, do later messages without the inbound prefix still wake that session? | Routing, stealing human mail | decided (D8) |
| Q2 | Exact inbound trigger (`bot:` at line start? `@` mention required?) and outbound stamp | Parser, host stamp | decided (D9) |
| Q3 | Session grain: one occupant per channel vs per thread | Naming, renew, queue | decided (D10) |
| Q4 | Dual connection as the operator pubkey next to Buzz desktop (presence, unread) | Ops | presence: decided (D18); unread: later |
| Q5 | Progress posts vs one stamped reply at end for long work | Turn state | decided (D17) |
| Q6 | Token- or event-count renew | Kelpie | later |
| Q7 | Mechanism for relaying occupant progress prose (stamped or not, reply-in-thread vs new post, rate caps) | Channel noise, D17/D33 host behavior | decided (D42) |
| Q8 | Presence proxy: author-activity watches waking occupants | Wake path, ingest filters | decided (D45, D46) |
| Q9 | Host-side recurring schedules for occupants | Scheduler, turns, D12 | decided (D44): they live in Kelpie, not the host |
| Q10 | Should D56 continuity depend on occupancy-budget renew, or on a trigger that fires on ordinary idle-bot usage? | Continuity, D56, D69 | later |
| Q11 | Should renew wait for an in-flight turn before prepare? If not, is a mid-turn checkpoint accurate enough to resume from? | Renew, D56, D69 | later |
