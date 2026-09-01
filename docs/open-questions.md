# Open questions

Status is `blocks-v1` or `later`. Do not implement a `blocks-v1` item
by guessing.

| ID | Question | Impact | Status |
| --- | --- | --- | --- |
| Q1 | After the first trigger in a thread, do later messages without the inbound prefix still wake that session? | Routing, stealing human mail | decided (D8) |
| Q2 | Exact inbound trigger (`bot:` at line start? `@` mention required?) and outbound stamp | Parser, `botcli` | decided (D9) |
| Q3 | Session grain: one occupant per channel vs per thread | Naming, renew, queue | decided (D10) |
| Q4 | Dual connection as the operator pubkey next to Buzz desktop (presence, unread) | Ops | presence: decided (D18); unread: later |
| Q5 | Progress posts vs one `botcli` at end for long work | Turn state | decided (D17) |
| Q6 | Token- or event-count renew | Kelpie | later |
