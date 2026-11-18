# Demo 1 → Demo 2: walkthrough script

Talking script (about 5 minutes). Commands are in code blocks; the *Say* lines are what to
explain. Run everything from the project folder.

## 0. Recap of Demo 1 (30 s)
*Say:* "In Demo 1 we had a Mininet topology, a TCP broker, publishers and subscribers.
Subscribers computed latency per message and wrote a CSV. It worked, but the broker was a
black box: it only knew *sockets*, not *who* was publishing or subscribing, and the only
statistics were two counters, `published` and `delivered`, printed at shutdown."

## 1. What was missing (30 s)
*Say:* "Three gaps: no registry of who is who, no statistics we can query while running,
and latency lived only in per-subscriber CSV files, so the broker never saw it."

## 2. Change 1 – clients identify themselves (45 s)
*Say:* "Clients now send `HELLO|role|name` right after connecting. It is optional, so Demo 1
clients still work (they show up as `ip:port`). The broker keeps one record per name that
survives disconnects, so a subscriber that has finished still appears in the final report."

Files: `broker.py` (`Rec`, `register`), `publisher.py`, `subscriber.py`.

## 3. Change 2 – the pub/sub table (60 s)
*Say:* "The broker already had `topic → subscribers`. Now it also records which
publishers have published to each topic, and counts messages in and deliveries out."

```bash
python3 demo2.py
```

*Point at:*
```
TOPIC   PUBLISHERS  SUBSCRIBERS  MSGS IN  DELIVERED
alerts  pub2        sub1, sub3        10         20
news    pub1, pub3  sub1, sub2        20         40
```
*Say:* "20 messages in on `news`, 2 subscribers, so 40 deliveries. The numbers check out
against Demo 1's expected counts (30 / 20 / 10 per subscriber)."

## 4. Change 3 – statistics integrated into the broker (75 s)
*Say:* "The broker now keeps counters per topic (messages, bytes, deliveries, msg/s) and per
client (messages and bytes in and out). They can be queried at any time without stopping
anything."

```bash
python3 stats.py            # table + stats
python3 stats.py stats --watch 2
```

*Say:* "Latency is the interesting part. Only the subscriber can measure one-way latency, so
subscribers send `REPORT|<json>` summaries (n, mean, p50, p95, min, max) to the broker every 2
seconds and on exit. The broker merges them: mean is weighted by message count, and the topic
P95 is the worst subscriber's P95 (an approximation, not a pooled percentile)."

## 5. Change 4 – output tuning (45 s)
*Say:* "Output is now aligned tables instead of raw lines."
- Subscriber: timestamped lines, `--quiet`, and a per-topic latency summary at exit.
- Publisher: prints messages, bytes and msg/s; `-v` shows each message.
- Broker: logs by client name (`sub1 subscribed to news`); `--table-interval 5` prints
  tables periodically and on Ctrl+C.

## 6. Compatibility and how it fits the SDN part (30 s)
*Say:* "Three protocol additions: `HELLO`, `REPORT`, `STATS`. `PUBLISH`, `SUBSCRIBE` and `MSG`
are unchanged, so the Mininet topology and the OpenFlow controller are untouched.
`topology.py` just calls `stats.py` at the end to show the broker's table next to the flow table."

For Mininet (Linux + controller running):
```bash
sudo python3 topology.py
```
`demo2.py` runs the same scenario on localhost with no root, for quick demos.

## 7. Summary slide (20 s)
| | Demo 1 | Demo 2 |
|---|---|---|
| Client identity | none | `HELLO|role|name` |
| Broker knowledge | topic → sockets | pub/sub table (who publishes, who subscribes) |
| Statistics | 2 counters at shutdown | per topic + per client, live via `stats.py` |
| Latency | subscriber CSV only | CSV **and** reported to broker, merged |
| Output | raw lines | aligned tables, summaries, `--quiet` |

## Likely questions
- **Why do subscribers report latency instead of the broker measuring it?** The timestamp is
  set by the publisher; only the receiving end can compute end-to-end time.
- **Is the clock the same across Mininet hosts?** Yes, they share the host kernel clock, so
  one-way latency is valid there. On separate machines it would need clock sync.
- **Why does a finished subscriber still appear?** Records are kept per name on purpose so the
  final report is complete; the `STATE` column shows `up`/`gone`.
