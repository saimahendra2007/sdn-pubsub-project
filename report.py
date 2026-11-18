"""Text rendering of a broker snapshot (pub/sub table + statistics).

Used by the broker (periodic / shutdown printing) and by stats.py (remote query)."""


def grid(headers, rows, right=()):
    """Aligned plain-text table. `right` = indexes of right-aligned columns."""
    if not rows:
        return "  (none)"
    rows = [[str(c) for c in r] for r in rows]
    w = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]

    def line(cells):
        return "  ".join(c.rjust(w[i]) if i in right else c.ljust(w[i])
                         for i, c in enumerate(cells)).rstrip()

    return "\n".join([line(headers), "  ".join("-" * n for n in w)] + [line(r) for r in rows])


def ms(v):
    return "-" if v is None else f"{v:.2f}"


def names(lst):
    return ", ".join(lst) if lst else "-"


def fmt_table(snap):
    """Who publishes / who is subscribed per topic."""
    rows = [[t["topic"], names(t["publishers"]), names(t["subscribers"]), t["msgs"], t["deliveries"]]
            for t in snap["topics"]]
    return "PUB/SUB TABLE\n" + grid(["TOPIC", "PUBLISHERS", "SUBSCRIBERS", "MSGS IN", "DELIVERED"],
                                    rows, right=(3, 4))


def fmt_stats(snap):
    b = snap["broker"]
    head = (f"BROKER STATS   uptime {b['uptime']:.1f}s   published {b['published']}   "
            f"delivered {b['delivered']}   send-failures {b['failed']}   "
            f"clients connected {b['connected']}")
    trows = []
    for t in snap["topics"]:
        lat = t["latency"]
        trows.append([t["topic"], t["msgs"], t["bytes"], t["deliveries"], f"{t['rate']:.1f}",
                      lat["n"] if lat else 0, ms(lat and lat["mean"]), ms(lat and lat["p95"]),
                      ms(lat and lat["max"])])
    crows = []
    for c in snap["clients"]:
        lat = c["latency"]
        crows.append([c["name"], c["role"], "up" if c["connected"] else "gone", names(c["topics"]),
                      c["msgs_in"], c["msgs_out"], c["bytes_in"], c["bytes_out"],
                      ms(lat and lat["mean"]), ms(lat and lat["p95"])])
    return "\n".join([
        head, "",
        "PER TOPIC   (latency = subscriber-measured one-way ms; p95 = worst subscriber's p95)",
        grid(["TOPIC", "MSGS", "BYTES", "DELIVERED", "MSG/S", "LAT N", "MEAN", "P95", "MAX"],
             trows, right=range(1, 9)),
        "",
        "PER CLIENT",
        grid(["NAME", "ROLE", "STATE", "TOPICS", "IN", "OUT", "BYTES IN", "BYTES OUT", "LAT MEAN", "LAT P95"],
             crows, right=range(4, 10)),
    ])


def fmt_all(snap):
    return fmt_table(snap) + "\n\n" + fmt_stats(snap)
