#!/usr/bin/env python3
"""Query a running broker for its pub/sub table and statistics.

    python3 stats.py                     # table + stats
    python3 stats.py table               # pub/sub table only
    python3 stats.py stats --watch 2     # refresh every 2 s
    python3 stats.py all --json          # raw snapshot
"""
import argparse
import json
import socket
import time

import report


def snapshot(host, port):
    with socket.create_connection((host, port), timeout=5) as s:
        f = s.makefile("rw", encoding="utf-8", newline="\n")
        f.write("HELLO|admin|stats-cli\nSTATS|\n")
        f.flush()
        for line in f:
            if line.startswith("STATS|"):
                return json.loads(line.split("|", 1)[1])
    raise SystemExit("broker closed the connection")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("view", nargs="?", choices=["all", "table", "stats"], default="all")
    ap.add_argument("--broker", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--watch", type=float, help="refresh every N seconds")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    render = {"all": report.fmt_all, "table": report.fmt_table, "stats": report.fmt_stats}[a.view]
    try:
        while True:
            snap = snapshot(a.broker, a.port)
            print(json.dumps(snap, indent=2) if a.json else render(snap))
            if not a.watch:
                break
            print()
            time.sleep(a.watch)
    except KeyboardInterrupt:
        pass
