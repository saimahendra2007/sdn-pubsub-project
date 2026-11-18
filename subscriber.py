#!/usr/bin/env python3
"""
Subscriber: subscribes to topics, prints messages and per-message latency.
While running, type  'sub <topic>'  or  'unsub <topic>'  to change
subscriptions dynamically.  Optional --log writes a CSV for evaluation.

Demo 2: latency statistics are kept per topic, printed as a summary table on exit,
and reported to the broker (REPORT|json) every --report-interval seconds so the broker's
statistics include end-to-end latency.
"""
import argparse
import json
import socket
import sys
import threading
import time

from report import grid, ms


def summarize(vals):
    v = sorted(vals)
    pick = lambda p: v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]
    return {"n": len(v), "mean": sum(v) / len(v), "p50": pick(50), "p95": pick(95),
            "min": v[0], "max": v[-1]}


class Sender:
    """Socket writes come from the stdin thread and the report thread."""

    def __init__(self, sock):
        self.sock, self.lock = sock, threading.Lock()

    def send(self, line):
        with self.lock:
            self.sock.sendall((line + "\n").encode())


def stdin_loop(tx):
    for cmd in sys.stdin:
        p = cmd.strip().split()
        if len(p) == 2 and p[0] in ("sub", "unsub"):
            verb = "SUBSCRIBE" if p[0] == "sub" else "UNSUBSCRIBE"
            tx.send(f"{verb}|{p[1]}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--topics", nargs="+", required=True)
    ap.add_argument("--duration", type=float, default=0, help="0 = run until Ctrl+C")
    ap.add_argument("--log", help="CSV file: recv_time,topic,latency_ms")
    ap.add_argument("--name", default="sub")
    ap.add_argument("--quiet", action="store_true", help="don't print each message, only the summary")
    ap.add_argument("--report-interval", type=float, default=2.0,
                    help="seconds between latency reports to the broker (0 = only at exit)")
    a = ap.parse_args()

    s = socket.create_connection((a.broker, a.port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    tx = Sender(s)
    tx.send(f"HELLO|sub|{a.name}")
    for t in a.topics:
        tx.send(f"SUBSCRIBE|{t}")
    threading.Thread(target=stdin_loop, args=(tx,), daemon=True).start()

    lats = {}                                   # topic -> [latency ms]
    lock = threading.Lock()

    def report():
        with lock:
            data = {t: summarize(v) for t, v in lats.items() if v}
        if data:
            try:
                tx.send("REPORT|" + json.dumps(data))
            except OSError:
                pass

    if a.report_interval:
        def report_loop():
            while True:
                time.sleep(a.report_interval)
                report()
        threading.Thread(target=report_loop, daemon=True).start()

    log = open(a.log, "w") if a.log else None
    if log:
        log.write("recv_time,topic,latency_ms\n")

    def finish():                                # final report, then unblock readline()
        report()
        try:
            s.shutdown(socket.SHUT_WR)           # broker sees EOF after the REPORT
        except OSError:
            pass                                 # broker then closes; readline() returns ''

    if a.duration:
        threading.Timer(a.duration, finish).start()
    f = s.makefile("r", encoding="utf-8", newline="\n")
    got = 0
    try:
        while True:
            try:
                line = f.readline()
            except OSError:
                break
            if not line:
                break
            parts = line.rstrip("\n").split("|", 3)
            if parts[0] == "MSG":
                now = time.time()
                lat = (now - float(parts[2])) * 1000
                got += 1
                with lock:
                    lats.setdefault(parts[1], []).append(lat)
                if not a.quiet:
                    print(f"{time.strftime('%H:%M:%S')}  [{a.name}] {parts[1]:<10} "
                          f"{lat:7.2f} ms  {parts[3]}")
                if log:
                    log.write(f"{now:.6f},{parts[1]},{lat:.3f}\n")
                    log.flush()
            elif parts[0] != "OK" or not a.quiet:
                print(f"{time.strftime('%H:%M:%S')}  [{a.name}] {parts[0]}: {'|'.join(parts[1:])}")
    except KeyboardInterrupt:
        report()
    finally:
        rows = [[t, x["n"], ms(x["min"]), ms(x["mean"]), ms(x["p50"]), ms(x["p95"]), ms(x["max"])]
                for t, x in ((t, summarize(v)) for t, v in sorted(lats.items()) if v)]
        print(f"\n[{a.name}] received {got} messages  (latency in ms)")
        print(grid(["TOPIC", "N", "MIN", "MEAN", "P50", "P95", "MAX"], rows, right=range(1, 7)))
        s.close()
        if log:
            log.close()
