#!/usr/bin/env python3
"""
Subscriber: subscribes to topics, prints messages and per-message latency.
While running, type  'sub <topic>'  or  'unsub <topic>'  to change
subscriptions dynamically.  Optional --log writes a CSV for evaluation.
"""
import argparse
import socket
import sys
import threading
import time


def stdin_loop(sock):
    for cmd in sys.stdin:
        p = cmd.strip().split()
        if len(p) == 2 and p[0] in ("sub", "unsub"):
            verb = "SUBSCRIBE" if p[0] == "sub" else "UNSUBSCRIBE"
            sock.sendall(f"{verb}|{p[1]}\n".encode())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--topics", nargs="+", required=True)
    ap.add_argument("--duration", type=float, default=0, help="0 = run until Ctrl+C")
    ap.add_argument("--log", help="CSV file: recv_time,topic,latency_ms")
    ap.add_argument("--name", default="sub")
    a = ap.parse_args()

    s = socket.create_connection((a.broker, a.port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    for t in a.topics:
        s.sendall(f"SUBSCRIBE|{t}\n".encode())
    threading.Thread(target=stdin_loop, args=(s,), daemon=True).start()

    log = open(a.log, "w") if a.log else None
    if log:
        log.write("recv_time,topic,latency_ms\n")
    if a.duration:   # unblock readline() after --duration seconds
        threading.Timer(a.duration, lambda: s.shutdown(socket.SHUT_RDWR)).start()
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
                print(f"[{a.name}] {parts[1]}: {parts[3]}  ({lat:.2f} ms)")
                if log:
                    log.write(f"{now:.6f},{parts[1]},{lat:.3f}\n")
                    log.flush()
            else:
                print(f"[{a.name}] {parts[0]}: {'|'.join(parts[1:])}")
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[{a.name}] received {got} messages")
        s.close()
        if log:
            log.close()
