#!/usr/bin/env python3
"""Publisher: sends N messages on a topic to the broker."""
import argparse
import socket
import time


def connect(host, port, retries=10):
    for _ in range(retries):
        try:
            s = socket.create_connection((host, port), timeout=5)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return s
        except OSError:
            time.sleep(1)          # broker may not be up yet
    raise SystemExit(f"cannot reach broker {host}:{port}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--count", type=int, default=10)
    ap.add_argument("--interval", type=float, default=0.5, help="seconds between msgs")
    ap.add_argument("--msg", default="hello")
    ap.add_argument("--name", default="pub")
    a = ap.parse_args()

    s = connect(a.broker, a.port)
    for i in range(a.count):
        # timestamp lets the subscriber compute one-way latency
        line = f"PUBLISH|{a.topic}|{time.time():.6f}|{a.name}-{i}: {a.msg}\n"
        s.sendall(line.encode())
        time.sleep(a.interval)
    s.close()
    print(f"[{a.name}] sent {a.count} messages on '{a.topic}'")
