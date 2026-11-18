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
    ap.add_argument("-v", "--verbose", action="store_true", help="print every message sent")
    a = ap.parse_args()

    s = connect(a.broker, a.port)
    s.sendall(f"HELLO|pub|{a.name}\n".encode())   # lets the broker list us in its table
    t0, sent_bytes = time.time(), 0
    for i in range(a.count):
        # timestamp lets the subscriber compute one-way latency
        payload = f"{a.name}-{i}: {a.msg}"
        s.sendall(f"PUBLISH|{a.topic}|{time.time():.6f}|{payload}\n".encode())
        sent_bytes += len(payload.encode())
        if a.verbose:
            print(f"{time.strftime('%H:%M:%S')}  [{a.name}] -> {a.topic:<10} {payload}")
        time.sleep(a.interval)
    elapsed = max(time.time() - t0, 1e-9)
    time.sleep(0.1)                # let the broker drain before we close
    s.close()
    print(f"[{a.name}] sent {a.count} msgs ({sent_bytes} B) on '{a.topic}' "
          f"in {elapsed:.2f}s = {a.count / elapsed:.1f} msg/s")
