#!/usr/bin/env python3
"""
Pub-Sub broker over raw TCP sockets  (Demo 2: pub/sub table + statistics).

Protocol (one message per line, '\n' terminated, UTF-8):
  client -> broker:
      HELLO|<role>|<name>                        (optional; role = pub|sub|admin)
      SUBSCRIBE|<topic>
      UNSUBSCRIBE|<topic>
      PUBLISH|<topic>|<timestamp>|<payload>      (payload may contain '|')
      REPORT|<json>                              (subscriber latency summary per topic)
      STATS|                                     (ask for a snapshot)
  broker -> client:
      OK|<info>                                  (reply to SUBSCRIBE/UNSUBSCRIBE/HELLO)
      ERR|<reason>
      MSG|<topic>|<timestamp>|<payload>          (delivered to subscribers)
      STATS|<json>                               (reply to STATS)

The broker keeps a pub/sub table (topic -> publishers, subscribers) and counters per
topic and per client, and merges the latency reports subscribers send back.
"""
import argparse
import json
import logging
import socket
import threading
import time

import report


class Rec:
    """Per-client-name record. Survives disconnects so final stats stay visible."""

    def __init__(self, name):
        self.name, self.role = name, "?"
        self.conns = 0
        self.topics = set()                   # topics subscribed (last known)
        self.msgs_in = self.msgs_out = self.bytes_in = self.bytes_out = 0
        self.lat = {}                         # topic -> {n, mean, p50, p95, min, max}


class TopicStat:
    def __init__(self):
        self.publishers = set()
        self.msgs = self.bytes = self.deliveries = 0
        self.first = self.last = None


class Client:
    """Wraps a connection; a per-client lock stops two threads interleaving
    their writes on the same socket."""

    def __init__(self, conn, addr):
        self.conn, self.addr = conn, addr
        self.send_lock = threading.Lock()
        self.rec = None

    def send(self, line):
        with self.send_lock:
            self.conn.sendall((line + "\n").encode("utf-8"))


class Broker:
    def __init__(self, host, port):
        self.host, self.port = host, port
        self.topics = {}                 # topic -> set(Client)
        self.tstats = {}                 # topic -> TopicStat
        self.recs = {}                   # name -> Rec
        self.lock = threading.Lock()     # protects everything above + stats
        self.stats = {"published": 0, "delivered": 0, "failed": 0}
        self.started = time.time()

    # ---- client registry ------------------------------------------------
    def register(self, client, role, name):
        with self.lock:
            old = client.rec
            if old and old.name != name:
                old.conns -= 1
                if old.conns <= 0 and not old.msgs_in and not old.msgs_out:
                    self.recs.pop(old.name, None)
            rec = self.recs.setdefault(name, Rec(name))
            if old is not rec:
                rec.conns += 1
            rec.role = role
            client.rec = rec
        return rec

    # ---- subscription table ---------------------------------------------
    def subscribe(self, topic, client):
        with self.lock:
            self.topics.setdefault(topic, set()).add(client)
            self.tstats.setdefault(topic, TopicStat())
            client.rec.topics.add(topic)

    def unsubscribe(self, topic, client):
        with self.lock:
            subs = self.topics.get(topic)
            if subs:
                subs.discard(client)
                if not subs:
                    del self.topics[topic]
            client.rec.topics.discard(topic)

    def remove_everywhere(self, client):
        with self.lock:
            for topic in list(self.topics):
                self.topics[topic].discard(client)
                if not self.topics[topic]:
                    del self.topics[topic]

    # ---- routing --------------------------------------------------------
    def publish(self, client, topic, ts, payload):
        nbytes = len(payload.encode("utf-8"))
        now = time.time()
        with self.lock:                          # copy, then send outside lock
            targets = list(self.topics.get(topic, ()))
            st = self.tstats.setdefault(topic, TopicStat())
            st.publishers.add(client.rec.name)
            st.msgs += 1
            st.bytes += nbytes
            st.first = st.first or now
            st.last = now
            client.rec.msgs_in += 1
            client.rec.bytes_in += nbytes
            self.stats["published"] += 1
        line = f"MSG|{topic}|{ts}|{payload}"
        ok = []
        for c in targets:
            try:
                c.send(line)
                ok.append(c)
            except OSError:
                self.remove_everywhere(c)
                with self.lock:
                    self.stats["failed"] += 1
        with self.lock:
            st.deliveries += len(ok)
            self.stats["delivered"] += len(ok)
            for c in ok:
                c.rec.msgs_out += 1
                c.rec.bytes_out += nbytes
        logging.debug("PUBLISH %s from %s -> %d subscribers", topic, client.rec.name, len(ok))

    # ---- statistics -----------------------------------------------------
    def merge_report(self, client, text):
        try:
            data = json.loads(text)
            assert isinstance(data, dict)
        except (ValueError, AssertionError):
            raise ValueError("bad REPORT json")
        with self.lock:
            client.rec.lat.update(data)

    @staticmethod
    def _merge(summaries):
        """Combine latency summaries: weighted mean, worst-case p95, overall max."""
        s = [x for x in summaries if x and x.get("n")]
        if not s:
            return None
        n = sum(x["n"] for x in s)
        return {"n": n, "mean": sum(x["mean"] * x["n"] for x in s) / n,
                "p95": max(x["p95"] for x in s), "max": max(x["max"] for x in s)}

    def snapshot(self):
        with self.lock:
            topics = []
            for t in sorted(set(self.tstats) | set(self.topics)):
                st = self.tstats.get(t) or TopicStat()
                span = (st.last - st.first) if st.first and st.last else 0
                topics.append({
                    "topic": t,
                    "publishers": sorted(st.publishers),
                    "subscribers": sorted({c.rec.name for c in self.topics.get(t, ())}),
                    "msgs": st.msgs, "bytes": st.bytes, "deliveries": st.deliveries,
                    "rate": (st.msgs - 1) / span if span > 0 else 0.0,
                    "latency": self._merge([r.lat.get(t) for r in self.recs.values()]),
                })
            clients = []
            for r in sorted(self.recs.values(), key=lambda r: (r.role, r.name)):
                if r.role == "admin":            # monitoring connections aren't pub/sub clients
                    continue
                clients.append({
                    "name": r.name, "role": r.role, "connected": r.conns > 0,
                    "topics": sorted(r.topics), "msgs_in": r.msgs_in, "msgs_out": r.msgs_out,
                    "bytes_in": r.bytes_in, "bytes_out": r.bytes_out,
                    "latency": self._merge(list(r.lat.values())),
                })
            broker = dict(self.stats, uptime=time.time() - self.started,
                          connected=sum(1 for r in self.recs.values()
                                        if r.conns > 0 and r.role != "admin"))
            return {"broker": broker, "topics": topics, "clients": clients}

    def print_report(self):
        print("\n" + report.fmt_all(self.snapshot()) + "\n", flush=True)

    # ---- per-connection thread -------------------------------------------
    def handle(self, conn, addr):
        client = Client(conn, addr)
        self.register(client, "?", f"{addr[0]}:{addr[1]}")
        logging.info("connected: %s:%d", *addr)
        try:
            f = conn.makefile("r", encoding="utf-8", newline="\n")
            for raw in f:                        # readline loop = message framing
                line = raw.rstrip("\n")
                if not line:
                    continue
                parts = line.split("|", 3)
                cmd = parts[0].upper()
                try:
                    if cmd == "HELLO" and len(parts) >= 3:
                        rec = self.register(client, parts[1], parts[2])
                        client.send(f"OK|hello {rec.name}")
                        logging.info("registered %s (%s) from %s:%d", rec.name, rec.role, *addr)
                    elif cmd == "SUBSCRIBE" and len(parts) >= 2:
                        self.subscribe(parts[1], client)
                        client.send(f"OK|subscribed {parts[1]}")
                        logging.info("%s subscribed to %s", client.rec.name, parts[1])
                    elif cmd == "UNSUBSCRIBE" and len(parts) >= 2:
                        self.unsubscribe(parts[1], client)
                        client.send(f"OK|unsubscribed {parts[1]}")
                        logging.info("%s unsubscribed from %s", client.rec.name, parts[1])
                    elif cmd == "PUBLISH" and len(parts) == 4:
                        self.publish(client, parts[1], parts[2], parts[3])
                    elif cmd == "REPORT" and "|" in line:
                        self.merge_report(client, line.split("|", 1)[1])
                    elif cmd == "STATS":
                        client.send("STATS|" + json.dumps(self.snapshot()))
                    else:
                        client.send("ERR|bad command")
                except ValueError as e:
                    client.send(f"ERR|{e}")
                except OSError:
                    break
        except (ConnectionResetError, UnicodeDecodeError):
            pass
        finally:
            self.remove_everywhere(client)
            with self.lock:
                client.rec.conns -= 1
            conn.close()
            logging.info("disconnected: %s (%s:%d)", client.rec.name, *addr)

    def table_loop(self, interval):
        while True:
            time.sleep(interval)
            self.print_report()

    # ---- accept loop ------------------------------------------------------
    def serve(self, table_interval=0):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(128)
        logging.info("broker listening on %s:%d", self.host, self.port)
        if table_interval:
            threading.Thread(target=self.table_loop, args=(table_interval,), daemon=True).start()
        try:
            while True:
                conn, addr = srv.accept()
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                threading.Thread(target=self.handle, args=(conn, addr),
                                 daemon=True).start()
        except KeyboardInterrupt:
            logging.info("shutting down")
            self.print_report()
        finally:
            srv.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--table-interval", type=float, default=0,
                    help="print pub/sub table + stats every N seconds (0 = only at shutdown)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    Broker(a.host, a.port).serve(a.table_interval)
