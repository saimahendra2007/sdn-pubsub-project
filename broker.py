#!/usr/bin/env python3
"""
Pub-Sub broker over raw TCP sockets.

Protocol (one message per line, '\n' terminated, UTF-8):
  client -> broker:
      SUBSCRIBE|<topic>
      UNSUBSCRIBE|<topic>
      PUBLISH|<topic>|<timestamp>|<payload>      (payload may contain '|')
  broker -> client:
      OK|<info>                                  (reply to SUBSCRIBE/UNSUBSCRIBE)
      ERR|<reason>
      MSG|<topic>|<timestamp>|<payload>          (delivered to subscribers)
"""
import argparse
import logging
import socket
import threading


class Client:
    """Wraps a connection; a per-client lock stops two threads interleaving
    their writes on the same socket."""

    def __init__(self, conn, addr):
        self.conn, self.addr = conn, addr
        self.send_lock = threading.Lock()

    def send(self, line):
        with self.send_lock:
            self.conn.sendall((line + "\n").encode("utf-8"))


class Broker:
    def __init__(self, host, port):
        self.host, self.port = host, port
        self.topics = {}                 # topic -> set(Client)
        self.lock = threading.Lock()     # protects self.topics
        self.stats = {"published": 0, "delivered": 0}

    # ---- subscription table -------------------------------------------
    def subscribe(self, topic, client):
        with self.lock:
            self.topics.setdefault(topic, set()).add(client)

    def unsubscribe(self, topic, client):
        with self.lock:
            subs = self.topics.get(topic)
            if subs:
                subs.discard(client)
                if not subs:
                    del self.topics[topic]

    def remove_everywhere(self, client):
        with self.lock:
            for topic in list(self.topics):
                self.topics[topic].discard(client)
                if not self.topics[topic]:
                    del self.topics[topic]

    # ---- routing --------------------------------------------------------
    def publish(self, topic, ts, payload):
        with self.lock:                          # copy, then send outside lock
            targets = list(self.topics.get(topic, ()))
        self.stats["published"] += 1
        line = f"MSG|{topic}|{ts}|{payload}"
        for c in targets:
            try:
                c.send(line)
                self.stats["delivered"] += 1
            except OSError:
                self.remove_everywhere(c)
        logging.debug("PUBLISH %s -> %d subscribers", topic, len(targets))

    # ---- per-connection thread -------------------------------------------
    def handle(self, conn, addr):
        client = Client(conn, addr)
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
                    if cmd == "SUBSCRIBE" and len(parts) >= 2:
                        self.subscribe(parts[1], client)
                        client.send(f"OK|subscribed {parts[1]}")
                    elif cmd == "UNSUBSCRIBE" and len(parts) >= 2:
                        self.unsubscribe(parts[1], client)
                        client.send(f"OK|unsubscribed {parts[1]}")
                    elif cmd == "PUBLISH" and len(parts) == 4:
                        self.publish(parts[1], parts[2], parts[3])
                    else:
                        client.send("ERR|bad command")
                except OSError:
                    break
        except (ConnectionResetError, UnicodeDecodeError):
            pass
        finally:
            self.remove_everywhere(client)
            conn.close()
            logging.info("disconnected: %s:%d", *addr)

    # ---- accept loop ------------------------------------------------------
    def serve(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(128)
        logging.info("broker listening on %s:%d", self.host, self.port)
        try:
            while True:
                conn, addr = srv.accept()
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                threading.Thread(target=self.handle, args=(conn, addr),
                                 daemon=True).start()
        except KeyboardInterrupt:
            logging.info("shutting down, stats=%s", self.stats)
        finally:
            srv.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(message)s")
    Broker(a.host, a.port).serve()
