"""Read-only recording relay for deployed UI acceptance; not a data fixture.

Normal responses are byte-for-byte backend responses. The explicitly named
hold control waits AFTER a real query returns, to observe downstream transport
cancellation. The deny control is a synthetic transport/privacy counterexample,
never evidence of an actual authentication outage. No logs or spans are pushed.
"""
from contextlib import AbstractContextManager
import http.server
import json
import select
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


class Probe(AbstractContextManager):
    MAX_RESPONSE = 4 << 20
    MAX_REQUESTS = 512

    def __init__(self, endpoints, token, error_marker):
        self.endpoints = dict(endpoints)
        self.token, self.error_marker = token, error_marker
        self.records, self.lock = [], threading.Lock()
        self.stopping = threading.Event()
        for endpoint in endpoints.values():
            u = urllib.parse.urlsplit(endpoint)
            if u.scheme != "http" or u.hostname != "127.0.0.1" or not u.port or u.path or u.query or u.fragment or u.username:
                raise ValueError("Probe requires explicit loopback port-forwards")

    def snapshot(self):
        with self.lock:
            return [dict(row) for row in self.records]

    def __enter__(self):
        probe = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                if self.path == "/stats":
                    self.reply(200, json.dumps(probe.snapshot()).encode())
                    return
                parts = self.path.split("/", 3)
                if len(parts) != 4 or parts[1] not in ("normal", "hold", "deny") or parts[2] not in probe.endpoints:
                    self.reply(404, b"unknown acceptance route")
                    return
                mode, source, tail = parts[1:]
                path = "/" + tail
                allowed = {"prometheus": ("/api/v1/query", "/api/v1/query_range"),
                           "trace": ("/api/traces",), "loki": ("/loki/api/v1/query_range",)}
                resource = urllib.parse.urlsplit(path).path
                if not any(resource == prefix or source == "trace" and resource.startswith(prefix + "/") for prefix in allowed[source]):
                    self.reply(403, b"read-only diagnostic routes only")
                    return
                with probe.lock:
                    if len(probe.records) >= probe.MAX_REQUESTS:
                        self.reply(429, b"acceptance request budget")
                        return
                    row = {"source": source, "mode": mode, "started_at": time.time(),
                           "authenticated": self.headers.get("Authorization") == "Bearer " + probe.token}
                    probe.records.append(row)
                if mode == "deny":
                    self.reply(401, probe.error_marker.encode())
                    with probe.lock:
                        row.update(status=401, finished_at=time.time(), controlled_transport=True)
                    return
                try:
                    request = urllib.request.Request(probe.endpoints[source] + path,
                        headers={"Authorization": self.headers.get("Authorization", "")})
                    try:
                        response = urllib.request.urlopen(request, timeout=8)
                    except urllib.error.HTTPError as error:
                        response = error
                    with response:
                        body = response.read(probe.MAX_RESPONSE + 1)
                        if len(body) > probe.MAX_RESPONSE:
                            raise RuntimeError("Acceptance backend response budget exceeded")
                        code, content_type = response.code, response.headers.get("Content-Type", "application/json")
                    with probe.lock:
                        row.update(status=code, response_bytes=len(body), backend_finished_at=time.time())
                    if mode == "hold":
                        # The actual backend query has completed. Only its return
                        # transport is delayed; do not claim server computation
                        # was still running or that upstream cancellation was seen.
                        deadline = time.monotonic() + 20
                        while not probe.stopping.is_set() and time.monotonic() < deadline:
                            readable, _, _ = select.select([self.connection], [], [], .05)
                            if readable and not self.connection.recv(1, socket.MSG_PEEK):
                                with probe.lock:
                                    row.update(client_disconnected_at=time.time(), finished_at=time.time())
                                return
                        with probe.lock:
                            row["hold_expired"] = True
                    self.reply(code, body, content_type)
                except (BrokenPipeError, ConnectionResetError):
                    with probe.lock:
                        row["client_disconnected_at"] = time.time()
                except Exception:
                    with probe.lock:
                        row["relay_failed"] = True
                    self.reply(502, b"acceptance relay failed; values withheld")
                finally:
                    with probe.lock:
                        row["finished_at"] = time.time()

            def reply(self, code, body, content_type="application/json"):
                self.send_response(code)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = "http://127.0.0.1:" + str(self.server.server_port)
        return self

    def __exit__(self, *_):
        self.stopping.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
