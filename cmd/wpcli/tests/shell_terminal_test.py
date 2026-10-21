#!/usr/bin/env python3
"""Validate a built wp shell in a Unix pseudo-terminal using local fixtures only.

Run: python3 cmd/wpcli/tests/shell_terminal_test.py --binary ./bin/wp
Requires Python 3.9+ on Linux or macOS; uses only the Python standard library.
"""
import argparse
import errno
import fcntl
import http.server
import json
import os
import pathlib
import pty
import select
import signal
import struct
import tempfile
import termios
import threading
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--binary", required=True)
BINARY = str(pathlib.Path(parser.parse_args().binary).resolve())
stall, started, release = threading.Event(), threading.Event(), threading.Event()
posts = []

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if stall.is_set():
            started.set()
            release.wait(10)
        port = self.server.server_port
        payload = json.dumps({"members": [{"id": "node-0", "service_addr": f"127.0.0.1:{port}", "tags": {"admin_port": str(port)}}]}).encode()
        try:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self):
        posts.append(self.path)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"{}")

class Terminal:
    def __init__(self, config):
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            env = dict(os.environ, WOODPECKER_CLI_CONFIG=str(config), WOODPECKER_ENDPOINT="http://wrong-cluster:1", TERM="xterm")
            os.execve(BINARY, [BINARY, "shell"], env)
        fcntl.ioctl(self.fd, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 160, 0, 0))
        self.pending = b""
    def send(self, data):
        os.write(self.fd, data)

    def expect(self, needle, timeout=6):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if needle in self.pending:
                index = self.pending.index(needle) + len(needle)
                data, self.pending = self.pending[:index], self.pending[index:]
                return data
            readable, _, _ = select.select([self.fd], [], [], 0.1)
            if readable:
                try:
                    chunk = os.read(self.fd, 65536)
                except OSError as exc:
                    if exc.errno != errno.EIO:
                        raise
                    break
                if not chunk:
                    break
                self.pending += chunk
        raise AssertionError(f"Missing {needle!r}; output={self.pending!r}")
    def exited(self):
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if pid:
                self.pid = 0
                assert os.waitstatus_to_exitcode(status) == 0, status
                return
            time.sleep(0.05)
        raise AssertionError("shell did not exit")
    def close(self):
        if self.pid:
            os.kill(self.pid, signal.SIGTERM)
            os.waitpid(self.pid, 0)
        os.close(self.fd)

server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
server.daemon_threads = True
threading.Thread(target=server.serve_forever, daemon=True).start()
results = []
try:
    with tempfile.TemporaryDirectory(prefix="wp-shell-pty-") as folder:
        config = pathlib.Path(folder) / "cli.yaml"
        original = f"""current-context: prod
contexts:
  prod:
    endpoint: http://127.0.0.1:{server.server_port}
    etcd:
      endpoints: [old-tenant:2379]
      meta_prefix: old/woodpecker
  dev:
    endpoint: http://127.0.0.1:1
"""
        config.write_text(original)
        config.chmod(0o600)
        terminal = Terminal(config)
        try:
            output = terminal.expect(b"Select context by number or name [2]:")
            assert b"1) dev" in output and b"2) prod" in output
            terminal.send(b"\n")
            terminal.expect(b"wp[context=prod | meta=disconnected]>")
            results.append("selector sorts contexts and Enter uses current context")
            terminal.send(b"node li\t\n")
            terminal.expect(b"wp[context=prod | meta=disconnected]>")
            results.append("Tab completes an operational subcommand")
            terminal.send(b"version\n")
            terminal.expect(b"wp version")
            terminal.expect(b"wp[context=prod | meta=disconnected]>")
            terminal.send(b"\x1b[A\n")
            terminal.expect(b"wp version")
            terminal.expect(b"wp[context=prod | meta=disconnected]>")
            results.append("Up arrow recalls a previous command")
            terminal.send(b"connect ignored --etcd-password test-secret\n")
            terminal.expect(b"wp[context=prod | meta=disconnected]>")
            terminal.send(b"\x1b[A\n")
            terminal.expect(b"wp version")
            terminal.expect(b"wp[context=prod | meta=disconnected]>")
            results.append("credential-bearing connection commands are excluded from recall")
            terminal.send(b"node cancel-decommission node-0\n")
            terminal.expect(b"Proceed? [y/N]:")
            terminal.send(b"y\n")
            terminal.expect(b"decommission cancelled")
            terminal.expect(b"wp[context=prod | meta=disconnected]>")
            assert len(posts) == 1
            results.append("existing interactive confirmations receive terminal input")
            stall.set()
            terminal.send(b"node list\n")
            assert started.wait(5), "operation did not reach Admin"
            terminal.send(b"\x03")
            terminal.expect(b"operation canceled; session retained")
            terminal.expect(b"wp[context=prod | meta=disconnected]>")
            release.set()
            stall.clear()
            terminal.send(b"version\n")
            terminal.expect(b"wp version")
            terminal.expect(b"wp[context=prod | meta=disconnected]>")
            results.append("Ctrl+C cancels running request and retains usable session")
            terminal.send(b"exit\n")
            terminal.exited()
        finally:
            terminal.close()
        for key, label in [(b"\x03", "Ctrl+C"), (b"\x04", "EOF")]:
            terminal = Terminal(config)
            try:
                terminal.expect(b"Select context by number or name [2]:")
                terminal.send(key)
                terminal.exited()
                results.append(f"{label} cancels context selection cleanly")
            finally:
                terminal.close()
        assert config.read_text() == original
        results.append("original cli.yaml is unchanged")
finally:
    release.set()
    server.shutdown()
    server.server_close()
print(json.dumps({"passed": results}, indent=2))
