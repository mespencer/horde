#!/usr/bin/env python3
"""Drive the real codex CLI against the spike's rmcp endpoint.

`codex exec` turns out to be unusable for this: it cancels every MCP tool call
with "user cancelled MCP tool call" before a byte reaches the server, because
the exec front-end never declares the client capability that would let it
answer codex's MCP tool-call approval request. The app-server path does declare
it, so that is the driving path here -- the same one
docs/spikes/codex-app-server.md settled on for the W2c spike.

Usage:
    drive_codex.py <codex-home> <cwd> <prompt> [--enable FEATURE] [--disable FEATURE]
"""

import json
import os
import pathlib
import select
import subprocess
import sys
import threading
import time

GRANULAR = {
    "granular": {
        "mcp_elicitations": True,
        "rules": True,
        "sandbox_approval": True,
        "request_permissions": True,
        "skill_approval": True,
    }
}


class AppServer:
    def __init__(self, extra_args):
        self.proc = subprocess.Popen(
            ["codex", "app-server", "--stdio"] + extra_args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self._next_id = 0
        self.stderr = []
        threading.Thread(target=self._drain, daemon=True).start()

    def _drain(self):
        for line in self.proc.stderr:
            self.stderr.append(line.rstrip())

    def send(self, msg):
        self.proc.stdin.write(json.dumps(msg) + "\n")
        self.proc.stdin.flush()

    def request(self, method, params):
        self._next_id += 1
        self.send({"jsonrpc": "2.0", "id": self._next_id, "method": method, "params": params})
        return self._next_id

    def notify(self, method, params=None):
        msg = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        self.send(msg)

    def read(self, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            ready, _, _ = select.select(
                [self.proc.stdout], [], [], max(0.1, deadline - time.time())
            )
            if not ready:
                return None
            line = self.proc.stdout.readline()
            if not line:
                return None
            line = line.strip()
            if not line:
                continue
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
        return None

    def close(self):
        try:
            self.proc.terminate()
            self.proc.wait(timeout=5)
        except Exception:
            self.proc.kill()


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        return 2
    codex_home, cwd, prompt = sys.argv[1], sys.argv[2], sys.argv[3]
    extra = []
    args = sys.argv[4:]
    while args:
        if args[0] in ("--enable", "--disable") and len(args) > 1:
            extra += [args[0], args[1]]
            args = args[2:]
        else:
            args = args[1:]

    os.environ["CODEX_HOME"] = codex_home
    pathlib.Path(cwd).mkdir(parents=True, exist_ok=True)

    srv = AppServer(extra)
    try:
        rid = srv.request(
            "initialize",
            {
                "clientInfo": {"name": "horde-rmcp-spike", "version": "0.1.0"},
                "capabilities": {"experimentalApi": True},
            },
        )
        while True:
            msg = srv.read(timeout=30)
            if msg is None:
                print("NO-INITIALIZE-RESPONSE")
                return 1
            if msg.get("id") == rid:
                srv.notify("initialized")
                break

        rid = srv.request(
            "thread/start",
            {
                "cwd": cwd,
                "approvalPolicy": GRANULAR,
                "approvalsReviewer": "user",
                "sandbox": "read-only",
            },
        )
        thread = None
        deadline = time.time() + 60
        while time.time() < deadline:
            msg = srv.read(timeout=30)
            if msg is None:
                break
            if msg.get("id") == rid and "result" in msg:
                result = msg["result"]
                thread = result.get("threadId") or (result.get("thread") or {}).get("id")
                break
        if not thread:
            print("NO-THREAD")
            return 1

        srv.request("turn/start", {"threadId": thread, "input": [{"type": "text", "text": prompt}]})

        # Accept every server->client request: the point of this run is to let
        # the MCP tool call reach the wire, not to exercise the approval gate.
        deadline = time.time() + 240
        while time.time() < deadline:
            msg = srv.read(timeout=30)
            if msg is None:
                continue
            method = msg.get("method")
            if method and "id" in msg:
                print(f"SERVER-REQUEST {method} {json.dumps(msg.get('params'))[:400]}")
                # MCP tool-call approvals arrive as an MCP elicitation, whose
                # result is `action`, not the `decision` the sandbox/exec
                # approvals use.
                if "elicitation" in method:
                    result = {"action": "accept", "content": {}}
                else:
                    result = {"decision": "accept"}
                srv.send({"jsonrpc": "2.0", "id": msg["id"], "result": result})
                continue
            if method in ("item/started", "item/completed", "item/updated"):
                item = (msg.get("params") or {}).get("item") or {}
                if "mcp" in str(item.get("type", "")).lower():
                    print(f"MCP-ITEM {method} {json.dumps(item)[:2000]}")
            if method == "turn/completed":
                print("TURN-COMPLETED")
                return 0
            if method == "turn/failed":
                print(f"TURN-FAILED {json.dumps(msg.get('params'))[:600]}")
                return 1
        print("TIMEOUT")
        return 1
    finally:
        srv.close()
        if srv.stderr:
            print("STDERR:", "\n".join(srv.stderr[-15:]))


if __name__ == "__main__":
    raise SystemExit(main())
