#!/usr/bin/env python3
"""Minimal streamable-HTTP MCP server exposing the same permission-prompt tool.
Probes whether --permission-prompt-tool works against an HTTP endpoint (the shape
Horde's per-agent MCP endpoint would take) rather than a stdio child."""
import json, os, sys, datetime, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = os.environ.get("HORDE_PERM_LOG", "/tmp/horde-perm-http.log")
DECISION = os.environ.get("HORDE_PERM_DECISION", "allow")
PORT = int(os.environ.get("HORDE_PERM_PORT", "8931"))
SESSION = uuid.uuid4().hex

def log(kind, payload):
    with open(LOG, "a") as f:
        f.write(json.dumps({"ts": datetime.datetime.now().isoformat(), "kind": kind, "payload": payload}) + "\n")

TOOL = {
    "name": "permission_prompt",
    "description": "Horde permission chain terminus over HTTP.",
    "inputSchema": {"type": "object",
                    "properties": {"tool_name": {"type": "string"}, "input": {"type": "object"},
                                   "tool_use_id": {"type": "string"}},
                    "required": ["tool_name", "input"]},
}

def handle(req):
    method, rid = req.get("method"), req.get("id")
    if method == "initialize":
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "protocolVersion": req.get("params", {}).get("protocolVersion", "2025-06-18"),
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "horde-perm-http", "version": "0.1.0"}}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": rid, "result": {"tools": [TOOL]}}
    if method == "tools/call":
        args = req.get("params", {}).get("arguments", {})
        decision = ({"behavior": "deny", "message": "Denied by horde HTTP permission chain"}
                    if DECISION == "deny"
                    else {"behavior": "allow", "updatedInput": args.get("input", {})})
        log("decision", {"args": args, "decision": decision})
        return {"jsonrpc": "2.0", "id": rid,
                "result": {"content": [{"type": "text", "text": json.dumps(decision)}], "isError": False}}
    if rid is None:
        return None
    return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"no method {method}"}}

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        try:
            req = json.loads(body)
        except json.JSONDecodeError:
            self.send_response(400); self.end_headers(); return
        log("recv", req)
        resp = handle(req)
        if resp is None:
            self.send_response(202)
            self.send_header("Mcp-Session-Id", SESSION)
            self.end_headers(); return
        data = json.dumps(resp).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Mcp-Session-Id", SESSION)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    def do_GET(self):
        self.send_response(405); self.end_headers()
    def do_DELETE(self):
        self.send_response(200); self.end_headers()

print(f"listening on {PORT}", file=sys.stderr, flush=True)
ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
