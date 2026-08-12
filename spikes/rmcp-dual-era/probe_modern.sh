#!/usr/bin/env bash
# Hand-driven 2026-07-28 stateless probes against the spike endpoint.
#
# No `initialize`. Version, capabilities and identity ride per-request `_meta`
# (`io.modelcontextprotocol/...`), and the SEP-2243 routing headers
# (MCP-Protocol-Version / Mcp-Method / Mcp-Name) accompany every POST.
#
# Usage: probe_modern.sh <url>
set -u
URL="${1:-http://127.0.0.1:8931/mcp}"
V=2026-07-28
BASE=(-sS -i -X POST "$URL"
  -H 'Content-Type: application/json'
  -H 'Accept: application/json, text/event-stream'
  -H 'Authorization: Bearer horde-agent-42-token'
  -H 'X-Horde-Agent: agent-42')

META='"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{},"io.modelcontextprotocol/clientInfo":{"name":"horde-modern-probe","version":"0.1.0"}}'

echo "===== M1 server/discover (mandatory in 2026-07-28)"
curl "${BASE[@]}" -H "MCP-Protocol-Version: $V" -H 'Mcp-Method: server/discover' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"server/discover\",\"params\":{$META}}"
echo; echo

echo "===== M2 tools/list, full modern metadata"
curl "${BASE[@]}" -H "MCP-Protocol-Version: $V" -H 'Mcp-Method: tools/list' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{$META}}"
echo; echo

echo "===== M3 tools/call with Mcp-Name"
curl "${BASE[@]}" -H "MCP-Protocol-Version: $V" -H 'Mcp-Method: tools/call' -H 'Mcp-Name: whoami' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"whoami\",\"arguments\":{},$META}}"
echo; echo

echo "===== M4 Mcp-Method disagrees with body method -> expect -32020 HeaderMismatch"
curl "${BASE[@]}" -H "MCP-Protocol-Version: $V" -H 'Mcp-Method: tools/list' -H 'Mcp-Name: whoami' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\",\"params\":{\"name\":\"whoami\",\"arguments\":{},$META}}"
echo; echo

echo "===== M5 Mcp-Name disagrees with body tool name -> expect -32020"
curl "${BASE[@]}" -H "MCP-Protocol-Version: $V" -H 'Mcp-Method: tools/call' -H 'Mcp-Name: echo' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/call\",\"params\":{\"name\":\"whoami\",\"arguments\":{},$META}}"
echo; echo

echo "===== M6 modern routing headers but NO _meta protocol version"
curl "${BASE[@]}" -H "MCP-Protocol-Version: $V" -H 'Mcp-Method: tools/list' \
  -d '{"jsonrpc":"2.0","id":6,"method":"tools/list","params":{}}'
echo; echo

echo "===== M7 _meta version but NO routing headers at all"
curl "${BASE[@]}" \
  -d "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"tools/list\",\"params\":{$META}}"
echo; echo

echo "===== M8 tools/call with NO Mcp-Name (required for tools/call by SEP-2243)"
curl "${BASE[@]}" -H "MCP-Protocol-Version: $V" -H 'Mcp-Method: tools/call' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"tools/call\",\"params\":{\"name\":\"echo\",\"arguments\":{\"message\":\"MODERN_OK\"},$META}}"
echo; echo

echo "===== M9 legacy initialize on the SAME instance, immediately after the modern traffic"
curl "${BASE[@]}" \
  -d '{"jsonrpc":"2.0","id":9,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"horde-legacy-probe","version":"0.1.0"}}}'
echo; echo

echo "===== M10 GET stream (removed in 2026-07-28) with modern version header"
curl -sS -i --max-time 3 -X GET "$URL" -H 'Accept: text/event-stream' \
  -H 'Authorization: Bearer horde-agent-42-token' -H 'X-Horde-Agent: agent-42' \
  -H "MCP-Protocol-Version: $V"
echo; echo

echo "===== M11 Origin validation (allowed_origins empty by default)"
curl "${BASE[@]}" -H 'Origin: https://evil.example.com' \
  -H "MCP-Protocol-Version: $V" -H 'Mcp-Method: tools/list' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":11,\"method\":\"tools/list\",\"params\":{$META}}"
echo; echo

echo "===== M12 Host header spoof (allowed_hosts loopback-only by default)"
curl "${BASE[@]}" -H 'Host: evil.example.com' \
  -H "MCP-Protocol-Version: $V" -H 'Mcp-Method: tools/list' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":12,\"method\":\"tools/list\",\"params\":{$META}}"
echo; echo
