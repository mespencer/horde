#!/usr/bin/env python3
"""Probe driver: spawn claude in stream-json mode, send N user messages on ONE stdin,
capture every line in both directions."""
import json, subprocess, sys, threading, time, os, signal

def main():
    out_path = sys.argv[1]
    n_turns = int(sys.argv[2])
    prompts = sys.argv[3:3+n_turns]
    extra = sys.argv[3+n_turns:]

    cmd = ["claude", "-p", "--input-format", "stream-json", "--output-format", "stream-json",
           "--verbose", "--model", "haiku", "--setting-sources", "", "--strict-mcp-config"] + extra
    print("CMD:", " ".join(repr(c) for c in cmd), file=sys.stderr)

    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True, bufsize=1)
    fout = open(out_path, "w")
    results = []
    done = threading.Event()

    def reader():
        for line in p.stdout:
            line = line.rstrip("\n")
            if not line:
                continue
            fout.write(json.dumps({"dir": "out", "t": round(time.time(), 3), "msg": json.loads(line)}) + "\n")
            fout.flush()
            m = json.loads(line)
            if m.get("type") == "result":
                results.append(m)
                done.set()
        done.set()

    th = threading.Thread(target=reader, daemon=True)
    th.start()

    for i, prompt in enumerate(prompts):
        msg = {"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": prompt}]}}
        fout.write(json.dumps({"dir": "in", "t": round(time.time(), 3), "msg": msg}) + "\n")
        fout.flush()
        done.clear()
        p.stdin.write(json.dumps(msg) + "\n")
        p.stdin.flush()
        if not done.wait(timeout=int(os.environ.get("DRIVE_TIMEOUT","180"))):
            print(f"TIMEOUT waiting for result of turn {i}", file=sys.stderr)
            break
        print(f"turn {i} -> result: {json.dumps(results[-1].get('result'))[:200]}", file=sys.stderr)

    p.stdin.close()
    try:
        p.wait(timeout=30)
    except subprocess.TimeoutExpired:
        print("process did NOT exit after stdin close; killing", file=sys.stderr)
        p.kill()
    err = p.stderr.read()
    if err.strip():
        print("STDERR:", err, file=sys.stderr)
    print("EXIT CODE:", p.returncode, file=sys.stderr)
    print("RESULTS:", len(results), file=sys.stderr)
    for r in results:
        print("  session_id:", r.get("session_id"), "| result:", json.dumps(r.get("result"))[:150], file=sys.stderr)
    fout.close()

main()
