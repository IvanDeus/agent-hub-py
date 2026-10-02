#!/usr/bin/env python3
"""selftest.py - verify an Agent Hub satisfies the agent-facing contract (v1.0 -> v1.4).

HTTP-only: runs against any hub URL (test instance or live tunnel). Shape-locks
are supersets, never exact equality, so future additive fields pass.

    python3 selftest.py --server https://<hub> --token "$AGENT_AUTH_TOKEN" [--logtoken <LOG_SECRET_TOKEN>] [--agent <connected_id>]

Exit 0 = all checks passed, 1 = at least one failure."""

import argparse
import hashlib
import json
import secrets
import sys

import requests

REQUIRED_FEATURES = ["llms_txt", "api_manifest", "json_errors", "method_405",
                     "inbox_peek", "agents_detail", "upload_sha256", "autojson_name",
                     "file_index", "events_json", "result_lookup", "events_mine",
                     "client_source", "dedupe_uploads", "superseded_notice",
                     "single_result_log_rows", "standby_takeover_chain",
                     "result_inbox_status", "unique_agent_ids"]

PASS = []
FAIL = []


def check(name: str, cond: bool, detail: str = "") -> None:
    (PASS if cond else FAIL).append(name)
    mark = "PASS" if cond else "FAIL"
    print(f"{mark:4}  {name}" + (f"  <- {detail}" if not cond and detail else ""))


def main() -> int:
    ap = argparse.ArgumentParser(description="Agent Hub contract self-test")
    ap.add_argument("--server", required=True, help="hub base URL")
    ap.add_argument("--token", required=True, help="AGENT_AUTH_TOKEN")
    ap.add_argument("--logtoken", default="", help="LOG_SECRET_TOKEN for events.json checks")
    ap.add_argument("--agent", default="", help="an agent_id that is connected: live task round-trip")
    ap.add_argument("--timeout", type=float, default=20)
    args = ap.parse_args()

    s = args.server.rstrip("/")
    auth = {"Authorization": f"Bearer {args.token}", "X-Agent-Id": "selftest",
            "ngrok-skip-browser-warning": "true", "Accept": "application/json"}
    T = args.timeout

    # ---- discovery, unauthenticated
    r = requests.get(f"{s}/health", timeout=T)
    check("health 200 + core keys",
          r.ok and {"status", "agents_connected", "version", "features",
                    "uptime_seconds", "docs", "api"} <= set(r.json()), r.text[:120])
    if r.ok:
        h = r.json()
        check("health features advertise the full v1.4 contract",
              set(REQUIRED_FEATURES) <= set(h.get("features", [])),
              str(h.get("features")))
        check("health shape-lock (legacy keys)",
              {"status", "agents_connected", "uptime"} <= set(h))

    r = requests.get(f"{s}/api", timeout=T)
    manifest = r.json() if r.ok and "json" in r.headers.get("content-type", "") else {}
    check("api manifest 200 + sections",
          r.ok and {"version", "base_url", "endpoints", "socket",
                    "footguns", "features", "auth"} <= set(manifest), r.text[:120])

    r = requests.get(f"{s}/llms.txt", timeout=T)
    llms = r.text if r.ok else ""
    check("llms.txt 200 markdown",
          r.ok and r.headers.get("content-type", "").startswith("text/markdown"),
          f"{r.status_code} {r.headers.get('content-type')}")
    check("llms.txt sane size", 0 < len(llms) < 20000, f"{len(llms)} bytes")
    drift = [e["path"] for e in manifest.get("endpoints", []) if e["path"] not in llms]
    check("docs drift: every /api path appears in /llms.txt", not drift, str(drift))

    r = requests.get(f"{s}/", timeout=T)
    page = r.text if r.ok else ""
    check("onboarding 200 html", r.ok and "text/html" in r.headers.get("content-type", ""))
    check("voting removed from onboarding", "Vote for your own roles" not in page)

    # ---- auth negotiation
    r = requests.get(f"{s}/agents", timeout=T)  # no token
    ok_json = (r.status_code == 401 and "json" in r.headers.get("content-type", "")
               and {"error", "hint", "docs", "api", "example"} <= set(r.json()))
    check("401 tokenless = JSON with hint", ok_json, f"{r.status_code} {r.text[:80]}")
    r = requests.get(f"{s}/agents", timeout=T, headers={"Accept": "text/html"})
    check("401 browser = HTML onboarding",
          r.status_code == 401 and "text/html" in r.headers.get("content-type", ""))
    r = requests.get(f"{s}/agents", timeout=T, headers=auth)
    check("agents 200 + legacy + new keys",
          r.ok and {"agents", "count", "agent_ids", "detail", "standby", "standby_note"}
          <= set(r.json()), str(list(r.json())))
    ag = r.json() if r.ok else {}
    check("agents detail carries standby accounting",
          all({"sid", "connected_at", "last_superseded_at", "standby_sockets"} <= set(d)
              for d in ag.get("detail", {}).values()), str(ag.get("detail"))[:140])

    # ---- method + validation errors
    r = requests.post(f"{s}/agents", timeout=T, headers=auth)
    check("wrong verb = 405 JSON (not 500)",
          r.status_code == 405 and "json" in r.headers.get("content-type", ""),
          f"{r.status_code}")
    r = requests.post(f"{s}/agent/bad%20id/message", timeout=T, headers=auth, json={"text": "x"})
    check("bad agent_id = 400 + pattern",
          r.status_code == 400 and "pattern" in r.json(), r.text[:80])
    r = requests.post(f"{s}/agent/nobody-9x/message", timeout=T, headers=auth, json={"text": "x"})
    check("offline agent = 404 + start-one hint",
          r.status_code == 404 and "mock_agent.py" in r.json().get("hint", ""), r.text[:80])
    r = requests.post(f"{s}/relay", timeout=T, headers=auth, json={"to": "bad id"})
    check("relay bad body = 400", r.status_code == 400 and "error" in r.json())

    # ---- file store
    payload = json.dumps({"selftest": secrets.token_hex(4)}).encode()
    sha = hashlib.sha256(payload).hexdigest()
    name = "selftest.json"
    r = requests.post(f"{s}/file", timeout=T, headers=auth, files={"file": (name, payload)})
    up = r.json() if r.ok else {}
    check("upload multipart 201 + sha256",
          r.status_code == 201 and {"status", "file_id", "name", "size", "sha256",
                                    "delivered", "download_url"} <= set(up)
          and up.get("sha256") == sha, r.text[:120])
    fid = up.get("file_id", "")

    r = requests.post(f"{s}/file", timeout=T, headers=auth, files={"file": (name, payload)})
    check("same bytes = duplicate_of advisory",
          r.status_code == 201 and r.json().get("duplicate_of") == fid, r.text[:120])

    r = requests.post(f"{s}/file", timeout=T,
                      headers={**auth, "Content-Type": "application/json"},
                      data=payload)
    check("raw JSON body auto-names body.json",
          r.status_code == 201 and r.json().get("name") == "body.json", r.text[:120])

    r = requests.post(f"{s}/file", timeout=T, headers=auth,
                      files={"file": ("\u5831\u544a.json", payload)})
    bad = r.json() if r.status_code == 415 else {}
    check("non-ASCII name = 415 echo sanitized name",
          r.status_code == 415 and "name_after_sanitize" in bad, r.text[:120])

    r = requests.post(f"{s}/file", timeout=T,
                      headers={**auth, "X-Target-Agent": "nosuch-9x"},
                      files={"file": (name, payload)})
    tgt = r.json() if r.ok else {}
    check("bad X-Target-Agent = 201 + target_error",
          r.status_code == 201 and tgt.get("delivered") is False and "target_error" in tgt,
          r.text[:120])

    r = requests.get(f"{s}/file/{fid}", timeout=T, headers=auth)
    check("download bytes match sha", r.ok and hashlib.sha256(r.content).hexdigest() == sha)
    r = requests.get(f"{s}/file/deadbeef00000000", timeout=T, headers=auth)
    check("unknown file_id = 404 + hint",
          r.status_code == 404 and "hint" in r.json(), r.text[:80])
    r = requests.get(f"{s}/files", timeout=T, headers=auth)
    fl = r.json() if r.ok else {}
    check("files list: count + created_iso",
          r.ok and {"files", "count"} <= set(fl)
          and "created_iso" in fl.get("files", {}).get(fid, {}), r.text[:120])

    # ---- v1.2: upload dedupe (same bytes, ?dedupe=1 -> reuse newest matching id)
    r = requests.post(f"{s}/file?dedupe=1", timeout=T, headers=auth,
                      files={"file": (name, payload)})
    dd = r.json() if r.status_code in (200, 201) else {}
    check("dedupe=1 = 200 existing id, nothing written",
          r.status_code == 200 and dd.get("status") == "existing" and dd.get("deduped") is True
          and dd.get("bytes_stored") is False and dd.get("file_id") in fl.get("files", {}),
          r.text[:160])
    r = requests.post(f"{s}/file", timeout=T, headers=auth, files={"file": (name, payload)})
    check("dedupe is opt-in: no flag still mints a new id",
          r.status_code == 201 and r.json().get("status") == "stored", r.text[:120])

    # ---- v1.2: result correlation
    r = requests.get(f"{s}/result/not-a-real-msg-id", timeout=T, headers=auth)
    check("GET /result/<unknown> = 404 + hint",
          r.status_code == 404 and "hint" in r.json(), r.text[:100])

    # ---- v1.2: per-agent event feed (no log secret needed)
    r = requests.get(f"{s}/events/mine", timeout=T,
                     headers={k: v for k, v in auth.items() if k != "X-Agent-Id"})
    check("events/mine without X-Agent-Id = 400",
          r.status_code == 400 and "X-Agent-Id" in r.text, r.text[:100])
    r = requests.get(f"{s}/events/mine?limit=20", timeout=T, headers=auth)
    ev = r.json() if r.ok else {}
    check("events/mine 200 + own rows only",
          r.ok and {"events", "count", "total_matching", "caller"} <= set(ev)
          and ev.get("caller") == "selftest"
          and all("selftest" in (e.get("agent", "") + e.get("dir", "") + e.get("payload", ""))
                  for e in ev.get("events", [])), r.text[:160])

    # ---- v1.2: fetchable reference client
    r = requests.get(f"{s}/client.py", timeout=T, headers=auth)
    check("client.py = python source of the reference agent",
          r.ok and "python" in r.headers.get("content-type", "")
          and "class Agent" in r.text and "agent_to_agent" in r.text,
          f"{r.status_code} {r.headers.get('content-type')} {r.text[:60]!r}")

    # ---- v1.2: new endpoints documented
    paths = {e["path"] for e in manifest.get("endpoints", [])}
    missing = {p for p in ("/result/{msg_id}", "/events/mine", "/client.py") if p not in paths}
    check("manifest documents the v1.2 endpoints", not missing, str(missing))
    fg = " ".join(manifest.get("footguns", []))
    check("footguns cover sid scope, forgeable `from` and stable caller ids",
          "get_sid(namespace='/agents')" in fg and "not evidence of who posted it" in fg
          and "ONE stable X-Agent-Id" in fg, fg[:120])

    # ---- v1.4: unique agent_id pre-flight
    free = "selftest-free-" + secrets.token_hex(3)
    r = requests.get(f"{s}/agent-id/{free}", timeout=T, headers=auth)
    idst = r.json() if r.ok else {}
    check("GET /agent-id/<free> = available + full keys",
          r.ok and idst.get("available") is True and idst.get("taken_by_sid") is None
          and {"agent_id", "connected_at", "standby_sockets", "last_rejection", "note"}
          <= set(idst), r.text[:140])
    r = requests.get(f"{s}/agent-id/bad%20id", timeout=T, headers=auth)
    check("GET /agent-id/<malformed> = 400 + pattern",
          r.status_code == 400 and "pattern" in r.json(), r.text[:100])
    r = requests.get(f"{s}/agent-id/selftest-NOT-CONNECTED", timeout=T, headers=auth)
    check("unknown id = 200 available:true (never a 404 dead end)",
          r.status_code == 200 and r.json().get("available") is True, r.text[:100])
    check("manifest documents the v1.4 endpoint + uniqueness footgun",
          "/agent-id/{agent_id}" in paths and "unique since v1.4" in fg
          and "force_takeover" in fg, str(sorted(paths))[:120])

    # ---- inbox semantics (idle agent, peek must be non-destructive)
    r = requests.get(f"{s}/agent/selftest-idle/inbox?peek=true", timeout=T, headers=auth)
    ib = r.json() if r.ok else {}
    check("inbox peek 200 + new keys",
          r.ok and {"messages", "count", "drained", "queue_max", "agent_online",
                    "peek"} <= set(ib) and ib.get("peek") is True and ib.get("drained") is False,
          r.text[:120])

    # ---- logs
    r = requests.get(f"{s}/logs/wrong-token-9x", timeout=T)
    check("logs wrong token = 404", r.status_code == 404)
    r = requests.get(f"{s}/logs/wrong-token-9x/events.json", timeout=T)
    check("events.json wrong token = 404", r.status_code == 404)
    if args.logtoken:
        r = requests.get(f"{s}/logs/{args.logtoken}/events.json?limit=5", timeout=T)
        check("events.json 200 structured",
              r.ok and {"events", "count", "total", "newest_last"} <= set(r.json()), r.text[:120])

    # ---- optional live round-trip
    if args.agent:
        r = requests.get(f"{s}/agent-id/{args.agent}", timeout=T, headers=auth)
        st = r.json() if r.ok else {}
        check(f"GET /agent-id/'{args.agent}' reports taken + holder sid",
              r.ok and st.get("available") is False and st.get("taken_by_sid")
              and isinstance(st.get("standby_sockets"), int) and st.get("connected_at"),
              r.text[:140])
        r = requests.post(f"{s}/agent/{args.agent}/message", timeout=T + 40,
                          headers=auth, json={"text": "selftest ping"})
        m = r.json() if r.ok else {}
        check(f"task round-trip to '{args.agent}' replies",
              r.ok and m.get("status") == "replied" and m.get("reply"), r.text[:120])
        mid = m.get("msg_id", "")
        r = requests.get(f"{s}/result/{mid}", timeout=T, headers=auth)
        res = r.json() if r.ok else {}
        check(f"GET /result/<msg_id> correlates the reply to '{args.agent}'",
              r.ok and res.get("msg_id") == mid
              and res.get("agent") == f"agent:{args.agent}"
              and len(res.get("results", [])) >= 1
              and {"count", "note", "status", "updated"} <= set(res)
              and res["results"][-1] == m.get("reply"), r.text[:160])
        r = requests.get(f"{s}/events/mine", timeout=T,
                         headers={**auth, "X-Agent-Id": args.agent})
        check(f"events/mine as '{args.agent}' shows the task",
              r.ok and any(mid in e.get("payload", "") for e in r.json().get("events", [])),
              r.text[:160])
        if args.logtoken:
            r = requests.get(f"{s}/logs/{args.logtoken}/events.json?limit=3000", timeout=T)
            rows = [e for e in (r.json().get("events") if r.ok else []) or []
                    if mid in e.get("payload", "") and e.get("event") == "MSG_RCVD"]
            check("one MSG_RCVD log row per result (no duplicates)",
                  len(rows) == 1, str(rows)[:200])

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("failed checks: " + ", ".join(FAIL), file=sys.stderr)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
