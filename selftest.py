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
import threading
import time

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
    ap.add_argument("--socket", action="store_true",
                    help="run the live socket section (needs python-socketio): verifies the "
                         "v1.4 duplicate-id refusal under concurrency and that a task caller "
                         "can poll its own GET /result row with its agent credential")
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

    # ---- v1.5.2: DELETE /file/<id> exists, is scoped, and actually reclaims
    r = requests.delete(f"{s}/file/deadbeef00000000", timeout=T, headers=auth)
    check("DELETE unknown file_id = 404 + hint",
          r.status_code == 404 and "hint" in r.json(), r.text[:100])
    r = requests.delete(f"{s}/file/deadbeef00000000", timeout=T,
                        headers={"Accept": "application/json",
                                 "ngrok-skip-browser-warning": "true"})
    check("DELETE tokenless = 401", r.status_code == 401, f"{r.status_code}")
    throw = json.dumps({"throwaway": secrets.token_hex(4)}).encode()
    r = requests.post(f"{s}/file", timeout=T, headers=auth,
                      files={"file": ("del-me.json", throw)})
    fid2 = r.json().get("file_id", "") if r.status_code == 201 else ""
    r = requests.delete(f"{s}/file/{fid2}", timeout=T, headers=auth)
    check("operator DELETE of own upload = 200 deleted + freed_bytes",
          bool(fid2) and r.status_code == 200 and r.json().get("status") == "deleted"
          and r.json().get("freed_bytes") == len(throw), r.text[:140])
    r = requests.get(f"{s}/file/{fid2}", timeout=T, headers=auth)
    check("deleted file_id 404s on download", r.status_code == 404, f"{r.status_code}")
    r = requests.get(f"{s}/files", timeout=T, headers=auth)
    check("deleted id gone from GET /files", bool(fid2)
          and fid2 not in r.json().get("files", {}))

    # ---- v1.7: retention / housekeeping (GET /retention, POST /retention/sweep)
    r = requests.get(f"{s}/retention", timeout=T, headers=auth)
    rep = r.json() if r.status_code == 200 else {}
    check("GET /retention = 200 dry-run report",
          r.status_code == 200 and rep.get("dry_run") is True
          and "removed" in rep and "retention_days" in rep, r.text[:150])
    check("retention report breaks down every surface",
          {"files", "bytes_freed", "ledger_rows", "log_rows", "inbox_messages",
           "dead_letter_rows"} <= set(rep.get("removed", {})), str(rep.get("removed")))
    check("retention config is reported (days + sweep cadence)",
          isinstance(rep.get("retention_days"), int)
          and "days" in (rep.get("config") or {})
          and "sweep_every_seconds" in (rep.get("config") or {}), str(rep.get("config")))
    r = requests.get(f"{s}/retention", timeout=T)
    check("tokenless GET /retention = 401 JSON", r.status_code == 401
          and "hint" in r.json(), f"{r.status_code}")
    r = requests.get(f"{s}/health", timeout=T)
    h = r.json()
    check("health advertises retention_sweep + its schedule",
          "retention_sweep" in h.get("features", []) and "retention" in h, str(h.get("retention")))
    before = set(requests.get(f"{s}/files", timeout=T, headers=auth).json().get("files", {}))
    r = requests.post(f"{s}/retention/sweep?dry=1", timeout=T, headers=auth)
    after = set(requests.get(f"{s}/files", timeout=T, headers=auth).json().get("files", {}))
    check("POST /retention/sweep?dry=1 = 200 and deletes nothing",
          r.status_code == 200 and before == after, f"{r.status_code}")
    r = requests.delete(f"{s}/file/{fid}", timeout=T, headers=auth)
    check("operator DELETE of a fresh upload still works after the sweep code landed",
          r.status_code == 200 and fid not in
          requests.get(f"{s}/files", timeout=T, headers=auth).json().get("files", {}),
          f"{r.status_code}")

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

    # ---- v1.5: task ledger, dead-letter triage, two principals
    dl = requests.get(f"{s}/tasks/dead-letter", timeout=T, headers=auth)
    check("GET /tasks/dead-letter 200 + triage keys",
          dl.ok and {"count", "tasks", "states", "ledger_rows", "outstanding_by_agent",
                     "expired_total", "ttl_seconds", "newest_last"} <= set(dl.json()),
          dl.text[:140])
    ag = requests.get(f"{s}/agents", timeout=T, headers=auth)
    agj = ag.json() if ag.ok else {}
    det = agj.get("detail") or {}
    check("GET /agents carries the v1.5 ledger summary",
          ag.ok and {"task_ledger", "last_seen_note", "stranded_note"} <= set(agj)
          and {"outstanding_by_agent", "stranded", "dead_letter", "ttl_seconds", "endpoint"}
          <= set(agj.get("task_ledger") or {}), ag.text[:160])
    if det:
        check("agents detail carries last_seen + queue depth + outstanding tasks",
              all({"last_seen", "inbox_backlog", "outstanding_tasks"} <= set(v)
                  for v in det.values()), str(list(det.values())[:1])[:160])
    check("GET /result/<never issued> 404 points at the ledger, not the eviction window",
          requests.get(f"{s}/result/neverissued0", timeout=T, headers=auth).status_code == 404
          and "/tasks/dead-letter" in requests.get(f"{s}/result/neverissued0",
                                                   timeout=T, headers=auth).json().get("hint", ""),
          "hint should name the dead-letter route")
    badhdr = {"Accept": "application/json", "ngrok-skip-browser-warning": "true"}
    for bad in ("not-a-credential", "agenta.shortepoch." + "0" * 64, "a.b.c"):
        r = requests.get(f"{s}/agents", timeout=T, headers={**badhdr, "X-Agent-Token": bad})
        check(f"bogus credential '{bad[:20]}' = 401", r.status_code == 401, r.text[:110])
    a = manifest.get("auth") or {}
    check("manifest documents both principals and the credential-derived model",
          {"operator", "agent"} <= set(a.get("principals") or {})
          and "credential" in str(a.get("model", "")), str(a)[:160])
    h2 = requests.get(f"{s}/health", timeout=T).json()
    check("health advertises the v1.5 features",
          {"task_ledger", "dead_letter_queue", "agent_last_seen", "agent_credentials",
           "scoped_reads", "operator_principal"} <= set(h2.get("features", [])),
          str(h2.get("features"))[:160])
    check("footguns cover credential-derived identity and the operator seat",
          "credential that authenticated" in fg and "HUB_OPERATOR_HTTP=0" in fg, fg[:160])
    check("socket contract documents the agent_token handshake",
          "agent_token" in json.dumps(manifest.get("socket", {})), "missing agent_token")
    at_keys = (manifest.get("socket", {}).get("hub_to_agent", {}) or {}).get("agent_token", {})
    check("agent_token declares the v1.8.1 client handoff (no undeclared handshake keys)",
          "client" in at_keys and "/client.py" in str(at_keys.get("client", "")), str(at_keys)[:150])

    # ---- logs
    r = requests.get(f"{s}/logs/wrong-token-9x", timeout=T)
    check("logs wrong token = 404", r.status_code == 404)
    r = requests.get(f"{s}/logs/wrong-token-9x/events.json", timeout=T)
    check("events.json wrong token = 404", r.status_code == 404)
    if args.logtoken:
        r = requests.get(f"{s}/logs/{args.logtoken}/events.json?limit=5", timeout=T)
        check("events.json 200 structured",
              r.ok and {"events", "count", "total", "newest_last"} <= set(r.json()), r.text[:120])

    # ---- v1.5.2: /logs token check must not 500 on non-ASCII (compare_digest TypeError)
    h404 = {"ngrok-skip-browser-warning": "true"}
    for enc, label in (("%E4%BD%A0%E5%A5%BD", "utf8 path segment"),
                       ("%C3%A9", "latin1 letter")):
        r = requests.get(f"{s}/logs/{enc}", timeout=T, headers=h404)
        check(f"non-ASCII log token ({label}) = 404, never 500",
              r.status_code == 404, f"{r.status_code} {r.text[:80]}")
        r = requests.get(f"{s}/logs/{enc}/events.json", timeout=T, headers=h404)
        check(f"non-ASCII log token ({label}) on events.json = 404",
              r.status_code == 404, f"{r.status_code}")

    # ---- optional live socket section (--socket): needs python-socketio
    if args.socket:
        try:
            import socketio
        except ImportError:
            print("SKIP  socket section: pip install python-socketio first")
        else:
            suffix = secrets.token_hex(3)
            hdr = {"ngrok-skip-browser-warning": "true"}

            def sconn(agent_id, responder=False):
                cli = socketio.Client()
                box = {"cred": None, "got": None, "tok": None}

                @cli.on("agent_token", namespace="/agents")
                def _tok(d=None):
                    box["tok"] = d
                    box["cred"] = (d or {}).get("token")

                @cli.on("task", namespace="/agents")
                def _task(d=None):
                    box["got"] = d
                    if responder:
                        cli.emit("result", {"msg_id": (d or {}).get("msg_id"),
                                            "text": "selftest socket answer"},
                                 namespace="/agents")
                cli.connect(s, namespaces=["/agents"],
                            auth={"token": args.token, "agent_id": agent_id},
                            wait_timeout=15, headers=hdr)
                return cli, box

            # A) a task caller may poll the ledger row it created (issue: the POST
            #    response advertises result_endpoint, but GET /result 403'd for the asker)
            probe_a, probe_b = f"st-alice-{suffix}", f"st-bob-{suffix}"
            alice, alice_box = sconn(probe_a)
            bob, bob_box = sconn(probe_b, responder=True)
            try:
                time.sleep(0.4)
                ok_cred = bool(alice_box["cred"])
                check("socket agent got a v1.5 credential pushed", ok_cred, "no agent_token")
                tip = str((alice_box["tok"] or {}).get("client") or "")
                check("handshake pushes a ready /client.py fetch, authorized by that credential",
                      "/client.py" in tip and "Bearer" in tip
                      and str(alice_box["cred"]) in tip, tip[:150])
                r = requests.get(f"{s}/client.py", timeout=T + 15,
                                 headers={**hdr, "X-Agent-Token": alice_box["cred"]})
                check("the pushed credential can actually download the reference client",
                      r.ok and "socketio" in r.text and len(r.text) > 2000,
                      f"{r.status_code} {len(r.text)} B")
                r = requests.post(f"{s}/agent/{probe_b}/message", timeout=T + 15,
                                  headers={**hdr, "X-Agent-Token": alice_box["cred"]},
                                  json={"text": "selftest caller-read probe"})
                m = r.json() if r.ok else {}
                mid = m.get("msg_id", "")
                check("agent principal can POST a task",
                      r.ok and m.get("status") == "replied"
                      and (m.get("reply") or {}).get("text") == "selftest socket answer",
                      r.text[:160])
                r = requests.get(f"{s}/result/{mid}", timeout=T,
                                 headers={**hdr, "X-Agent-Token": alice_box["cred"]})
                row = r.json() if r.ok else {}
                check("caller's GET /result/<msg_id> is its own readable row (200, not 403)",
                      r.status_code == 200 and row.get("from") == f"agent:{probe_a}"
                      and len(row.get("results", [])) >= 1, r.text[:160])
                carol, carol_box = sconn(f"st-carol-{suffix}")
                try:
                    time.sleep(0.4)
                    r = requests.get(f"{s}/result/{mid}", timeout=T,
                                     headers={**hdr, "X-Agent-Token": carol_box["cred"]})
                    check("a THIRD agent still gets 403 on that row (scoping not widened)",
                          r.status_code == 403, r.text[:120])
                finally:
                    carol.disconnect()
            finally:
                alice.disconnect(); bob.disconnect()

            # B) v1.4 duplicate-id refusal is atomic: N concurrent non-forced connects
            #    to one fresh id must leave exactly 1 registered socket and 0 standbys.
            dup_id = f"st-dup-{suffix}"
            accepted, refused = [], []
            done = threading.Event()
            gate = threading.Barrier(8)

            def dup_one():
                cli = socketio.Client()
                try:
                    gate.wait(timeout=10)
                    cli.connect(s, namespaces=["/agents"],
                                auth={"token": args.token, "agent_id": dup_id},
                                wait_timeout=15, headers=hdr)
                    accepted.append(cli)
                    time.sleep(4.0)        # hold the winner up while the hub is probed
                except Exception:
                    refused.append(1)
                finally:
                    try:
                        cli.disconnect()
                    except Exception:
                        pass
                    if len(accepted) + len(refused) == 8:
                        done.set()

            ts = [threading.Thread(target=dup_one) for _ in range(8)]
            for t in ts:
                t.start()
            time.sleep(2.5)
            r = requests.get(f"{s}/agent-id/{dup_id}", timeout=T, headers=auth)
            st_mid = r.json() if r.ok else {}
            done.wait(timeout=45)
            r = requests.get(f"{s}/agent-id/{dup_id}", timeout=T, headers=auth)
            st = r.json() if r.ok else {}
            check("duplicate-id storm: exactly one socket registered",
                  len(accepted) == 1 and len(refused) == 7,
                  f"accepted={len(accepted)} refused={len(refused)}")
            check("duplicate-id storm: no silent take-over left a standby (v1.4 contract)",
                  st_mid.get("available") is False and not st_mid.get("standby_sockets"),
                  json.dumps(st_mid)[:160])
            check("duplicate-id storm: refusals are recorded in last_rejection",
                  bool(st.get("last_rejection")), json.dumps(st.get("last_rejection"))[:120])
            time.sleep(0.5)

            # C) v1.5.2: /events/mine scoping is whole-word, and JSON rows carry the
            #    payload the human page already shows (payload_full). An agent whose id is
            #    a PREFIX of another agent's id used to see the longer id's traffic.
            pfx = f"st-pfx-{suffix}"
            pfx2 = pfx + "x"
            one, two = sconn(pfx), sconn(pfx2)
            (one_cli, one_box), (two_cli, two_box) = one, two
            try:
                time.sleep(0.4)
                marker = "PRIV-" + secrets.token_hex(6)
                rr = requests.post(f"{s}/relay", timeout=T, headers=auth,
                                   json={"to": pfx2, "text": f"traffic only for {pfx2}: {marker}"})
                check("relay to prefix-colliding peer succeeded", rr.ok, rr.text[:100])
                time.sleep(0.4)
                r = requests.get(f"{s}/events/mine", timeout=T,
                                 headers={**hdr, "X-Agent-Token": one_box["cred"]})
                ev1 = r.json() if r.ok else {}
                leaked = [e for e in ev1.get("events", [])
                          if marker in e.get("payload", "") + e.get("payload_full", "")]
                check("short-id agent does NOT see the longer-id peer's traffic",
                      r.ok and not leaked, f"leaked {len(leaked)} rows: {str(leaked)[:120]}")
                r = requests.get(f"{s}/events/mine", timeout=T,
                                 headers={**hdr, "X-Agent-Token": two_box["cred"]})
                ev2 = r.json() if r.ok else {}
                own = [e for e in ev2.get("events", [])
                       if marker in e.get("payload", "") + e.get("payload_full", "")]
                check("longer-id agent still sees its OWN traffic (positive control)",
                      r.ok and len(own) >= 1, f"own rows {len(own)}")
                # payload_full: task bodies past 160 chars were invisible to every JSON feed
                tail = "R2CEND-" + secrets.token_hex(4)
                long_text = ("usability probe " + ("lorem ipsum " * 40)).strip() + " " + tail
                rr = requests.post(f"{s}/agent/{pfx2}/message?wait=0", timeout=T,
                                   headers=auth, json={"text": long_text})
                check("long task POST delivered (payload_full probe)", rr.ok, rr.text[:100])
                time.sleep(0.4)
                r = requests.get(f"{s}/events/mine?limit=50", timeout=T,
                                 headers={**hdr, "X-Agent-Token": two_box["cred"]})
                rows = r.json().get("events", []) if r.ok else []
                row = next((e for e in rows if tail in str(e.get("payload_full", ""))), None)
                check("events/mine row carries payload_full beyond the 160-char clip",
                      row is not None and tail in row["payload_full"],
                      "no payload_full contains the task tail" if not row else "")
                if row:
                    check("short payload summary still present (additive, shape kept)",
                          "payload" in row and len(row["payload"]) <= 162, str(row)[:120])
                # ---- v1.5.2 DELETE scoping with two live credentials
                own_bytes = json.dumps({"own": secrets.token_hex(3)}).encode()
                rr = requests.post(f"{s}/file", timeout=T,
                                   headers={**hdr, "X-Agent-Token": one_box["cred"]},
                                   files={"file": ("one-owned.json", own_bytes)})
                fid_own = rr.json().get("file_id", "") if rr.status_code in (200, 201) else ""
                r = requests.delete(f"{s}/file/{fid_own}", timeout=T,
                                    headers={**hdr, "X-Agent-Token": two_box["cred"]})
                check("peer agent gets 403 deleting another agent's upload",
                    bool(fid_own) and r.status_code == 403, r.text[:120])
                r = requests.delete(f"{s}/file/{fid_own}", timeout=T,
                                    headers={**hdr, "X-Agent-Token": one_box["cred"]})
                check("uploader agent deletes its own file (200)",
                      r.status_code == 200 and r.json().get("status") == "deleted",
                      r.text[:120])
            finally:
                one_cli.disconnect(); two_cli.disconnect()
            time.sleep(0.5)

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
        # v1.5: an emitted task is a ledger row even if nobody answers it, and the caller
        # label comes from which credential authenticated
        r = requests.post(f"{s}/agent/{args.agent}/message?wait=0", timeout=T, headers=auth,
                          json={"text": "selftest ledger probe"})
        led = r.json() if r.ok else {}
        mid2 = led.get("msg_id", "")
        check("POST wait=0 answers with the ledger handle",
              r.ok and led.get("task_state") in ("delivered", "acked")
              and led.get("result_endpoint") == f"/result/{mid2}", r.text[:160])
        r = requests.get(f"{s}/result/{mid2}", timeout=T, headers=auth)
        row = r.json() if r.ok else {}
        check("the task is readable as a ledger row (state + deadline), not a 404",
              r.ok and row.get("state") in ("delivered", "acked", "answered")
              and row.get("deadline_at") and row.get("delivered_at")
              and row.get("hub_issued") is True, r.text[:180])
        check("an operator-token call is labeled operator, never agent",
              str(row.get("from", "")).startswith("operator:"), str(row.get("from")))
        r = requests.get(f"{s}/tasks/dead-letter", timeout=T, headers=auth)
        dlj = r.json() if r.ok else {}
        check("the waiting task shows up as outstanding for this agent",
              dlj.get("outstanding_by_agent", {}).get(args.agent, 0) >= 1
              or dlj.get("states", {}).get("answered", 0) >= 1, str(dlj)[:180])

    # ================= round 3: item 1 (cadence, not render), item 2, item 3, pain (e)
    # The rings are the source of truth and /logs renders from them on read, so batching the
    # DISK is allowed and delaying a RENDER is not. Burst enough offline relays that every one
    # logs a row with a unique agent id, then demand the newest in both readers immediately.
    burst = [f"stburst-{secrets.token_hex(3)}-{i}" for i in range(40)]
    hm0 = (requests.get(f"{s}/health", timeout=T).json() or {}).get("log_mirror") or {}
    t0 = time.time()
    for bid in burst:
        requests.post(f"{s}/relay", headers=auth, timeout=T, json={"to": bid, "text": "x"})
    elapsed = time.time() - t0
    newest = burst[-1]
    if args.logtoken:
        page = requests.get(f"{s}/logs/{args.logtoken}", timeout=T)
        check("freshness: newest event is in the /logs page on read (render is not deferred)",
              page.ok and newest in page.text, f"{page.status_code} '{newest}' absent")
        ej = requests.get(f"{s}/logs/{args.logtoken}/events.json?limit=3000", timeout=T)
        rows = (ej.json().get("events") if ej.ok else []) or []
        check("freshness: newest event is in events.json on read",
              ej.ok and any(e.get("agent") == newest for e in rows), f"{len(rows)} rows")
    hm1 = (requests.get(f"{s}/health", timeout=T).json() or {}).get("log_mirror") or {}
    if hm1.get("interval_seconds", 0) > 0:
        bound = int(elapsed / hm1["interval_seconds"]) + 3
        delta = hm1.get("writes", 0) - hm0.get("writes", 0)
        check(f"log writes bounded by flush cadence, not event count ({len(burst)} events -> "
              f"{delta} writes)", delta <= bound, f"delta={delta} bound={bound} "
              f"elapsed={elapsed:.1f}s interval={hm1['interval_seconds']}s")
    else:
        print(f"SKIP  write-bounding: hub runs HUB_LOG_WRITE_INTERVAL=0 (legacy per-event)")

    # /events/mine exact-id index: the caller's own rows must be there, and ?mentions=1 (the
    # pre-index whole-ring scan) must still be a superset - the index may not lose a row.
    ie = requests.get(f"{s}/events/mine?limit=500", headers={**auth, "X-Agent-Id": newest},
                      timeout=T)
    ij = ie.json() if ie.ok else {}
    me = requests.get(f"{s}/events/mine?limit=500&mentions=1",
                      headers={**auth, "X-Agent-Id": newest}, timeout=T)
    mj = me.json() if me.ok else {}
    check("events/mine index carries the caller's own row without a log secret",
          ie.ok and any(e.get("agent") == newest for e in ij.get("events", [])),
          str(ij)[:160])
    check("events/mine ?mentions=1 stays a superset of the indexed answer",
          me.ok and mj.get("total_matching", -1) >= ij.get("total_matching", 0),
          f"scan={mj.get('total_matching')} index={ij.get('total_matching')}")
    # An id the index dropped reads empty, which looks exactly like "you did nothing". The answer
    # has to say which case it is, or a caller cannot tell a slow path from a lost bucket.
    fresh = f"stidx-{secrets.token_hex(3)}"
    requests.post(f"{s}/relay", headers=auth, timeout=T, json={"to": fresh, "text": "index me"})
    fj = requests.get(f"{s}/events/mine", headers={**auth, "X-Agent-Id": fresh},
                      timeout=T).json()
    ghost = requests.get(f"{s}/events/mine", headers={**auth, "X-Agent-Id": "stidx-never-seen"},
                         timeout=T).json()
    check("events/mine says whether this caller is index-backed (indexed=true for a live id)",
          fj.get("indexed") is True and fj.get("total_matching", 0) >= 1, str(fj)[:150])
    check("events/mine reports indexed=false rather than a silent empty feed",
          ghost.get("indexed") is False and ghost.get("count") == 0, str(ghost)[:150])

    # /files payload trimming, additive: no params keeps the legacy shape exactly.
    payload = json.dumps({"selftest": secrets.token_hex(3)}).encode()
    up = requests.post(f"{s}/file", headers=auth, timeout=T,
                       files={"file": ("st-ids.json", payload)})
    fid = (up.json() or {}).get("file_id", "") if up.status_code in (200, 201) else ""
    one = requests.get(f"{s}/files?ids={fid}", headers=auth, timeout=T)
    oj = one.json() if one.ok else {}
    check("GET /files?ids=<one> answers with exactly that row plus total_matching",
          bool(fid) and one.ok and oj.get("count") == 1 and fid in (oj.get("files") or {})
          and "total_matching" in oj, str(oj)[:160])
    lim = requests.get(f"{s}/files?limit=1", headers=auth, timeout=T)
    lj = lim.json() if lim.ok else {}
    check("GET /files?limit=1 trims and says it trimmed",
          lim.ok and lj.get("count") == 1 and lj.get("trimmed") is True, str(lj)[:160])
    whole = requests.get(f"{s}/files", headers=auth, timeout=T)
    wj = whole.json() if whole.ok else {}
    check("GET /files with no params keeps the legacy shape (whole table, no trim keys)",
          whole.ok and fid in (wj.get("files") or {}) and "trimmed" not in wj, str(wj)[:120])

    # pain (e): a documented key list that has drifted from the response is worse than none.
    man = requests.get(f"{s}/api", headers=auth, timeout=T).json()

    def declared(method, path):
        for e in man.get("endpoints", []):
            if e.get("method") == method and e.get("path") == path:
                return {k.split(" ")[0] for k in (e.get("returns") or {}).get("keys") or []}
        return set()

    drift = requests.post(f"{s}/file", headers=auth, timeout=T,
                          files={"file": ("st-drift.json", payload)})
    got = set((drift.json() or {}).keys()) if drift.status_code in (200, 201) else set()
    undeclared = sorted(got - declared("POST", "/file") - {"docs", "api", "hint"})
    check("POST /file answers only with keys /api declares (pain: doc drift)",
          bool(got) and not undeclared, f"undeclared: {undeclared}")
    hk = set(requests.get(f"{s}/health", timeout=T).json())
    check("/health answers only with keys /api declares",
          hk and not sorted(hk - declared("GET", "/health")),
          f"undeclared: {sorted(hk - declared('GET', '/health'))}")

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("failed checks: " + ", ".join(FAIL), file=sys.stderr)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
