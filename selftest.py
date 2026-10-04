#!/usr/bin/env python3
"""selftest.py - verify an Agent Hub satisfies the agent-facing contract (v1.0 -> v1.4).

HTTP-only: runs against any hub URL (test instance or live tunnel). Shape-locks
are supersets, never exact equality, so future additive fields pass.

    python3 selftest.py --server https://<hub> --token "$AGENT_AUTH_TOKEN" [--logtoken <LOG_SECRET_TOKEN>] [--agent <connected_id>]

Exit 0 = all checks passed, 1 = at least one failure. A --server that isn't reachable or
isn't a hub stops the run at once with a diagnosis instead of ~100 FAIL lines and a
traceback; set SELFTEST_DEBUG=1 to see that traceback anyway."""

import argparse
import hashlib
import json
import os
import re
import secrets
import sys
import threading
import time
from urllib.parse import urlparse, urljoin

import requests

REQUIRED_FEATURES = ["llms_txt", "api_manifest", "json_errors", "method_405",
                     "inbox_peek", "agents_detail", "upload_sha256", "autojson_name",
                     "file_index", "events_json", "result_lookup", "events_mine",
                     "client_source", "dedupe_uploads", "superseded_notice",
                     "single_result_log_rows", "standby_takeover_chain",
                     "result_inbox_status", "unique_agent_ids",
                     "unread_nudge", "relay_queue"]

PASS = []
FAIL = []


def one_line(x: object, limit: int = 160) -> str:
    """Collapse whitespace and clip: a detail printed after `<-` must stay on one line."""
    flat = " ".join(str(x).split())
    return flat if len(flat) <= limit else flat[:limit].rstrip() + " ..."


def follow_link(base_url: str, href: str) -> str:
    """Resolve an <a href> the way a browser does, which urljoin does not: a reference starting
    with '?' REPLACES the query (so '?' means "same page, no filter"), while urljoin cannot tell an
    empty query from an absent one and keeps the base's. Resolving with urljoin here re-reads the
    filtered page and reports a working clear link as broken."""
    if href.startswith("?"):
        return base_url.split("?", 1)[0] + href
    return urljoin(base_url, href)


def port_hint(base: str) -> str:
    """The likeliest fix when something owns :80 but isn't a hub: HUB_PORT is 5000."""
    netloc = urlparse(base).netloc.split("@")[-1]
    if ":" in netloc or netloc not in ("localhost", "127.0.0.1", "0.0.0.0"):
        return ""
    return (f"a hub on this machine listens on HUB_PORT, 5000 by default - try "
            f"--server http://{netloc}:5000")


def jval(r: requests.Response) -> dict:
    """JSON body as a dict, {} if it isn't JSON -- a hub answering HTML fails its check
    instead of raising JSONDecodeError partway through the run."""
    try:
        body = r.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def neterr(exc: BaseException, url: str) -> str:
    """One human line for a requests failure; the raw exception is nested urllib3 reprs."""
    txt = one_line(exc, 400)
    for key, say in (
            ("Connection refused", f"nothing is listening at {url}"),
            ("Name or service not known", f"cannot resolve the host in {url}"),
            ("Read timed out", f"{url} took the connection but never answered"),
            ("timed out", f"{url} did not answer in time"),
            ("No scheme supplied", f"{url} has no http:// or https:// scheme"),
            ("No connection adapters", f"{url} has no http:// or https:// scheme"),
            ("Max retries exceeded", f"{url} refused the request")):
        if key in txt:
            return say
    return f"{type(exc).__name__} talking to {url}: {one_line(txt, 140)}"


def redact(secret: str) -> str:
    return f"{secret[:3]}*** ({len(secret)} chars)" if secret else "(empty)"


def bail(title: str, *lines: str) -> int:
    """Stop the run with a diagnosis on stderr and the exit code a failure uses."""
    notes = list(lines)
    while notes and not notes[-1]:
        notes.pop()
    print(f"\nERROR  {title}", file=sys.stderr)
    for ln in notes:
        print(f"       {ln}" if ln else "", file=sys.stderr)
    return 1


def check(name: str, cond: bool, detail: str = "") -> None:
    (PASS if cond else FAIL).append(name)
    mark = "PASS" if cond else "FAIL"
    note = one_line(detail)
    print(f"{mark:4}  {name}" + (f"  <- {note}" if not cond and note else ""))


def main() -> int:
    ap = argparse.ArgumentParser(description="Agent Hub contract self-test")
    ap.add_argument("--server", required=True, help="hub base URL")
    ap.add_argument("--token", required=True, help="AGENT_AUTH_TOKEN")
    ap.add_argument("--logtoken", default="", help="LOG_SECRET_TOKEN for events.json checks")
    ap.add_argument("--agent", default="", help="an agent_id that is connected: live task round-trip")
    ap.add_argument("--socket", action="store_true",
                    help="run the live socket section (needs python-socketio): verifies the "
                         "v1.4 duplicate-id refusal under concurrency, that a task caller "
                         "can poll its own GET /result row with its agent credential, the "
                         "reconnect cycle (graceful drop -> blind window -> same-token rejoin on "
                         "a freshly minted credential), and the v1.9 unread nudge (a missed file "
                         "announced at connect, a relay to an offline agent queued rather than "
                         "dropped, nothing ever named twice)")
    ap.add_argument("--timeout", type=float, default=20)
    args = ap.parse_args()

    s = args.server.rstrip("/")
    auth = {"Authorization": f"Bearer {args.token}", "X-Agent-Id": "selftest",
            "ngrok-skip-browser-warning": "true", "Accept": "application/json"}
    T = args.timeout

    # ---- pre-flight: /health is the hub's own unauthenticated handshake. Without this gate a
    #      wrong --server (typo, dead port, nginx site root) cascades through every check.
    url = f"{s}/health"
    local_hint = port_hint(s)
    fix = ([local_hint] if local_hint else
           ["--server is the hub's base URL: no path, no trailing slash, e.g.",
            "    --server http://127.0.0.1:5000        a hub on this machine",
            "    --server https://<id>.ngrok-free.app  a tunnel to it"])
    try:
        probe = requests.get(url, timeout=T,
                             headers={"ngrok-skip-browser-warning": "true",
                                      "Accept": "application/json"})
    except requests.RequestException as e:
        return bail(f"cannot reach {url}", neterr(e, url), "",
                    "Start a hub: AGENT_AUTH_TOKEN=<6-50 chars> LOG_SECRET_TOKEN=<secret> "
                    "python3 app.py", *fix)

    body, ctype = jval(probe), probe.headers.get("content-type", "")
    if probe.status_code in (401, 403):
        return bail(f"{url} answered {probe.status_code}, but a hub never asks for a "
                    "token on /health",
                    f"{ctype or 'no content-type'}: {one_line(probe.text, 140)}",
                    "", "A proxy in front of this URL is enforcing auth, or it is another "
                    "service. Point --server at the hub itself.", *fix)
    if not probe.ok or "json" not in ctype or not body:
        says = " - ".join(one_line(x, 90) for x in (body.get("error"), body.get("hint")) if x)
        prefix = urlparse(s).path.strip("/")
        return bail(f"{s} is not an Agent Hub",
                    f"GET /health -> {probe.status_code} {ctype or 'no content-type'}"
                    + (f" from {probe.headers['server']}" if probe.headers.get("server") else ""),
                    (f"it says: {says}" if says
                     else f"body: {one_line(probe.text, 140) or '(empty)'}"),
                    "",
                    "Expected 200 application/json carrying status / agents_connected / features.",
                    (f"Your --server carries a path (/{prefix}); /health lives at the hub's own "
                     "root - drop it unless nginx mounts the hub under that prefix."
                     if prefix else
                     "Whatever answers here is not the hub: behind nginx that is usually the "
                     "site root, not the location proxying to it."),
                    *([local_hint] if prefix else fix))
    if not {"status", "agents_connected", "features"} <= set(body):
        return bail(f"{s} answers /health with JSON but is not an Agent Hub",
                    f"keys present: {sorted(body)[:8]}",
                    "", "A different service on that port: expected at least status, "
                    "agents_connected and features.", *fix)

    r = requests.get(f"{s}/agents", timeout=T, headers=auth)
    if r.status_code == 401:
        print(f"WARN   --token {redact(args.token)} was refused by GET /agents "
              f"({one_line(jval(r).get('error') or r.text, 90)}) - every authenticated "
              "check below will fail")

    # ---- discovery, unauthenticated
    r = requests.get(f"{s}/health", timeout=T)
    check("health 200 + core keys",
          r.ok and {"status", "agents_connected", "version", "features",
                    "uptime_seconds", "docs", "api"} <= set(jval(r)), r.text[:120])
    if r.ok:
        h = jval(r)
        check("health features advertise the full v1.4 contract",
              set(REQUIRED_FEATURES) <= set(h.get("features", [])),
              str(h.get("features")))
        check("health shape-lock (legacy keys)",
              {"status", "agents_connected", "uptime"} <= set(h))

    r = requests.get(f"{s}/api", timeout=T)
    manifest = jval(r) if r.ok and "json" in r.headers.get("content-type", "") else {}
    check("api manifest 200 + sections",
          r.ok and {"version", "base_url", "endpoints", "socket",
                    "footguns", "features", "auth"} <= set(manifest), r.text[:120])

    r = requests.get(f"{s}/llms.txt", timeout=T)
    llms = r.text if r.ok else ""
    check("llms.txt 200 markdown",
          r.ok and r.headers.get("content-type", "").startswith("text/markdown"),
          f"{r.status_code} {r.headers.get('content-type')}")
    # v1.9 moved this ceiling 20,000 -> 23,000: the unread notice is a whole socket event and the
    # guard exists to catch runaway doc growth, not to keep the guide at an arbitrary size.
    # v1.10 moves it 23,000 -> 24,000 for the same reason: the log row grammar, its five query
    # params, the events.json row keys and the `agent` vs `frm`/`to` footgun are +947 chars over
    # the v1.9.1 render (measured: 22,602 -> 23,549 chars, 22,626 -> 23,573 B).
    # v1.11 moves it 24,000 -> 25,000: /logs carries a download route, the store stops being
    # scoped per agent and the ceiling is 100 MB, which is +928 chars over the v1.10 render
    # (measured: 23,549 -> 24,477 characters).
    # v1.11.1 moves it 25,000 -> 27,000: /llms.txt now says how to GET the client you are about
    # to run, pre-flights the agent_id and names the credential connect pushes back, which is
    # +1,042 chars over this build's own baseline (measured: 24,699 -> 25,741 chars,
    # 24,723 -> 25,765 B). That baseline is 222 chars ABOVE the 24,477 the v1.11 note recorded:
    # HEAD~1 still renders 23,549 - exactly its own note - so the drift happened inside v1.11 and
    # this suite never caught it, because a ceiling only fails when it is crossed. Headroom here is
    # deliberately ~1,250 rather than the 12 B v1.9 ran out of.
    # `len(r.text)` counts CHARACTERS, not bytes - labelling it "bytes" below is how a README
    # claim once came out 24 B off its own arithmetic.
    check("llms.txt sane size", 0 < len(llms) < 27000, f"{len(llms)} chars")
    drift = [e["path"] for e in manifest.get("endpoints", []) if e["path"] not in llms]
    check("docs drift: every /api path appears in /llms.txt", not drift, str(drift))
    # The gap this closes: the guide used to say `python3 mock_agent.py ...` without ever saying the
    # hub serves that file. An agent with a URL and a token and no checkout was stuck here.
    check("llms.txt says how to get the client",
          "client.py" in llms and "--check-id" in llms, llms[llms.find("Quickstart"):][:90])

    r = requests.get(f"{s}/", timeout=T)
    page = r.text if r.ok else ""
    check("onboarding 200 html", r.ok and "text/html" in r.headers.get("content-type", ""))
    check("voting removed from onboarding", "Vote for your own roles" not in page)
    # The page an agent lands on has to be enough to get connected, not just to read about it:
    # fetch the client, pre-flight the id, join, then answer with the pushed credential.
    check("onboarding carries the agent join recipe",
          all(m in page for m in ("Join as an agent", "/client.py", "--check-id",
                                  "agent_token", "outbox.jsonl", "--peek")),
          str([m for m in ("Join as an agent", "/client.py", "--check-id", "agent_token",
                           "outbox.jsonl", "--peek") if m not in page]))
    check("join recipe precedes the operator quick start",
          0 < page.find("Join as an agent") < page.find("<h2>Quick start</h2>"),
          f"join={page.find('Join as an agent')} quickstart={page.find('<h2>Quick start</h2>')}")

    # ---- auth negotiation
    r = requests.get(f"{s}/agents", timeout=T)  # no token
    ok_json = (r.status_code == 401 and "json" in r.headers.get("content-type", "")
               and {"error", "hint", "docs", "api", "example"} <= set(jval(r)))
    check("401 tokenless = JSON with hint", ok_json, f"{r.status_code} {r.text[:80]}")
    r = requests.get(f"{s}/agents", timeout=T, headers={"Accept": "text/html"})
    check("401 browser = HTML onboarding",
          r.status_code == 401 and "text/html" in r.headers.get("content-type", ""))
    r = requests.get(f"{s}/agents", timeout=T, headers=auth)
    check("agents 200 + legacy + new keys",
          r.ok and {"agents", "count", "agent_ids", "detail", "standby", "standby_note"}
          <= set(jval(r)), str(list(jval(r))))
    ag = jval(r) if r.ok else {}
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
          r.status_code == 400 and "pattern" in jval(r), r.text[:80])
    r = requests.post(f"{s}/agent/nobody-9x/message", timeout=T, headers=auth, json={"text": "x"})
    check("offline agent = 404 + start-one hint",
          r.status_code == 404 and "mock_agent.py" in jval(r).get("hint", ""), r.text[:80])
    r = requests.post(f"{s}/relay", timeout=T, headers=auth, json={"to": "bad id"})
    check("relay bad body = 400", r.status_code == 400 and "error" in jval(r))

    # ---- file store
    payload = json.dumps({"selftest": secrets.token_hex(4)}).encode()
    sha = hashlib.sha256(payload).hexdigest()
    name = "selftest.json"
    r = requests.post(f"{s}/file", timeout=T, headers=auth, files={"file": (name, payload)})
    up = jval(r) if r.ok else {}
    check("upload multipart 201 + sha256",
          r.status_code == 201 and {"status", "file_id", "name", "size", "sha256",
                                    "delivered", "download_url"} <= set(up)
          and up.get("sha256") == sha, r.text[:120])
    fid = up.get("file_id", "")

    r = requests.post(f"{s}/file", timeout=T, headers=auth, files={"file": (name, payload)})
    check("same bytes = duplicate_of advisory",
          r.status_code == 201 and jval(r).get("duplicate_of") == fid, r.text[:120])

    r = requests.post(f"{s}/file", timeout=T,
                      headers={**auth, "Content-Type": "application/json"},
                      data=payload)
    check("raw JSON body auto-names body.json",
          r.status_code == 201 and jval(r).get("name") == "body.json", r.text[:120])

    r = requests.post(f"{s}/file", timeout=T, headers=auth,
                      files={"file": ("\u5831\u544a.json", payload)})
    bad = jval(r) if r.status_code == 415 else {}
    check("non-ASCII name = 415 echo sanitized name",
          r.status_code == 415 and "name_after_sanitize" in bad, r.text[:120])

    r = requests.post(f"{s}/file", timeout=T,
                      headers={**auth, "X-Target-Agent": "nosuch-9x"},
                      files={"file": (name, payload)})
    tgt = jval(r) if r.ok else {}
    check("bad X-Target-Agent = 201 + target_error",
          r.status_code == 201 and tgt.get("delivered") is False and "target_error" in tgt,
          r.text[:120])

    r = requests.get(f"{s}/file/{fid}", timeout=T, headers=auth)
    check("download bytes match sha", r.ok and hashlib.sha256(r.content).hexdigest() == sha)
    r = requests.get(f"{s}/file/deadbeef00000000", timeout=T, headers=auth)
    check("unknown file_id = 404 + hint",
          r.status_code == 404 and "hint" in jval(r), r.text[:80])
    r = requests.get(f"{s}/files", timeout=T, headers=auth)
    fl = jval(r) if r.ok else {}
    check("files list: count + created_iso",
          r.ok and {"files", "count"} <= set(fl)
          and "created_iso" in fl.get("files", {}).get(fid, {}), r.text[:120])

    # ---- v1.11: the ceiling is behavior, not a doc string. A 26 MB upload used to cost a 413;
    #      the body is zeros so the hub writes 26 MB of near-nothing to its own store, and this
    #      test hands it straight back with DELETE.
    big = bytes(26 * 1024 * 1024)
    r = requests.post(f"{s}/file", timeout=T + 120, headers=auth,
                      files={"file": ("st-big-26mb.json", big)})
    big_j = jval(r) if r.status_code in (200, 201) else {}
    check("v1.11 26 MB upload stores (the old 25 MB ceiling is gone)",
          r.status_code == 201 and big_j.get("size") == len(big),
          f"{r.status_code} {one_line(r.text, 100)}")
    if big_j.get("file_id"):
        requests.delete(f"{s}/file/{big_j['file_id']}", timeout=T, headers=auth)

    # ---- v1.11: the log page hands a file to the human reading it. An <a href> cannot carry an
    #      Authorization header, so the link uses the same secret path segment that opens the
    #      page, and a row whose bytes have left the store gets no link at all.
    if args.logtoken:
        href = f"/logs/{args.logtoken}/file/{fid}"
        page = requests.get(f"{s}/logs/{args.logtoken}", timeout=T + 15)
        check("v1.11 /logs links a file row to its bytes while the store still holds them",
              page.ok and href in page.text,
              f"{page.status_code}, link "
              + ("there" if href in page.text else "ABSENT"))
        dl = requests.get(f"{s}{href}", timeout=T + 15)
        check("v1.11 that link streams the bytes as an attachment with no token header",
              dl.ok and dl.content == payload and name in dl.headers.get("content-disposition", ""),
              f"{dl.status_code}, {len(dl.content)} B")
        wrong = requests.get(f"{s}/logs/{args.logtoken}x/file/{fid}", timeout=T)
        check("v1.11 a wrong log token on a file link 404s like the page itself does",
              wrong.status_code == 404, f"{wrong.status_code}")
        gone = requests.get(f"{s}/logs/{args.logtoken}/file/deadbeef00000000", timeout=T)
        check("v1.11 a file link for an id the store does not have 404s with a hint",
              gone.status_code == 404 and "hint" in jval(gone), f"{gone.status_code}")

    # ---- v1.5.2: DELETE /file/<id> exists, is scoped, and actually reclaims
    r = requests.delete(f"{s}/file/deadbeef00000000", timeout=T, headers=auth)
    check("DELETE unknown file_id = 404 + hint",
          r.status_code == 404 and "hint" in jval(r), r.text[:100])
    r = requests.delete(f"{s}/file/deadbeef00000000", timeout=T,
                        headers={"Accept": "application/json",
                                 "ngrok-skip-browser-warning": "true"})
    check("DELETE tokenless = 401", r.status_code == 401, f"{r.status_code}")
    throw = json.dumps({"throwaway": secrets.token_hex(4)}).encode()
    r = requests.post(f"{s}/file", timeout=T, headers=auth,
                      files={"file": ("del-me.json", throw)})
    fid2 = jval(r).get("file_id", "") if r.status_code == 201 else ""
    r = requests.delete(f"{s}/file/{fid2}", timeout=T, headers=auth)
    check("operator DELETE of own upload = 200 deleted + freed_bytes",
          bool(fid2) and r.status_code == 200 and jval(r).get("status") == "deleted"
          and jval(r).get("freed_bytes") == len(throw), r.text[:140])
    r = requests.get(f"{s}/file/{fid2}", timeout=T, headers=auth)
    check("deleted file_id 404s on download", r.status_code == 404, f"{r.status_code}")
    r = requests.get(f"{s}/files", timeout=T, headers=auth)
    check("deleted id gone from GET /files", bool(fid2) and r.ok
          and fid2 not in jval(r).get("files", {}))

    # ---- v1.7: retention / housekeeping (GET /retention, POST /retention/sweep)
    r = requests.get(f"{s}/retention", timeout=T, headers=auth)
    rep = jval(r) if r.status_code == 200 else {}
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
          and "hint" in jval(r), f"{r.status_code}")
    r = requests.get(f"{s}/health", timeout=T)
    h = jval(r)
    check("health advertises retention_sweep + its schedule",
          "retention_sweep" in h.get("features", []) and "retention" in h, str(h.get("retention")))
    before = set(jval(requests.get(f"{s}/files", timeout=T, headers=auth)).get("files", {}))
    r = requests.post(f"{s}/retention/sweep?dry=1", timeout=T, headers=auth)
    after = set(jval(requests.get(f"{s}/files", timeout=T, headers=auth)).get("files", {}))
    check("POST /retention/sweep?dry=1 = 200 and deletes nothing",
          r.status_code == 200 and before == after, f"{r.status_code}")
    r = requests.delete(f"{s}/file/{fid}", timeout=T, headers=auth)
    check("operator DELETE of a fresh upload still works after the sweep code landed",
          r.status_code == 200 and fid not in
          jval(requests.get(f"{s}/files", timeout=T, headers=auth)).get("files", {}),
          f"{r.status_code}")

    # ---- v1.2: upload dedupe (same bytes, ?dedupe=1 -> reuse newest matching id)
    r = requests.post(f"{s}/file?dedupe=1", timeout=T, headers=auth,
                      files={"file": (name, payload)})
    dd = jval(r) if r.status_code in (200, 201) else {}
    check("dedupe=1 = 200 existing id, nothing written",
          r.status_code == 200 and dd.get("status") == "existing" and dd.get("deduped") is True
          and dd.get("bytes_stored") is False and dd.get("file_id") in fl.get("files", {}),
          r.text[:160])
    r = requests.post(f"{s}/file", timeout=T, headers=auth, files={"file": (name, payload)})
    check("dedupe is opt-in: no flag still mints a new id",
          r.status_code == 201 and jval(r).get("status") == "stored", r.text[:120])

    # ---- v1.2: result correlation
    r = requests.get(f"{s}/result/not-a-real-msg-id", timeout=T, headers=auth)
    check("GET /result/<unknown> = 404 + hint",
          r.status_code == 404 and "hint" in jval(r), r.text[:100])

    # ---- v1.2: per-agent event feed (no log secret needed)
    r = requests.get(f"{s}/events/mine", timeout=T,
                     headers={k: v for k, v in auth.items() if k != "X-Agent-Id"})
    check("events/mine without X-Agent-Id = 400",
          r.status_code == 400 and "X-Agent-Id" in r.text, r.text[:100])
    r = requests.get(f"{s}/events/mine?limit=20", timeout=T, headers=auth)
    ev = jval(r) if r.ok else {}
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
    idst = jval(r) if r.ok else {}
    check("GET /agent-id/<free> = available + full keys",
          r.ok and idst.get("available") is True and idst.get("taken_by_sid") is None
          and {"agent_id", "connected_at", "standby_sockets", "last_rejection", "note"}
          <= set(idst), r.text[:140])
    r = requests.get(f"{s}/agent-id/bad%20id", timeout=T, headers=auth)
    check("GET /agent-id/<malformed> = 400 + pattern",
          r.status_code == 400 and "pattern" in jval(r), r.text[:100])
    r = requests.get(f"{s}/agent-id/selftest-NOT-CONNECTED", timeout=T, headers=auth)
    check("unknown id = 200 available:true (never a 404 dead end)",
          r.status_code == 200 and jval(r).get("available") is True, r.text[:100])
    check("manifest documents the v1.4 endpoint + uniqueness footgun",
          "/agent-id/{agent_id}" in paths and "unique since v1.4" in fg
          and "force_takeover" in fg, str(sorted(paths))[:120])

    # ---- inbox semantics (idle agent, peek must be non-destructive)
    r = requests.get(f"{s}/agent/selftest-idle/inbox?peek=true", timeout=T, headers=auth)
    ib = jval(r) if r.ok else {}
    check("inbox peek 200 + new keys",
          r.ok and {"messages", "count", "drained", "queue_max", "agent_online",
                    "peek"} <= set(ib) and ib.get("peek") is True and ib.get("drained") is False,
          r.text[:120])

    # ---- v1.5: task ledger, dead-letter triage, two principals
    dl = requests.get(f"{s}/tasks/dead-letter", timeout=T, headers=auth)
    check("GET /tasks/dead-letter 200 + triage keys",
          dl.ok and {"count", "tasks", "states", "ledger_rows", "outstanding_by_agent",
                     "expired_total", "ttl_seconds", "newest_last"} <= set(jval(dl)),
          dl.text[:140])
    ag = requests.get(f"{s}/agents", timeout=T, headers=auth)
    agj = jval(ag) if ag.ok else {}
    det = agj.get("detail") or {}
    check("GET /agents carries the v1.5 ledger summary",
          ag.ok and {"task_ledger", "last_seen_note", "stranded_note"} <= set(agj)
          and {"outstanding_by_agent", "stranded", "dead_letter", "ttl_seconds", "endpoint"}
          <= set(agj.get("task_ledger") or {}), ag.text[:160])
    if det:
        check("agents detail carries last_seen + queue depth + outstanding tasks",
              all({"last_seen", "inbox_backlog", "outstanding_tasks"} <= set(v)
                  for v in det.values()), str(list(det.values())[:1])[:160])
    r = requests.get(f"{s}/result/neverissued0", timeout=T, headers=auth)
    check("GET /result/<never issued> 404 points at the ledger, not the eviction window",
          r.status_code == 404 and "/tasks/dead-letter" in jval(r).get("hint", ""),
          "hint should name the dead-letter route")
    badhdr = {"Accept": "application/json", "ngrok-skip-browser-warning": "true"}
    for bad in ("not-a-credential", "agenta.shortepoch." + "0" * 64, "a.b.c"):
        r = requests.get(f"{s}/agents", timeout=T, headers={**badhdr, "X-Agent-Token": bad})
        check(f"bogus credential '{bad[:20]}' = 401", r.status_code == 401, r.text[:110])
    a = manifest.get("auth") or {}
    check("manifest documents both principals and the credential-derived model",
          {"operator", "agent"} <= set(a.get("principals") or {})
          and "credential" in str(a.get("model", "")), str(a)[:160])
    h2 = jval(requests.get(f"{s}/health", timeout=T))
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
              r.ok and {"events", "count", "total", "newest_last"} <= set(jval(r)), r.text[:120])

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
                box = {"cred": None, "got": None, "tok": None, "unread": [], "peers": []}

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

                # v1.9: the recovery notice and the relays it releases. A socket with no handler
                # for an event silently drops it, which would make this section pass on nothing.
                @cli.on("unread", namespace="/agents")
                def _unread(d=None):
                    box["unread"].append(d)

                @cli.on("peer_msg", namespace="/agents")
                def _peer(d=None):
                    box["peers"].append(d)

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
                m = jval(r) if r.ok else {}
                mid = m.get("msg_id", "")
                check("agent principal can POST a task",
                      r.ok and m.get("status") == "replied"
                      and (m.get("reply") or {}).get("text") == "selftest socket answer",
                      r.text[:160])
                r = requests.get(f"{s}/result/{mid}", timeout=T,
                                 headers={**hdr, "X-Agent-Token": alice_box["cred"]})
                row = jval(r) if r.ok else {}
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
            st_mid = jval(r) if r.ok else {}
            done.wait(timeout=45)
            r = requests.get(f"{s}/agent-id/{dup_id}", timeout=T, headers=auth)
            st = jval(r) if r.ok else {}
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
                ev1 = jval(r) if r.ok else {}
                leaked = [e for e in ev1.get("events", [])
                          if marker in e.get("payload", "") + e.get("payload_full", "")]
                check("short-id agent does NOT see the longer-id peer's traffic",
                      r.ok and not leaked, f"leaked {len(leaked)} rows: {str(leaked)[:120]}")
                r = requests.get(f"{s}/events/mine", timeout=T,
                                 headers={**hdr, "X-Agent-Token": two_box["cred"]})
                ev2 = jval(r) if r.ok else {}
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
                rows = jval(r).get("events", []) if r.ok else []
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
                fid_own = jval(rr).get("file_id", "") if rr.status_code in (200, 201) else ""
                r = requests.delete(f"{s}/file/{fid_own}", timeout=T,
                                    headers={**hdr, "X-Agent-Token": two_box["cred"]})
                check("peer agent gets 403 deleting another agent's upload",
                    bool(fid_own) and r.status_code == 403, r.text[:120])
                # ---- v1.11: the store is shared to READ. Two live credentials, so this is the
                #      whole contract in one shot: peer pulls the bytes, peer lists the object,
                #      peer still cannot destroy it (the 403 above is the point of the split).
                r = requests.get(f"{s}/file/{fid_own}", timeout=T,
                                 headers={**hdr, "X-Agent-Token": two_box["cred"]})
                check("v1.11 a peer agent DOWNLOADS another agent's upload (shared store)",
                      bool(fid_own) and r.status_code == 200 and r.content == own_bytes,
                      f"{r.status_code} {len(r.content)} B")
                r = requests.get(f"{s}/files", timeout=T,
                                 headers={**hdr, "X-Agent-Token": two_box["cred"]})
                fj = jval(r) if r.ok else {}
                check("v1.11 a peer agent LISTs another agent's upload, and says the store is "
                      "shared rather than claiming a scope",
                      r.ok and fid_own in (fj.get("files") or {}) and "scoped_to" not in fj
                      and "scope_note" in fj, one_line(str(fj)[:150]))
                r = requests.delete(f"{s}/file/{fid_own}", timeout=T,
                                    headers={**hdr, "X-Agent-Token": one_box["cred"]})
                check("uploader agent deletes its own file (200)",
                      r.status_code == 200 and jval(r).get("status") == "deleted",
                      r.text[:120])
            finally:
                one_cli.disconnect(); two_cli.disconnect()
            time.sleep(0.5)

            # D) the reconnect cycle. A NAT agent loses its socket constantly (tunnel blip), and
            #    an operator may cycle it on a timer, so the contract has two halves worth pinning
            #    down together: the *connect* token is static and reusable, while the minted HTTP
            #    credential is epoch-rotated per socket on purpose - revocation is immediate and
            #    free. The offline gap is pinned too: a hub with no socket does not queue the
            #    task, it answers 404, which is the one thing a cycling client must expect.
            rec_id = f"st-recon-{suffix}"
            first_cli, first_box = sconn(rec_id)
            time.sleep(0.4)
            cred1 = first_box["cred"]
            r = requests.get(f"{s}/agents", timeout=T, headers={**hdr, "X-Agent-Token": cred1})
            check("cycle: the live socket's credential authenticates over HTTP",
                  bool(cred1) and r.ok, f"{r.status_code} {r.text[:110]}")
            first_cli.disconnect()
            time.sleep(0.8)
            r = requests.get(f"{s}/agents", timeout=T, headers={**hdr, "X-Agent-Token": cred1})
            check("cycle: that credential dies with the socket (401 = revocation for free)",
                  r.status_code == 401, f"{r.status_code} {r.text[:110]}")
            st = jval(requests.get(f"{s}/agent-id/{rec_id}", timeout=T, headers=auth))
            check("cycle: a graceful disconnect frees the id (no stale socket, 0 standbys)",
                  st.get("available") is True and not st.get("standby_sockets"),
                  json.dumps(st)[:150])
            r = requests.post(f"{s}/agent/{rec_id}/message", timeout=T, headers=auth,
                              json={"text": "selftest during the gap"})
            check("cycle: the gap is a blind window - offline 404s, nothing is queued",
                  r.status_code == 404 and "offline" in jval(r).get("error", ""),
                  f"{r.status_code} {r.text[:120]}")
            time.sleep(1.0)
            refusal = ""
            try:
                second_cli, second_box = sconn(rec_id, responder=True)
            except Exception as exc:  # noqa: BLE001 - a refused reconnect is a finding, not a crash
                second_cli, second_box, refusal = None, {"cred": None}, one_line(str(exc), 140)
            time.sleep(0.4)
            cred2 = (second_box or {}).get("cred")
            check("cycle: same agent_id + same AGENT_AUTH_TOKEN reconnects unforced",
                  second_cli is not None and second_cli.connected, refusal or "refused")
            check("cycle: the new socket is handed a FRESH credential, never the old one",
                  bool(cred2) and cred2 != cred1, f"same value reused: {str(cred2)[:60]}")
            if cred2:
                r = requests.get(f"{s}/agents", timeout=T,
                                 headers={**hdr, "X-Agent-Token": cred2})
                check("cycle: the fresh credential restores HTTP capability",
                      r.ok, f"{r.status_code} {r.text[:110]}")
                r = requests.get(f"{s}/agents", timeout=T,
                                 headers={**hdr, "X-Agent-Token": cred1})
                check("cycle: the retired credential stays dead even with a socket live",
                      r.status_code == 401, f"{r.status_code} {r.text[:110]}")
                ag2 = jval(requests.get(f"{s}/agents", timeout=T, headers=auth))
                check("cycle: exactly one socket registered for the id after reconnect",
                      ag2.get("agent_ids", []).count(rec_id) == 1,
                      f"count={ag2.get('agent_ids', []).count(rec_id)} standby={ag2.get('standby')}")
                r = requests.post(f"{s}/agent/{rec_id}/message", timeout=T + 15, headers=auth,
                                  json={"text": "selftest post-reconnect routing"})
                m = jval(r) if r.ok else {}
                check("cycle: routing points at the new socket (task round-trip replies)",
                      r.ok and m.get("status") == "replied"
                      and (m.get("reply") or {}).get("text") == "selftest socket answer",
                      r.text[:160])
            if second_cli:
                second_cli.disconnect()
            time.sleep(0.5)

            # E) the unread nudge (v1.9). Section D pinned what a gap costs: nothing is queued, so
            #    everything aimed at the missing socket is simply lost. This checks the two things
            #    the hub now owes an agent that rejoins - that it is *told* what it missed, and that
            #    peer text is *held* instead of destroyed - and that being told happens once, not
            #    every 45 seconds forever.
            miss_id = f"st-miss-{suffix}"
            miss_bytes = json.dumps({"for": miss_id, "n": secrets.token_hex(4)}).encode()
            miss_sha = hashlib.sha256(miss_bytes).hexdigest()
            r = requests.post(f"{s}/file", timeout=T,
                              headers={**auth, "X-Target-Agent": miss_id},
                              files={"file": ("missed.json", miss_bytes)})
            up = jval(r) if r.status_code in (200, 201) else {}
            miss_fid = up.get("file_id", "")
            check("gap: upload for an offline target is stored and reports target_error",
                  bool(miss_fid) and up.get("delivered") is False
                  and "unread notice" in up.get("target_error", ""), r.text[:150])
            hb = jval(requests.get(f"{s}/health", timeout=T))
            check("gap: /health counts the file it now owes that agent",
                  (hb.get("unread") or {}).get("files_unannounced", 0) >= 1,
                  one_line(json.dumps(hb.get("unread")), 150))

            miss_cli, miss_box = sconn(miss_id)
            time.sleep(0.8)
            notice = (miss_box["unread"] or [None])[-1]
            check("nudge: the connect notice names the file nobody announced",
                  notice is not None and notice.get("reason") == "connect"
                  and (notice.get("unread") or {}).get("files", 0) >= 1
                  and any(f.get("file_id") == miss_fid and f.get("sha256") == miss_sha
                          for f in notice.get("files") or []),
                  "no unread event" if notice is None else one_line(json.dumps(notice), 150))
            listed = next((f for f in (notice or {}).get("files") or []
                           if f.get("file_id") == miss_fid), {})
            if listed.get("url"):
                r = requests.get(f"{s}{listed['url']}", timeout=T,
                                 headers={**hdr, "X-Agent-Token": miss_box["cred"]})
                check("nudge: the credential pushed with the notice can fetch the missed file",
                      r.status_code == 200 and r.content == miss_bytes,
                      f"{r.status_code} {len(r.content)} B")
            else:
                check("nudge: the credential pushed with the notice can fetch the missed file",
                      False, "no url in the notice")
            # v1.10 guards the two rows that used to state something untrue, checked where they
            # are actually produced. FILE_SENT used to print the *notify recipient* in the bold
            # agent column, so 41 live rows read "qoder2 | agent:hubmaster -> Server -> File
            # store" while qoder2 was offline all session. FILE_RCVD passed agent:"qoder", which
            # fails AGENT_ID_RE, with a direction that named nobody - so every download was
            # bucketed in no agent's GET /events/mine at all.
            if args.logtoken:
                evs = jval(requests.get(f"{s}/logs/{args.logtoken}/events.json?limit=3000",
                                        timeout=T)).get("events") or []
                ups = [e for e in evs if e.get("ref") == miss_fid
                       and e.get("event") == "FILE_SENT"]
                check("v1.10 FILE_SENT credits the uploader; the target is only a subject",
                      len(ups) == 1 and ups[0].get("frm") == "operator:selftest"
                      and ups[0].get("to") == "store" and ups[0].get("agent") == miss_id,
                      one_line(json.dumps(ups[:1]), 200))
            mm = jval(requests.get(f"{s}/events/mine?limit=300", timeout=T,
                                   headers={**auth, "X-Agent-Id": miss_id}))
            check("v1.10 an agent's own feed carries its download (index hole closed)",
                  any(e.get("event") == "FILE_RCVD" and e.get("ref") == miss_fid
                      for e in mm.get("events") or []),
                  one_line(json.dumps([e.get("event") for e in mm.get("events") or []]), 160))
            miss_cli.disconnect()
            time.sleep(1.0)

            r = requests.post(f"{s}/relay", timeout=T, headers=auth,
                              json={"to": miss_id, "text": "held for you, not dropped"})
            q = jval(r) if r.ok else {}
            check("relay: an offline target now gets status queued instead of 404",
                  r.status_code == 200 and q.get("status") == "queued"
                  and q.get("queue_depth") == 1 and q.get("msg_id"), r.text[:150])
            miss2_cli, miss2_box = sconn(miss_id)
            time.sleep(0.8)
            n2 = (miss2_box["unread"] or [None])[-1]
            peers = miss2_box["peers"]
            check("relay: the queued text arrives as an ordinary peer_msg with its sender intact",
                  any(p.get("text") == "held for you, not dropped"
                      and str(p.get("from", "")).startswith("operator") for p in peers),
                  one_line(json.dumps(peers), 150))
            check("nudge: that same notice reports what it flushed",
                  n2 is not None and (n2.get("unread") or {}).get("relays", 0) >= 1
                  and n2.get("relays_flushed") == len(peers),
                  "no unread event" if n2 is None else one_line(json.dumps(n2), 150))
            check("nudge: an already-announced file is not announced twice",
                  n2 is not None and not any(f.get("file_id") == miss_fid
                                             for f in n2.get("files") or []),
                  one_line(json.dumps((n2 or {}).get("files")), 120))
            ag3 = jval(requests.get(f"{s}/agents", timeout=T, headers=auth))
            det = (ag3.get("detail") or {}).get(miss_id) or {}
            check("nudge: /agents detail carries the relay backlog beside the inbox one",
                  "mail_backlog" in det and "inbox_backlog" in det, one_line(det, 140))
            declared = json.dumps(manifest.get("endpoints", []))
            check("nudge: /api declares every key the notice and /agents now answer with",
                  "mail_backlog" in declared and "queued" in declared
                  and "unread" in json.dumps((manifest.get("socket") or {}).get("hub_to_agent", {})),
                  "")
            miss2_cli.disconnect()
            time.sleep(1.0)

            # A task the agent never answered stays `delivered`, so it is the one unread item that
            # has no durable body behind it: the hub hands back handles, never the text. Eleven of
            # them prove the list is capped while the count stays honest.
            nag_id = f"st-nag-{suffix}"
            nag_cli, nag_box = sconn(nag_id)
            time.sleep(0.5)
            nag_ids = []
            for i in range(11):
                rr = requests.post(f"{s}/agent/{nag_id}/message?wait=0", timeout=T,
                                   headers=auth, json={"text": f"never answered {i}"})
                if rr.ok and jval(rr).get("msg_id"):
                    nag_ids.append(jval(rr)["msg_id"])
            nag_cli.disconnect()
            time.sleep(1.0)
            nag2_cli, nag2_box = sconn(nag_id)
            time.sleep(0.8)
            n3 = (nag2_box["unread"] or [None])[-1]
            check("nudge: every unanswered task is counted, and the list is capped at 10",
                  n3 is not None and len(nag_ids) == 11
                  and (n3.get("unread") or {}).get("tasks", 0) >= 11
                  and len(n3.get("task_ids") or []) == 10 and n3.get("tasks_truncated") is True,
                  one_line(json.dumps(n3), 160))
            if (n3 or {}).get("task_ids"):
                r = requests.get(f"{s}/result/{n3['task_ids'][0]}", timeout=T, headers=auth)
                row = jval(r) if r.ok else {}
                check("nudge: a listed task really is one that never got a result frame",
                      row.get("state") == "delivered" or row.get("status") == "delivered",
                      one_line(json.dumps(row), 140))
            else:
                check("nudge: a listed task really is one that never got a result frame",
                      False, "no task_ids in the notice")
            nag2_cli.disconnect()
            time.sleep(1.0)
            nag3_cli, nag3_box = sconn(nag_id)
            time.sleep(0.8)
            # The cap means one of the eleven was still owed to it, so a second notice is correct -
            # what must never happen is a *repeat*. Everything the two sockets were told has to be
            # a subset of the eleven, named once each.
            once = [m for b in (nag2_box, nag3_box) for n in b["unread"]
                    for m in (n.get("task_ids") or [])]
            check("nudge: no task handle is ever named twice (the never-twice rule)",
                  len(once) == len(set(once)) and set(once) <= set(nag_ids),
                  f"{len(once)} mentions, {len(set(once))} distinct, "
                  f"strangers {sorted(set(once) - set(nag_ids))[:2]}")
            nag3_cli.disconnect()

            # The 10-handle cap has to *defer*, not forget. Twelve files owed to one id is a name
            # list of 10 plus two that must still be owed afterwards - and the notice that names them
            # also has to flush a relay, because an earlier version cleared the whole pending set
            # whenever a queue drained, which lost those two announcements for good while telling
            # nobody. So the relay is queued on purpose, in the same notice.
            cap_id = f"st-cap-{suffix}"
            cap_fids = []
            for i in range(12):
                rr = requests.post(f"{s}/file", timeout=T,
                                   headers={**auth, "X-Target-Agent": cap_id},
                                   files={"file": (f"owed{i}.json",
                                                   json.dumps({"i": i, "s": suffix}).encode())})
                if rr.status_code in (200, 201) and jval(rr).get("file_id"):
                    cap_fids.append(jval(rr)["file_id"])
            requests.post(f"{s}/relay", timeout=T, headers=auth,
                          json={"to": cap_id, "text": "flush me with the twelve"})
            cap_cli, cap_box = sconn(cap_id)
            time.sleep(0.8)
            cn = (cap_box["unread"] or [None])[-1]
            named = [f.get("file_id") for f in (cn or {}).get("files") or []]
            check("nudge: 12 owed files are counted as 12 and named 10 at a time",
                  len(cap_fids) == 12 and (cn or {}).get("files") and cn["files_truncated"] is True
                  and len(named) == 10 and (cn.get("unread") or {}).get("files") == 12
                  and cn.get("relays_flushed") == 1,
                  one_line(json.dumps(cn), 150))
            cap_cli.disconnect()
            time.sleep(1.0)
            cap2_cli, cap2_box = sconn(cap_id)
            time.sleep(0.8)
            cn2 = (cap2_box["unread"] or [None])[-1]
            named2 = [f.get("file_id") for f in (cn2 or {}).get("files") or []]
            check("nudge: the surplus past the cap is still owed later, never dropped",
                  cn2 is not None and set(named2) == set(cap_fids) - set(named)
                  and cn2.get("files_truncated") in (False, None),
                  f"owed {len(cap_fids)}, first notice {len(named)}, second {named2}")
            cap2_cli.disconnect()

            health_decl = next((e for e in manifest.get("endpoints", [])
                                if e.get("path") == "/health"), {})
            hd = jval(requests.get(f"{s}/health", timeout=T))
            unread_h = hd.get("unread") or {}
            check("nudge: /health reports the cadence, and 0 means the periodic sweep is off",
                  "unread" in (health_decl.get("returns") or {}).get("keys", [])
                  and unread_h.get("every_seconds", -1) >= 0
                  and unread_h.get("enabled") is bool(unread_h.get("every_seconds"))
                  and unread_h.get("list_max") == 10 and unread_h.get("mail_queue_max") == 200,
                  one_line(json.dumps(unread_h), 160))
            if args.logtoken:
                r = requests.get(f"{s}/logs/{args.logtoken}", timeout=T + 15)
                check("nudge: its two new badges are styled (an undeclared one renders invisible)",
                      ".b-UNREAD_NUDGE" in r.text and ".b-MSG_QUEUED" in r.text,
                      f"{r.status_code}, css absent" if ".b-UNREAD_NUDGE" not in r.text else "")

    # ---- optional live round-trip
    if args.agent:
        r = requests.get(f"{s}/agent-id/{args.agent}", timeout=T, headers=auth)
        st = jval(r) if r.ok else {}
        check(f"GET /agent-id/'{args.agent}' reports taken + holder sid",
              r.ok and st.get("available") is False and st.get("taken_by_sid")
              and isinstance(st.get("standby_sockets"), int) and st.get("connected_at"),
              r.text[:140])
        r = requests.post(f"{s}/agent/{args.agent}/message", timeout=T + 40,
                          headers=auth, json={"text": "selftest ping"})
        m = jval(r) if r.ok else {}
        check(f"task round-trip to '{args.agent}' replies",
              r.ok and m.get("status") == "replied" and m.get("reply"), r.text[:120])
        mid = m.get("msg_id", "")
        r = requests.get(f"{s}/result/{mid}", timeout=T, headers=auth)
        res = jval(r) if r.ok else {}
        # An auto-reply agent emits ACK (kind=ack) + the real answer, so results may have
        # more than one entry and results[-1] (the answer) != m["reply"] (the ACK text the
        # POST returned).  Accept: the POST's reply text appears in *any* result entry.
        reply_text = m.get("reply", "")
        results = res.get("results", [])
        reply_in_results = any(
            (isinstance(r_entry, dict) and r_entry.get("text") == reply_text)
            or r_entry == reply_text
            for r_entry in results
        ) if reply_text else len(results) >= 1
        check(f"GET /result/<msg_id> correlates the reply to '{args.agent}'",
              r.ok and res.get("msg_id") == mid
              and res.get("agent") == f"agent:{args.agent}"
              and len(results) >= 1
              and {"count", "note", "status", "updated"} <= set(res)
              and reply_in_results, r.text[:160])
        r = requests.get(f"{s}/events/mine", timeout=T,
                         headers={**auth, "X-Agent-Id": args.agent})
        check(f"events/mine as '{args.agent}' shows the task",
              r.ok and any(mid in e.get("payload", "") for e in jval(r).get("events", [])),
              r.text[:160])
        if args.logtoken:
            r = requests.get(f"{s}/logs/{args.logtoken}/events.json?limit=3000", timeout=T)
            evs = (jval(r).get("events") or []) if r.ok else []
            rows = [e for e in evs
                    if mid in e.get("payload", "") and e.get("event") == "MSG_RCVD"]
            # An auto-reply agent emits ACK + real answer, so there may be 2 MSG_RCVD rows.
            # What matters is at least 1; zero would mean the result was silently swallowed.
            check("at least one MSG_RCVD log row per result (no silent drops)",
                  len(rows) >= 1, str(rows)[:200])
            # v1.10: the row must say WHO CALLED. Pre-v1.10 the HTTP task path logged the
            # literal "Client -> Server -> Agent", so 19 of 50 MSG_SENT rows had no caller
            # anywhere and the page could not be read as a record of who said what to whom.
            # All of this needs a real msg_id -- with the agent offline the round-trip above
            # never ran, mid is "", and every "ref == mid" filter would match the whole ring.
            if mid:
                sent = [e for e in evs if e.get("event") == "MSG_SENT" and e.get("ref") == mid]
                check("v1.10 who-column: the MSG_SENT row carries frm/to/ref as FIELDS",
                      len(sent) >= 1 and sent[0].get("to") == args.agent
                      and bool(sent[0].get("frm")) and "->" in sent[0].get("dir", ""),
                      one_line(json.dumps(sent[:1]), 200))
                rcvd = [e for e in rows if e.get("ref") == mid]
                check("v1.10 who-column: the reply row names both ends (agent -> caller)",
                      len(rcvd) >= 1 and rcvd[0].get("frm") == args.agent
                      and bool(rcvd[0].get("to")),
                      one_line(json.dumps(rcvd[:1]), 200))
                page = requests.get(f"{s}/logs/{args.logtoken}?n=3000", timeout=T + 15)
                check("v1.10 page: the task reads as an arrow between two named ends",
                      page.ok and f"@{args.agent}" in page.text and f"#{mid[:16]}" in page.text,
                      f"{page.status_code}, '@{args.agent}' "
                      + ("there" if f"@{args.agent}" in page.text else "ABSENT"))
                qpage = requests.get(f"{s}/logs/{args.logtoken}?q={mid}", timeout=T + 15)
                check("v1.10 filter ?q=<msg_id> isolates the rows for one task",
                      qpage.ok and "MSG_SENT" in qpage.text.split("<div id='log'>")[-1],
                      f"{qpage.status_code}")
            nopage = requests.get(f"{s}/logs/{args.logtoken}?agent=no_such_agent_zz",
                                  timeout=T + 15)
            nbody = nopage.text.split("<div id='log'>")[-1]
            check("v1.10 a filter matching nothing says so instead of rendering a blank page",
                  nopage.ok and "no rows match" in nbody.lower() and "class='row" not in nbody,
                  f"{nopage.status_code}, body {len(nbody)} bytes")
            probe = next((e.get("to") or e.get("frm") for e in reversed(evs)
                          if re.fullmatch(r"[A-Za-z0-9_-]{1,40}", e.get("to") or e.get("frm") or "")),
                         "")
            fpage = requests.get(f"{s}/logs/{args.logtoken}?agent={probe}&n=3000", timeout=T + 15)
            fbody = fpage.text.split("<div id='log'>")[-1]
            check("v1.10 filter ?agent= keeps rows naming that id",
                  probe != "" and fpage.ok and "class='row" in fbody and f"@{probe}" in fbody,
                  f"{fpage.status_code}, probe '{probe}', {len(fbody)} bytes")
            f1 = requests.get(f"{s}/logs/{args.logtoken}?fold=1&n=3000", timeout=T + 15)
            check("v1.10 ?fold=1 renders (a fold bug blanks the page, it does not error)",
                  f1.ok and len(f1.text) > 2000 and "MSG" in f1.text, f"{f1.status_code}")
            # v1.11.2: "clear" must actually clear. `href=''` resolves to the current URL INCLUDING
            # its query string, so the click re-requested the filter it was meant to drop and the
            # button read as dead. Followed as a browser follows it, then judged on the page that
            # comes back - no scope line, nothing left in the form, more rows than the filter
            # showed. Matching the href string alone would pass a link that resolves elsewhere.
            def _rows(p):
                m = re.search(r"class='sub'>(\d+) of (\d+) rows", p.text)
                return int(m.group(1)) if m else -1

            for cname, cqs in (("agent filter", f"?agent={probe or 'zz'}"),
                               ("no-match filter", "?agent=no_such_agent_zz"),
                               ("fold + search + n", "?fold=1&q=zzz&n=5")):
                cpage = requests.get(f"{s}/logs/{args.logtoken}{cqs}", timeout=T + 15)
                hrefs = re.findall(r"href='([^']*)'>clear", cpage.text)
                landed = [requests.get(follow_link(cpage.url, h), timeout=T + 15) for h in hrefs]
                before = _rows(cpage)
                check(f"v1.11.2 the clear link drops the filter ({cname})",
                      bool(hrefs) and all(c.ok and "filtered to" not in c.text
                                          and "placeholder='agent id' value=''" in c.text
                                          and "placeholder='msg_id / text' value=''" in c.text
                                          and "name='fold'" not in c.text
                                          and _rows(c) > before for c in landed),
                      f"{cqs}: hrefs {hrefs} -> rows {before} became {[_rows(c) for c in landed]}, "
                      f"scope left on any: {[('filtered-to' if 'filtered to' in c.text else '')
                                            for c in landed]}")
        # v1.5: an emitted task is a ledger row even if nobody answers it, and the caller
        # label comes from which credential authenticated
        r = requests.post(f"{s}/agent/{args.agent}/message?wait=0", timeout=T, headers=auth,
                          json={"text": "selftest ledger probe"})
        led = jval(r) if r.ok else {}
        mid2 = led.get("msg_id", "")
        check("POST wait=0 answers with the ledger handle",
              r.ok and led.get("task_state") in ("delivered", "acked")
              and led.get("result_endpoint") == f"/result/{mid2}", r.text[:160])
        r = requests.get(f"{s}/result/{mid2}", timeout=T, headers=auth)
        row = jval(r) if r.ok else {}
        check("the task is readable as a ledger row (state + deadline), not a 404",
              r.ok and row.get("state") in ("delivered", "acked", "answered")
              and row.get("deadline_at") and row.get("delivered_at")
              and row.get("hub_issued") is True, r.text[:180])
        check("an operator-token call is labeled operator, never agent",
              str(row.get("from", "")).startswith("operator:"), str(row.get("from")))
        r = requests.get(f"{s}/tasks/dead-letter", timeout=T, headers=auth)
        dlj = jval(r) if r.ok else {}
        check("the waiting task shows up as outstanding for this agent",
              dlj.get("outstanding_by_agent", {}).get(args.agent, 0) >= 1
              or dlj.get("states", {}).get("answered", 0) >= 1, str(dlj)[:180])

    # ================= round 3: item 1 (cadence, not render), item 2, item 3, pain (e)
    # The rings are the source of truth and /logs renders from them on read, so batching the
    # DISK is allowed and delaying a RENDER is not. Burst enough offline relays that every one
    # logs a row with a unique agent id, then demand the newest in both readers immediately.
    burst = [f"stburst-{secrets.token_hex(3)}-{i}" for i in range(40)]
    hm0 = jval(requests.get(f"{s}/health", timeout=T)).get("log_mirror") or {}
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
        rows = (jval(ej).get("events") if ej.ok else []) or []
        check("freshness: newest event is in events.json on read",
              ej.ok and any(e.get("agent") == newest for e in rows), f"{len(rows)} rows")
    hm1 = jval(requests.get(f"{s}/health", timeout=T)).get("log_mirror") or {}
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
    ij = jval(ie) if ie.ok else {}
    me = requests.get(f"{s}/events/mine?limit=500&mentions=1",
                      headers={**auth, "X-Agent-Id": newest}, timeout=T)
    mj = jval(me) if me.ok else {}
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
    fj = jval(requests.get(f"{s}/events/mine", headers={**auth, "X-Agent-Id": fresh},
                           timeout=T))
    ghost = jval(requests.get(f"{s}/events/mine",
                              headers={**auth, "X-Agent-Id": "stidx-never-seen"}, timeout=T))
    check("events/mine says whether this caller is index-backed (indexed=true for a live id)",
          fj.get("indexed") is True and fj.get("total_matching", 0) >= 1, str(fj)[:150])
    check("events/mine reports indexed=false rather than a silent empty feed",
          ghost.get("indexed") is False and ghost.get("count") == 0, str(ghost)[:150])

    # /files payload trimming, additive: no params keeps the legacy shape exactly.
    payload = json.dumps({"selftest": secrets.token_hex(3)}).encode()
    up = requests.post(f"{s}/file", headers=auth, timeout=T,
                       files={"file": ("st-ids.json", payload)})
    fid = jval(up).get("file_id", "") if up.status_code in (200, 201) else ""
    one = requests.get(f"{s}/files?ids={fid}", headers=auth, timeout=T)
    oj = jval(one) if one.ok else {}
    check("GET /files?ids=<one> answers with exactly that row plus total_matching",
          bool(fid) and one.ok and oj.get("count") == 1 and fid in (oj.get("files") or {})
          and "total_matching" in oj, str(oj)[:160])
    lim = requests.get(f"{s}/files?limit=1", headers=auth, timeout=T)
    lj = jval(lim) if lim.ok else {}
    check("GET /files?limit=1 trims and says it trimmed",
          lim.ok and lj.get("count") == 1 and lj.get("trimmed") is True, str(lj)[:160])
    whole = requests.get(f"{s}/files", headers=auth, timeout=T)
    wj = jval(whole) if whole.ok else {}
    check("GET /files with no params keeps the legacy shape (whole table, no trim keys)",
          whole.ok and fid in (wj.get("files") or {}) and "trimmed" not in wj, str(wj)[:120])

    # pain (e): a documented key list that has drifted from the response is worse than none.
    man = jval(requests.get(f"{s}/api", headers=auth, timeout=T))

    def declared(method, path):
        for e in man.get("endpoints", []):
            if e.get("method") == method and e.get("path") == path:
                return {k.split(" ")[0] for k in (e.get("returns") or {}).get("keys") or []}
        return set()

    drift = requests.post(f"{s}/file", headers=auth, timeout=T,
                          files={"file": ("st-drift.json", payload)})
    got = set(jval(drift)) if drift.status_code in (200, 201) else set()
    undeclared = sorted(got - declared("POST", "/file") - {"docs", "api", "hint"})
    check("POST /file answers only with keys /api declares (pain: doc drift)",
          bool(got) and not undeclared, f"undeclared: {undeclared}")
    hk = set(jval(requests.get(f"{s}/health", timeout=T)))
    check("/health answers only with keys /api declares",
          hk and not sorted(hk - declared("GET", "/health")),
          f"undeclared: {sorted(hk - declared('GET', '/health'))}")

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("failed checks: " + ", ".join(FAIL), file=sys.stderr)
    return 1 if FAIL else 0


if __name__ == "__main__":
    try:
        rc = main()
    except KeyboardInterrupt:
        print("\nERROR  interrupted", file=sys.stderr)
        rc = 130
    except Exception as e:
        if os.environ.get("SELFTEST_DEBUG"):
            raise
        rc = bail(f"selftest stopped after {len(PASS) + len(FAIL)} checks",
                  f"{type(e).__name__}: {one_line(e, 200)}",
                  f"{len(PASS)} passed, {len(FAIL)} failed before the stop.",
                  "The run ended on an error, not a failed check: re-run with SELFTEST_DEBUG=1 "
                  "for the traceback, and report which line stopped it.")
    sys.exit(rc)
