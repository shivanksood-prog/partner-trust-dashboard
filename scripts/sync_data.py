"""
Partner Trust Line — Agent PSAT Dashboard sync
Source: Google Sheet "Calling Sheet" (gid=0) — one row per PSAT call attempt.
Joined to Kapture by Ticket ID to recover agent (assignedToName).

Runs every 15 min via GitHub Actions, commits data.json + state.json.
"""

from __future__ import annotations
import csv, io, json, os, re, time
from datetime import datetime, timezone
from typing import Any

import requests

# ── Config ────────────────────────────────────────────────────────────────────

SHEET_ID    = "1HB79kOjNIeHJXPlDET4ksE3QiUvPi6MfG6i5RSw-gm4"
SHEET_GID   = "0"                              # "Calling Sheet" tab
SHEET_URL   = (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
               f"/export?format=csv&gid={SHEET_GID}")

KAPTURE_API_URL = "https://wiomin.kapturecrm.com/search-ticket-by-ticket-id.html/v.2.0"
KAPTURE_AUTH    = os.environ.get("KAPTURE_AUTH", "")  # required from env / GH secret

DATA_FILE  = "data/data.json"
STATE_FILE = "data/state.json"

L1_BUCKETS         = {"L1"}
L2_BUCKETS         = {"L2"}
NEWPROJECT_BUCKETS = {"NewProject", "NEW PROJECT", "New Project"}

CONNECTED_STATUSES = {"Connected"}
FEEDBACK_OUTCOMES  = {"Satisfied", "Not Satisfied"}   # PSAT denominator
POSITIVE_OUTCOMES  = {"Satisfied"}                    # PSAT numerator

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_csv(url: str) -> list[dict]:
    resp = requests.get(url, allow_redirects=True, timeout=60)
    resp.raise_for_status()
    return list(csv.DictReader(io.StringIO(resp.text)))


def load_json(path: str, default):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return default


def save_json(path: str, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, separators=(",", ":"))


def normalize_bucket(b: str) -> str:
    """Fold all NewProject variants to a single label; pass L1/L2 through."""
    b = (b or "").strip()
    if b in NEWPROJECT_BUCKETS:
        return "NewProject"
    return b


def view_for_bucket(bucket: str) -> str | None:
    """Which dashboard tab this row belongs to."""
    if bucket in L2_BUCKETS:
        return "L2"
    if bucket in L1_BUCKETS or bucket in NEWPROJECT_BUCKETS:
        return "L1_NEWPROJ"
    return None


def parse_date_ddmmyyyy(s: str) -> str:
    """Normalize DD/MM/YYYY → ISO YYYY-MM-DD. Tolerate a couple of fallbacks."""
    s = (s or "").strip()
    if not s:
        return ""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s.split()[0], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s[:10]


def grade(psat_pct: float | None) -> str:
    if psat_pct is None:
        return "N/A"
    if psat_pct >= 90: return "A"
    if psat_pct >= 80: return "B"
    if psat_pct >= 70: return "C"
    if psat_pct >= 60: return "D"
    return "E"

# ── Calling Sheet → call rows ─────────────────────────────────────────────────

def read_calling_sheet() -> list[dict]:
    rows = fetch_csv(SHEET_URL)
    out = []
    for r in rows:
        date_iso = parse_date_ddmmyyyy(r.get("Date", ""))
        if not date_iso:
            continue
        bucket = normalize_bucket(r.get("Bucket", ""))
        if not bucket:
            continue
        view = view_for_bucket(bucket)
        if view is None:
            continue
        ticket_id = (r.get("Ticket ID") or "").strip()
        if not ticket_id:
            continue

        calling_status = (r.get("Calling Status") or "").strip()
        sat_status     = (r.get("Satisfaction Status") or "").strip()

        is_connected   = calling_status in CONNECTED_STATUSES
        is_feedback    = is_connected and sat_status in FEEDBACK_OUTCOMES
        is_satisfied   = is_feedback and sat_status in POSITIVE_OUTCOMES

        out.append({
            "date":           date_iso,
            "category":       (r.get("Category") or "").strip(),
            "sub_category":   (r.get("Sub Category") or "").strip(),
            "phone":          (r.get("Phone") or "").strip(),
            "question":       (r.get("Question by Partner") or "").strip()[:500],
            "final_bucket":   (r.get("Final Bucket") or "").strip(),
            "bucket":         bucket,
            "view":           view,
            "ticket_id":      ticket_id,
            "calling_status": calling_status,
            "sat_status":     sat_status,
            "is_connected":   is_connected,
            "is_feedback":    is_feedback,
            "is_satisfied":   is_satisfied,
            "remarks":        (r.get("REMARKS") or "").strip()[:300],
        })
    return out

# ── Kapture (agent + URL) ─────────────────────────────────────────────────────

def fetch_kapture_agents(ticket_ids: list[str]) -> dict[str, dict]:
    """Returns {ticket_id: {agent, url}}. Best-effort, never raises."""
    if not ticket_ids or not KAPTURE_AUTH:
        return {}

    headers = {"Authorization": KAPTURE_AUTH, "Content-Type": "application/json"}
    payload_base = {
        "history_type":                "all",
        "conversation_type":           "notes",
        "read_ticket_history_details": "1",
    }
    out: dict[str, dict] = {}

    # Try batch first; fall back to per-ticket on any failure.
    BATCH = 50
    for i in range(0, len(ticket_ids), BATCH):
        chunk = ticket_ids[i:i + BATCH]
        try:
            resp = requests.post(
                KAPTURE_API_URL, headers=headers, timeout=60,
                json={**payload_base, "ticket_ids": ",".join(chunk)},
            )
            data = resp.json()
            if isinstance(data, list) and len(data) == len(chunk):
                for obj in data:
                    rec = _extract_kapture(obj)
                    if rec:
                        out[rec["ticket_id"]] = {"agent": rec["agent"], "url": rec["url"]}
                continue   # batch worked, move on
        except Exception as e:
            print(f"  Kapture batch failed: {e}")

        for tid in chunk:
            try:
                resp = requests.post(
                    KAPTURE_API_URL, headers=headers, timeout=30,
                    json={**payload_base, "ticket_ids": tid},
                )
                data = resp.json()
                if isinstance(data, list) and data:
                    rec = _extract_kapture(data[0])
                    if rec:
                        out[rec["ticket_id"]] = {"agent": rec["agent"], "url": rec["url"]}
            except Exception as e:
                print(f"    Failed ticket {tid}: {e}")
            time.sleep(0.15)

    return out


def _extract_kapture(api_obj: dict) -> dict | None:
    td = api_obj.get("task_details", {})
    tid = str(td.get("ticketId", "")).strip()
    if not tid:
        return None
    return {
        "ticket_id": tid,
        "agent":     (td.get("assignedToName") or "Unassigned").strip() or "Unassigned",
        "url":       td.get("url", ""),
    }

# ── Aggregation ───────────────────────────────────────────────────────────────

def aggregate(call_rows: list[dict], view: str) -> list[dict]:
    """One agent leaderboard for a given view (L2 or L1_NEWPROJ)."""
    rows = [r for r in call_rows if r["view"] == view]

    by_agent: dict[str, dict] = {}
    for r in rows:
        agent = r.get("agent") or "Unassigned"
        a = by_agent.setdefault(agent, {
            "name":         agent,
            "total":        0,    # all call rows for this agent in view
            "connected":    0,
            "feedback":     0,    # PSAT denominator
            "satisfied":    0,    # PSAT numerator
            "not_sat":      0,
            "call_back":    0,
            "dnp":          0,
            "uncalled":     0,
            "daily":        {},   # date → {feedback, satisfied}
            "tickets":      [],
        })
        a["total"]     += 1
        if r["is_connected"]: a["connected"] += 1
        if r["is_feedback"]:  a["feedback"]  += 1
        if r["is_satisfied"]: a["satisfied"] += 1
        if r["sat_status"] == "Not Satisfied": a["not_sat"]   += 1
        if r["sat_status"] == "Call Back":     a["call_back"] += 1
        if r["calling_status"].startswith("DNP"): a["dnp"]      += 1
        if not r["calling_status"]:               a["uncalled"] += 1

        d = r["date"]
        if d:
            day = a["daily"].setdefault(d, {"feedback": 0, "satisfied": 0})
            if r["is_feedback"]:  day["feedback"]  += 1
            if r["is_satisfied"]: day["satisfied"] += 1

        a["tickets"].append({
            "date":           r["date"],
            "phone":          r["phone"],
            "bucket":         r["bucket"],
            "final_bucket":   r["final_bucket"],
            "category":       r["category"],
            "sub_category":   r["sub_category"],
            "question":       r["question"],
            "ticket_id":      r["ticket_id"],
            "url":            r.get("url", ""),
            "calling_status": r["calling_status"],
            "sat_status":     r["sat_status"],
            "remarks":        r["remarks"],
        })

    agents = []
    for name, a in by_agent.items():
        psat_pct = round(a["satisfied"] / a["feedback"] * 100, 1) if a["feedback"] else None
        daily = [
            {"date": d, "feedback": v["feedback"], "satisfied": v["satisfied"],
             "psat_pct": round(v["satisfied"] / v["feedback"] * 100, 1) if v["feedback"] else None}
            for d, v in sorted(a["daily"].items())
        ]
        tickets = sorted(a["tickets"], key=lambda x: x["date"], reverse=True)
        agents.append({
            "name":      name,
            "total":     a["total"],
            "connected": a["connected"],
            "feedback":  a["feedback"],
            "satisfied": a["satisfied"],
            "not_sat":   a["not_sat"],
            "call_back": a["call_back"],
            "dnp":       a["dnp"],
            "uncalled":  a["uncalled"],
            "psat_pct":  psat_pct,
            "grade":     grade(psat_pct),
            "daily":     daily,
            "tickets":   tickets,
        })

    # Sort: graded first (A→E by PSAT% desc), then unranked (None) by volume.
    grade_order = {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4, "N/A": 5}
    agents.sort(key=lambda x: (
        grade_order[x["grade"]],
        -(x["psat_pct"] if x["psat_pct"] is not None else -1),
        -x["feedback"],
    ))
    return agents


def overall_summary(call_rows: list[dict]) -> dict:
    feedback  = sum(1 for r in call_rows if r["is_feedback"])
    satisfied = sum(1 for r in call_rows if r["is_satisfied"])
    connected = sum(1 for r in call_rows if r["is_connected"])
    return {
        "total_rows": len(call_rows),
        "connected":  connected,
        "feedback":   feedback,
        "satisfied":  satisfied,
        "psat_pct":   round(satisfied / feedback * 100, 1) if feedback else None,
    }

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=== Partner Trust Line — Agent PSAT sync ===")

    state = load_json(STATE_FILE, {"agents": {}})
    agent_cache: dict[str, dict] = state.get("agents", {})  # ticket_id → {agent, url}
    print(f"State: {len(agent_cache)} cached ticket→agent mappings")

    print("Fetching Calling Sheet…")
    call_rows = read_calling_sheet()
    print(f"  → {len(call_rows)} usable PSAT rows")

    unique_ticket_ids = sorted({r["ticket_id"] for r in call_rows})
    to_fetch = [tid for tid in unique_ticket_ids if tid not in agent_cache]
    print(f"  Unique tickets: {len(unique_ticket_ids)} · new to fetch: {len(to_fetch)}")

    if to_fetch:
        fetched = fetch_kapture_agents(to_fetch)
        agent_cache.update(fetched)
        print(f"  Resolved {len(fetched)} agents from Kapture")

    # Attach agent + url to each call row.
    for r in call_rows:
        rec = agent_cache.get(r["ticket_id"], {})
        r["agent"] = rec.get("agent") or "Unassigned"
        r["url"]   = rec.get("url", "")

    # Save state.
    save_json(STATE_FILE, {"agents": agent_cache})

    # Aggregate per view.
    l2_agents          = aggregate(call_rows, "L2")
    l1_newproj_agents  = aggregate(call_rows, "L1_NEWPROJ")

    # Per-view summaries.
    l2_summary         = overall_summary([r for r in call_rows if r["view"] == "L2"])
    l1_newproj_summary = overall_summary([r for r in call_rows if r["view"] == "L1_NEWPROJ"])
    overall            = overall_summary(call_rows)

    # Date range for the hero strip.
    dates = sorted({r["date"] for r in call_rows if r["date"]})
    date_range = {"from": dates[0] if dates else "", "to": dates[-1] if dates else ""}

    data = {
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "date_range":   date_range,
        "overall":      overall,
        "views": {
            "L2": {
                "label":   "L2",
                "summary": l2_summary,
                "agents":  l2_agents,
            },
            "L1_NEWPROJ": {
                "label":   "L1 + NewProject",
                "summary": l1_newproj_summary,
                "agents":  l1_newproj_agents,
            },
        },
    }
    save_json(DATA_FILE, data)

    print(f"Done. {overall['feedback']} feedback calls · PSAT {overall['psat_pct']}% · "
          f"L2 agents={len(l2_agents)} · L1+NewProj agents={len(l1_newproj_agents)}")


if __name__ == "__main__":
    main()
