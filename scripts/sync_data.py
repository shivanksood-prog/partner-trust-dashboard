"""
Partner Trust Line - Kapture Data Sync
Runs every 15 minutes via GitHub Actions.
Reads ticket IDs from Google Sheet, fetches from Kapture API, merges PSAT scores.
"""

import requests
import json
import csv
import io
import os
import re
import time
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
KAPTURE_API_URL = "https://wiomin.kapturecrm.com/search-ticket-by-ticket-id.html/v.2.0"
KAPTURE_AUTH    = os.environ.get(
    "KAPTURE_AUTH",
    "Basic cGgwYmg3eDJhZWljenZ3aHIxdmdwZ20wcmprcDVycms2ZzZvZTJqZG1pM3ZrdDh3N20="
)

TICKETS_SHEET = (
    "https://docs.google.com/spreadsheets/d/"
    "1jJwFZ6nOu-sx3_EN-lk9t_Q4jIqYQoQTaJhRfNwoRsg"
    "/export?format=csv&gid=427384486"
)
PSAT_SHEET = (
    "https://docs.google.com/spreadsheets/d/"
    "1GJ3FIOepe3VmqeNinEYHJP-iMKmIM76_obN8znQauwM"
    "/export?format=csv&gid=64685464"
)

DATA_FILE  = "data/data.json"
STATE_FILE = "data/state.json"   # lightweight ticket store (not served to browser)

HEADERS = {
    "Authorization": KAPTURE_AUTH,
    "Content-Type":  "application/json",
}

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


def parse_tat_minutes(s: str) -> int:
    if not s:
        return 0
    mins = 0
    m = re.search(r"(\d+)\s*Day",  s); mins += int(m.group(1)) * 1440 if m else 0
    m = re.search(r"(\d+)\s*Hr",   s); mins += int(m.group(1)) * 60   if m else 0
    m = re.search(r"(\d+)\s*Min",  s); mins += int(m.group(1))        if m else 0
    return mins


def fmt_tat(mins: int) -> str:
    if mins <= 0:
        return "—"
    if mins < 60:
        return f"{mins}m"
    h, m = divmod(mins, 60)
    if h < 24:
        return f"{h}h {m}m" if m else f"{h}h"
    d, hr = divmod(h, 24)
    return f"{d}d {hr}h" if hr else f"{d}d"


def parse_kapture_date(s: str):
    """Parse '16-10-2025 12:09' → datetime, return None on fail."""
    for fmt in ("%d-%m-%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except Exception:
            pass
    return None

# ── Kapture API ───────────────────────────────────────────────────────────────

def fetch_kapture_batch(ticket_ids: list[str]) -> list[dict]:
    """Try comma-separated batch; fall back to one-by-one."""
    if not ticket_ids:
        return []

    payload = {
        "ticket_ids":               ",".join(ticket_ids),
        "history_type":             "all",
        "conversation_type":        "notes",
        "read_ticket_history_details": "1",
    }
    try:
        resp = requests.post(KAPTURE_API_URL, headers=HEADERS, json=payload, timeout=60)
        data = resp.json()
        if isinstance(data, list) and len(data) == len(ticket_ids):
            return data
        # API may not support batch — fall through
    except Exception as e:
        print(f"  Batch call failed ({e}), retrying individually…")

    results = []
    for tid in ticket_ids:
        try:
            resp = requests.post(
                KAPTURE_API_URL, headers=HEADERS,
                json={**payload, "ticket_ids": tid}, timeout=30
            )
            data = resp.json()
            if isinstance(data, list) and data:
                results.append(data[0])
        except Exception as e:
            print(f"  Failed ticket {tid}: {e}")
        time.sleep(0.2)   # polite rate limit
    return results


def extract_ticket(api_obj: dict) -> dict | None:
    td = api_obj.get("task_details", {})
    tid = str(td.get("ticketId", "")).strip()
    if not tid:
        return None

    status    = td.get("status", "")
    substatus = td.get("substatus", "")
    is_closed = (status == "Complete") or (substatus in ("Completed",))

    created_dt = parse_kapture_date(td.get("date", ""))
    closed_dt  = parse_kapture_date(td.get("taskEnddate", ""))

    tat_mins = parse_tat_minutes(td.get("tat", ""))
    if tat_mins == 0 and created_dt and closed_dt:
        tat_mins = max(0, int((closed_dt - created_dt).total_seconds() / 60))

    # Issue bucket — last 2 segments of disposition path
    disposition = td.get("disposition", "")
    parts = [p.strip() for p in disposition.split("|") if p.strip()]
    bucket = " › ".join(parts[-2:]) if len(parts) >= 2 else (parts[-1] if parts else "")

    # Question by partner — try multiple paths in additional_info
    ai = api_obj.get("additional_info", {})
    question = (
        ai.get("partner_details_add_info", {}).get("question_by_partner", "")
        or ai.get("ticket_source_info", {}).get("partner_app_ticket_title", "")
        or td.get("title", "")
    )

    return {
        "id":             tid,
        "agent":          td.get("assignedToName", "Unassigned").strip() or "Unassigned",
        "status":         status,
        "substatus":      substatus,
        "is_closed":      is_closed,
        "created":        td.get("date", ""),
        "date":           created_dt.strftime("%Y-%m-%d") if created_dt else "",
        "closed_at":      td.get("taskEnddate", ""),
        "tat_mins":       tat_mins,
        "title":          td.get("title", ""),
        "disposition":    disposition,
        "bucket":         bucket,
        "question":       question[:250].strip(),
        "url":            td.get("url", ""),
        "psat":           None,    # filled later
        "calling_status": None,    # filled later
        "call_dt":        None,    # filled later
    }

# ── PSAT ──────────────────────────────────────────────────────────────────────

def load_psat() -> dict[str, dict]:
    """Returns {ticket_id: {psat, calling_status, call_dt}} for ALL called rows."""
    rows = fetch_csv(PSAT_SHEET)
    out = {}
    for row in rows:
        calling_status = row.get("Calling Status", "").strip()
        if not calling_status:
            continue
        tid = row.get("ticket_id", "").strip() or row.get("ptl_ticket_id", "").strip()
        if not tid:
            continue
        connected = row.get("Connected", "").strip() == "1"
        psat = None
        if connected:
            try:
                psat = int(row.get("PSAT", "0").strip())
            except ValueError:
                psat = 0
        # call_dt is the date the PSAT call was made (YYYY-MM-DD)
        call_dt = row.get("call_dt", "").strip()[:10]   # "2026-02-17"
        out[tid] = {"psat": psat, "calling_status": calling_status, "call_dt": call_dt}
    return out

# ── Aggregation ───────────────────────────────────────────────────────────────

def aggregate(tickets: dict[str, dict]) -> tuple[dict, list, list]:
    """Returns (summary, agents_list, open_tickets_list)."""
    agent_acc: dict[str, dict] = {}

    for tid, t in tickets.items():
        agent = t.get("agent", "Unassigned")
        if agent not in agent_acc:
            agent_acc[agent] = {
                "total": 0, "open": 0, "closed": 0,
                "tat_sum": 0, "tat_n": 0,
                "psat_sum": 0, "psat_n": 0,
                "daily": {},
                "ticket_list": [],
            }
        a = agent_acc[agent]
        a["total"] += 1

        if t.get("is_closed"):
            a["closed"] += 1
            tat = t.get("tat_mins", 0)
            if tat > 0:
                a["tat_sum"] += tat
                a["tat_n"]   += 1
        else:
            a["open"] += 1

        psat = t.get("psat")
        if psat is not None:
            a["psat_sum"] += psat
            a["psat_n"]   += 1

        d = t.get("date", "")
        if d:
            if d not in a["daily"]:
                a["daily"][d] = {"total": 0, "closed": 0, "tat_sum": 0, "tat_n": 0}
            a["daily"][d]["total"] += 1
            if t.get("is_closed"):
                a["daily"][d]["closed"] += 1
                tat = t.get("tat_mins", 0)
                if tat > 0:
                    a["daily"][d]["tat_sum"] += tat
                    a["daily"][d]["tat_n"]   += 1

        # Per-ticket display record
        a["ticket_list"].append({
            "id":             tid,
            "date":           d,
            "call_dt":        t.get("call_dt", ""),
            "bucket":         t.get("bucket", ""),
            "question":       t.get("question", ""),
            "psat":           t.get("psat"),
            "calling_status": t.get("calling_status"),
            "tat":            fmt_tat(t.get("tat_mins", 0)) if t.get("is_closed") else "Open",
            "url":            t.get("url", ""),
            "closed":         t.get("is_closed", False),
        })

    agents = []
    for name, a in agent_acc.items():
        res_pct   = round(a["closed"] / a["total"] * 100, 1) if a["total"] else 0
        avg_tat   = round(a["tat_sum"] / a["tat_n"]) if a["tat_n"] else 0
        psat_pct  = round(a["psat_sum"] / a["psat_n"] * 100, 1) if a["psat_n"] else None

        if psat_pct is None:
            grade = "N/A"
        elif psat_pct >= 90: grade = "A"
        elif psat_pct >= 80: grade = "B"
        elif psat_pct >= 70: grade = "C"
        elif psat_pct >= 60: grade = "D"
        else:                grade = "E"

        # Build daily list sorted descending
        daily_list = []
        for day in sorted(a["daily"].keys(), reverse=True)[:60]:  # last 60 days
            dd = a["daily"][day]
            day_tat = round(dd["tat_sum"] / dd["tat_n"]) if dd["tat_n"] else 0
            daily_list.append({
                "date":    day,
                "total":   dd["total"],
                "closed":  dd["closed"],
                "res_pct": round(dd["closed"] / dd["total"] * 100, 1) if dd["total"] else 0,
                "avg_tat": fmt_tat(day_tat),
            })

        # Tickets sorted newest first
        ticket_list = sorted(a["ticket_list"], key=lambda x: x["date"], reverse=True)

        agents.append({
            "name":         name,
            "total":        a["total"],
            "open":         a["open"],
            "closed":       a["closed"],
            "res_pct":      res_pct,
            "avg_tat_mins": avg_tat,
            "avg_tat":      fmt_tat(avg_tat),
            "psat_pct":     psat_pct,
            "psat_n":       a["psat_n"],
            "tickets":      ticket_list,
            "grade":        grade,
            "daily":        daily_list,
        })

    agents.sort(key=lambda x: x["total"], reverse=True)

    # Summary
    total  = len(tickets)
    closed = sum(1 for t in tickets.values() if t.get("is_closed"))
    open_c = total - closed
    tat_vals = [t["tat_mins"] for t in tickets.values()
                if t.get("is_closed") and t.get("tat_mins", 0) > 0]
    avg_tat = round(sum(tat_vals) / len(tat_vals)) if tat_vals else 0

    summary = {
        "total":    total,
        "open":     open_c,
        "closed":   closed,
        "res_pct":  round(closed / total * 100, 1) if total else 0,
        "avg_tat":  fmt_tat(avg_tat),
    }

    # Open tickets detail
    now = datetime.utcnow()
    open_tickets = []
    for tid, t in tickets.items():
        if t.get("is_closed"):
            continue
        created_dt = parse_kapture_date(t.get("created", ""))
        age_mins = int((now - created_dt).total_seconds() / 60) if created_dt else 0
        open_tickets.append({
            "id":      tid,
            "agent":   t.get("agent", ""),
            "title":   t.get("title", ""),
            "created": t.get("created", ""),
            "date":    t.get("date", ""),
            "age":     fmt_tat(age_mins),
            "age_mins": age_mins,
        })
    open_tickets.sort(key=lambda x: x["age_mins"], reverse=True)

    return summary, agents, open_tickets

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=== Partner Trust Line Sync ===")

    # Load state (all processed ticket data)
    state = load_json(STATE_FILE, {"tickets": {}})
    tickets: dict[str, dict] = state.get("tickets", {})
    print(f"State: {len(tickets)} tickets loaded")

    # Fetch all ticket IDs from Google Sheet
    print("Fetching ticket IDs from sheet…")
    sheet_rows = fetch_csv(TICKETS_SHEET)
    all_ids = set()
    for row in sheet_rows:
        tid = row.get("ticket_ids", "").strip()
        if tid and tid.isdigit():
            all_ids.add(tid)
    print(f"Sheet has {len(all_ids)} ticket IDs")

    # Decide what to fetch: new + still-open + any missing new fields (one-time backfill)
    new_ids      = all_ids - set(tickets.keys())
    open_ids     = {tid for tid, t in tickets.items() if not t.get("is_closed")}
    backfill_ids = {tid for tid, t in tickets.items()
                    if "url" not in t or "calling_status" not in t}  # missing new fields
    to_fetch     = list(new_ids | open_ids | backfill_ids)
    print(f"To fetch: {len(to_fetch)} ({len(new_ids)} new, {len(open_ids)} open, {len(backfill_ids)} backfill)")

    # Batch fetch from Kapture (50 per batch)
    BATCH = 50
    fetched = 0
    for i in range(0, len(to_fetch), BATCH):
        batch = to_fetch[i : i + BATCH]
        print(f"  Batch {i//BATCH + 1}/{(len(to_fetch)-1)//BATCH + 1} ({len(batch)} tickets)…")
        results = fetch_kapture_batch(batch)
        for obj in results:
            t = extract_ticket(obj)
            if t:
                tickets[t["id"]] = t
                fetched += 1
    print(f"Fetched {fetched} tickets from Kapture")

    # Load PSAT and apply
    print("Loading PSAT…")
    psat_map = load_psat()
    for tid in tickets:
        if tid in psat_map:
            tickets[tid]["psat"]           = psat_map[tid]["psat"]
            tickets[tid]["calling_status"] = psat_map[tid]["calling_status"]
            tickets[tid]["call_dt"]        = psat_map[tid]["call_dt"]
    print(f"PSAT matched {len(psat_map)} tickets")

    # Aggregate
    summary, agents, open_tickets = aggregate(tickets)

    # Save state (compact, not served to browser directly)
    save_json(STATE_FILE, {"tickets": tickets})

    # Save data.json (served to browser)
    data = {
        "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "summary":      summary,
        "agents":       agents,
        "open_tickets": open_tickets,
    }
    save_json(DATA_FILE, data)

    print(f"Done. {summary['total']} tickets | {summary['open']} open | "
          f"{summary['closed']} closed | {len(agents)} agents")


if __name__ == "__main__":
    main()
