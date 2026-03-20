#!/usr/bin/env python3.13
"""
HQ Intake Billing Portal — Flask Web Application
Port 8090

Routes:
  /                  — Admin dashboard (Robert)
  /firm/<firm_id>    — Firm detail page
  /share/<token>     — Client share page (public, token-based)
  /api/refresh       — Re-run data collection
  /api/share/generate/<firm_id> — Generate share token
  /api/invoice/draft/<firm_id>  — Create FreshBooks invoice draft
  /api/invoice/send/<invoice_id> — Send FreshBooks invoice
  /api/leads/<firm_id>          — Get HubSpot leads for a firm
  /api/share/leads/<token>      — Get leads for client share page
  /api/export/all               — Download all firms as Excel
  /api/export/firm/<firm_id>    — Download firm leads as Excel
  /api/export/share/<token>     — Download client share leads as Excel
"""

import json
import io
import os
import subprocess
import sys
import uuid
import hashlib
import time
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Thread

from flask import (Flask, render_template, jsonify, request, redirect,
                   url_for, abort, send_file, Response)

# ── Paths ──
# When deployed to Railway, use relative paths from the app directory
DASHBOARD_DIR = Path(__file__).parent
BASE_DIR = DASHBOARD_DIR
DATA_FILE = DASHBOARD_DIR / "billing_data.json"
SHARE_TOKENS_FILE = DASHBOARD_DIR / "share_tokens.json"
DATA_QUALITY_FILE = DASHBOARD_DIR / "data_quality_log.json"
SALES_SNAPSHOT_TOKENS_FILE = DASHBOARD_DIR / "sales_snapshot_tokens.json"
CREDENTIALS_DIR = DASHBOARD_DIR / "credentials"
FRESHBOOKS_TOKENS_FILE = CREDENTIALS_DIR / "freshbooks_tokens.json"
ENV_FILE = DASHBOARD_DIR / ".env"

# ── Load .env ──
if ENV_FILE.exists():
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line and "PRIVATE_KEY" not in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

HUBSPOT_API_KEY = os.environ.get("HUBSPOT_API_KEY", "")
CONVOSO_TOKEN = "ku0i6z26w423j6p1xxn17fxejapfumv4"
FRESHBOOKS_ACCOUNT_ID = "wgZEN2"

# ── Flask App ──
app = Flask(
    __name__,
    template_folder=str(DASHBOARD_DIR / "templates"),
    static_folder=str(DASHBOARD_DIR / "static"),
    static_url_path="/static",
)
app.jinja_env.globals.update(max=max, min=min, abs=abs, int=int, round=round)


# ── Data Loading ──
def load_billing_data():
    """Load billing data from JSON file."""
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text())
        except Exception:
            return {"summary": {}, "firms": {}, "convoso_summary": {}}
    return {"summary": {}, "firms": {}, "convoso_summary": {}}


def load_share_tokens():
    """Load share tokens mapping."""
    if SHARE_TOKENS_FILE.exists():
        try:
            return json.loads(SHARE_TOKENS_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_share_tokens(tokens):
    """Save share tokens mapping."""
    SHARE_TOKENS_FILE.write_text(json.dumps(tokens, indent=2))


def load_data_quality():
    """Load data quality log."""
    if DATA_QUALITY_FILE.exists():
        try:
            return json.loads(DATA_QUALITY_FILE.read_text())
        except Exception:
            return {"issues": [], "last_check": None}
    return {"issues": [], "last_check": None}


def firm_id_from_name(name):
    """Generate a stable firm ID from name."""
    return hashlib.md5(name.encode()).hexdigest()[:12]


def get_firm_by_id(firm_id, data):
    """Find a firm by its ID."""
    for name, firm in data.get("firms", {}).items():
        if firm_id_from_name(name) == firm_id:
            return name, firm
    return None, None


def get_firm_by_token(token):
    """Find a firm by its share token."""
    tokens = load_share_tokens()
    # Token might be the key itself (hash) or the inner UUID
    if token in tokens:
        # The URL token IS the key — need to find the firm_id
        t = tokens[token]
        firm_name = t.get("firm_name", "") if isinstance(t, dict) else t
        return firm_id_from_name(firm_name)
    # Fallback: check inner token field
    for key, t in tokens.items():
        if isinstance(t, dict) and t.get("token") == token:
            firm_name = t.get("firm_name", "")
            return firm_id_from_name(firm_name)
    # Check vendor tokens
    vendor_tokens = load_vendor_tokens()
    if token in vendor_tokens:
        return token  # vendor tokens are self-contained
    return None


def load_vendor_tokens():
    """Load vendor share tokens (filtered by marketing source)."""
    vfile = DASHBOARD_DIR / "vendor_tokens.json"
    if vfile.exists():
        try:
            return json.loads(vfile.read_text())
        except Exception:
            return {}
    return {}


def save_vendor_tokens(tokens):
    """Save vendor share tokens."""
    vfile = DASHBOARD_DIR / "vendor_tokens.json"
    vfile.write_text(json.dumps(tokens, indent=2))


def hubspot_get_leads_by_marketing_source(source_filters):
    """Get leads from HubSpot filtered by marketing_source values."""
    import requests
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }
    all_leads = []
    for source in source_filters:
        after = None
        while True:
            body = {
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "marketing_source",
                        "operator": "CONTAINS_TOKEN",
                        "value": source
                    }]
                }],
                "properties": [
                    "firstname", "lastname", "email", "phone",
                    "hs_lead_status", "createdate", "marketing_source",
                    "case_type", "notes_last_updated", "rejection_reason",
                    "special_circumstances"
                ],
                "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
                "limit": 100
            }
            if after:
                body["after"] = after
            resp = requests.post(
                "https://api.hubapi.com/crm/v3/objects/contacts/search",
                headers=headers, json=body
            )
            if resp.status_code != 200:
                break
            data = resp.json()
            for c in data.get("results", []):
                props = c.get("properties", {})
                ms = (props.get("marketing_source") or "").lower()
                if any(sf.lower() in ms for sf in source_filters):
                    all_leads.append({
                        "name": f"{props.get('firstname', '')} {props.get('lastname', '')}".strip(),
                        "email": props.get("email", ""),
                        "phone": props.get("phone", ""),
                        "status": props.get("hs_lead_status", "Unknown"),
                        "date": props.get("createdate", "")[:10] if props.get("createdate") else "",
                        "lead_source": props.get("marketing_source", ""),
                        "case_type": props.get("case_type", ""),
                        "notes": props.get("special_circumstances", ""),
                        "rejection_reason": props.get("rejection_reason", "")
                    })
            paging = data.get("paging", {}).get("next", {})
            after = paging.get("after")
            if not after:
                break
    # Deduplicate by phone
    seen = set()
    unique = []
    for l in all_leads:
        key = l.get("phone", "") or l.get("email", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(l)
        elif not key:
            unique.append(l)
    return unique


def hubspot_get_leads_by_deal_name(name_filters):
    """Get leads from HubSpot by searching deals whose name contains the filter terms.
    Used for vendors like Wommster where leads are tracked in deal names, not marketing_source."""
    import requests
    import time
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }
    all_leads = []
    for name_filter in name_filters:
        after = None
        for _ in range(20):  # max 2000 deals per filter
            body = {
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "dealname",
                        "operator": "CONTAINS_TOKEN",
                        "value": name_filter
                    }]
                }],
                "properties": ["dealname", "dealstage", "createdate"],
                "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
                "limit": 100
            }
            if after:
                body["after"] = str(after)

            for attempt in range(3):
                resp = requests.post(
                    "https://api.hubapi.com/crm/v3/objects/deals/search",
                    headers=headers, json=body
                )
                if resp.status_code == 429:
                    time.sleep(int(resp.headers.get("Retry-After", 2)) + 1)
                    continue
                break

            if resp.status_code != 200:
                break
            data = resp.json()
            results = data.get("results", [])
            if not results:
                break

            for d in results:
                props = d.get("properties", {})
                dealname = props.get("dealname", "")
                stage_id = props.get("dealstage", "")
                stage_label = DEAL_STAGE_LABELS.get(stage_id, stage_id)

                # Parse contact name and case type from "Name / Case Type - Firm"
                contact_name = dealname.split("/")[0].strip() if "/" in dealname else dealname
                case_type = ""
                if "/" in dealname:
                    rest = dealname.split("/", 1)[1].strip()
                    case_type = rest.split("-")[0].strip() if "-" in rest else rest

                all_leads.append({
                    "name": contact_name,
                    "email": "",
                    "phone": "",
                    "status": stage_label,
                    "date": props.get("createdate", "")[:10] if props.get("createdate") else "",
                    "lead_source": name_filter,
                    "case_type": case_type,
                    "notes": "",
                    "rejection_reason": ""
                })

            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                break

    # Deduplicate by name + date
    seen = set()
    unique = []
    for l in all_leads:
        key = f"{l['name']}|{l['date']}"
        if key not in seen:
            seen.add(key)
            unique.append(l)
    return unique


def compute_aging(firms):
    """Compute outstanding aging buckets from invoice data."""
    aging = {"0_30": 0.0, "30_60": 0.0, "60_90": 0.0, "90_plus": 0.0}
    today = datetime.now().date()

    for name, firm in firms.items():
        invoices = firm.get("fb_invoices", [])
        for inv in invoices:
            outstanding = inv.get("outstanding", 0)
            if outstanding <= 0:
                continue
            inv_date_str = inv.get("date", "")
            if not inv_date_str:
                aging["90_plus"] += outstanding
                continue
            try:
                inv_date = datetime.strptime(inv_date_str, "%Y-%m-%d").date()
                days = (today - inv_date).days
                if days <= 30:
                    aging["0_30"] += outstanding
                elif days <= 60:
                    aging["30_60"] += outstanding
                elif days <= 90:
                    aging["60_90"] += outstanding
                else:
                    aging["90_plus"] += outstanding
            except Exception:
                aging["90_plus"] += outstanding

    return aging


def get_firm_health(firm):
    """Compute health status for a firm: green/yellow/red."""
    issues = 0
    if firm.get("fb_total_outstanding", 0) > 0:
        issues += 1
    if firm.get("retainer_alert"):
        issues += 2
    if firm.get("minutes_alert"):
        issues += 2
    rpct = firm.get("retainer_remaining_pct")
    if rpct is not None and rpct <= 30 and rpct > 20:
        issues += 1
    mpct = firm.get("minutes_remaining_pct")
    if mpct is not None and mpct <= 30 and mpct > 20:
        issues += 1

    if issues >= 3:
        return "red"
    elif issues >= 1:
        return "yellow"
    return "green"


# ── FreshBooks Helpers ──
def load_freshbooks_tokens():
    """Load FreshBooks OAuth tokens."""
    if FRESHBOOKS_TOKENS_FILE.exists():
        try:
            return json.loads(FRESHBOOKS_TOKENS_FILE.read_text())
        except Exception:
            return None
    return None


def refresh_freshbooks_token():
    """Refresh FreshBooks OAuth token if expired."""
    import requests
    tokens = load_freshbooks_tokens()
    if not tokens:
        return None

    client_id = os.environ.get("FRESHBOOKS_CLIENT_ID", "")
    client_secret = os.environ.get("FRESHBOOKS_CLIENT_SECRET", "")

    resp = requests.post("https://api.freshbooks.com/auth/oauth/token", json={
        "grant_type": "refresh_token",
        "refresh_token": tokens["refresh_token"],
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": "https://www.freshbooks.com"
    })

    if resp.status_code == 200:
        new_tokens = resp.json()
        new_tokens["created_at"] = datetime.now(timezone.utc).isoformat()
        FRESHBOOKS_TOKENS_FILE.write_text(json.dumps(new_tokens, indent=2))
        return new_tokens
    return tokens


def fb_headers():
    """Get FreshBooks API headers with valid token."""
    tokens = load_freshbooks_tokens()
    if not tokens:
        return None
    return {
        "Authorization": f"Bearer {tokens['access_token']}",
        "Content-Type": "application/json",
        "Api-Version": "alpha"
    }


def fb_get_client_id(firm_name):
    """Find FreshBooks client ID by firm name."""
    import requests
    headers = fb_headers()
    if not headers:
        return None

    resp = requests.get(
        f"https://api.freshbooks.com/accounting/account/{FRESHBOOKS_ACCOUNT_ID}/users/clients",
        headers=headers,
        params={"search[organization_like]": firm_name}
    )
    if resp.status_code == 200:
        clients = resp.json().get("response", {}).get("result", {}).get("clients", [])
        if clients:
            return clients[0].get("id")
    # Try refreshing token
    refresh_freshbooks_token()
    headers = fb_headers()
    if not headers:
        return None
    resp = requests.get(
        f"https://api.freshbooks.com/accounting/account/{FRESHBOOKS_ACCOUNT_ID}/users/clients",
        headers=headers,
        params={"search[organization_like]": firm_name}
    )
    if resp.status_code == 200:
        clients = resp.json().get("response", {}).get("result", {}).get("clients", [])
        if clients:
            return clients[0].get("id")
    return None


# ── Deal Stage Mapping ──
DEAL_STAGE_LABELS = {
    "3022527194": "Contacting",
    "qualifiedtobuy": "CB",
    "presentationscheduled": "Rejected",
    "decisionmakerboughtin": "Decision Maker Bought-In",
    "contractsent": "Contacted",
    "closedwon": "Signed",
    "closedlost": "Signed e-Sign",
    "3022527195": "Scheduled Appointment",
    "3022527196": "Signed e-Sign - Commercial",
    "3022527197": "Sent e-Sign",
    "3022527198": "Signed Commercial",
    "3022527199": "HQ Credit",
    "3022527201": "In Call",
    "3022527202": "Dropped",
    "3022527203": "Intake Under Review",
    "3022527204": "DNC",
    "3022527205": "DAIR",
    "3022527206": "Intake Questionnaire Emailed",
    "3022527207": "SC Credit",
    "3022527208": "Temporary Rejection",
    "3022527209": "Cancelled E-Sign",
    "appointmentscheduled": "Rejected",
    "3022527210": "Rejected - Retainer Not Returned",
}


def _get_deal_stages_by_name(firm_name, headers):
    """Search deals by firm name and return {contact_name_lower: {"stage": label, "date": YYYY-MM-DD}}.
    Uses the search API which has its own rate limit bucket. Fast and efficient."""
    import requests
    import time

    name_to_info = {}
    after = 0
    # Deal name format: "Contact Name / Case Type - Firm Name"
    search_token = firm_name.split()[0]

    for _ in range(20):  # max 2000 deals
        body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "dealname",
                    "operator": "CONTAINS_TOKEN",
                    "value": search_token
                }]
            }],
            "properties": ["dealname", "dealstage", "createdate"],
            "limit": 100,
        }
        if after:
            body["after"] = str(after)

        for attempt in range(3):
            resp = requests.post(
                "https://api.hubapi.com/crm/v3/objects/deals/search",
                headers=headers,
                json=body
            )
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 2)) + 1)
                continue
            break

        if resp.status_code != 200:
            break

        data = resp.json()
        results = data.get("results", [])
        if not results:
            break

        for d in results:
            props = d.get("properties", {})
            dealname = props.get("dealname", "")
            stage_id = props.get("dealstage", "")
            stage_label = DEAL_STAGE_LABELS.get(stage_id, stage_id)
            deal_date = props.get("createdate", "")[:10] if props.get("createdate") else ""

            # Parse contact name from "Name / Case Type - Firm"
            if "/" in dealname:
                contact_name = dealname.split("/")[0].strip().lower()
                name_to_info[contact_name] = {"stage": stage_label, "date": deal_date}

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return name_to_info


# ── HubSpot Helpers ──
def hubspot_get_leads_for_firm(firm_name):
    """Get leads from HubSpot by searching deals for the firm, then fetching contact info.
    This approach uses deals as the source of truth for status and dates."""
    import requests
    import time

    if not HUBSPOT_API_KEY:
        return []

    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    IMPORT_DATE = "2026-02-28"

    # Step 1: Get ALL deals for this firm (paginated)
    search_token = firm_name.split()[0]
    deals = []
    after = 0
    for _ in range(30):  # max 3000 deals
        body = {
            "filterGroups": [{"filters": [{
                "propertyName": "dealname",
                "operator": "CONTAINS_TOKEN",
                "value": search_token
            }]}],
            "properties": ["dealname", "dealstage", "createdate"],
            "limit": 100,
        }
        if after:
            body["after"] = str(after)

        for attempt in range(3):
            resp = requests.post(
                "https://api.hubapi.com/crm/v3/objects/deals/search",
                headers=headers, json=body
            )
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 2)) + 1)
                continue
            break

        if resp.status_code != 200:
            break

        data = resp.json()
        results = data.get("results", [])
        if not results:
            break
        deals.extend(results)
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    if not deals:
        return []

    # Step 2: Build lead list from deals, fetch contact info in batches
    # First collect deal_id -> {stage, date, contact_name_from_deal}
    deal_contact_ids = {}
    leads = []

    # Get contact associations for deals in batches
    deal_ids = [d["id"] for d in deals]
    deal_props = {}
    for d in deals:
        props = d.get("properties", {})
        deal_props[d["id"]] = {
            "dealname": props.get("dealname", ""),
            "stage": DEAL_STAGE_LABELS.get(props.get("dealstage", ""), props.get("dealstage", "")),
            "date": props.get("createdate", "")[:10] if props.get("createdate") else "",
        }

    # Batch get associations (deal -> contacts)
    for batch_start in range(0, len(deal_ids), 25):
        batch = deal_ids[batch_start:batch_start + 25]
        assoc_resp = requests.post(
            "https://api.hubapi.com/crm/v4/associations/deals/contacts/batch/read",
            headers=headers,
            json={"inputs": [{"id": did} for did in batch]}
        )
        if assoc_resp.status_code == 200:
            for item in assoc_resp.json().get("results", []):
                did = item.get("from", {}).get("id", "")
                for to in item.get("to", []):
                    cid = to.get("toObjectId", "")
                    if cid and did:
                        deal_contact_ids.setdefault(did, []).append(str(cid))
        time.sleep(0.1)  # Rate limit

    # Collect unique contact IDs
    all_cids = set()
    for cids in deal_contact_ids.values():
        all_cids.update(cids)

    # Batch read contacts
    contact_data = {}
    cid_list = list(all_cids)
    for batch_start in range(0, len(cid_list), 50):
        batch = cid_list[batch_start:batch_start + 50]
        cresp = requests.post(
            "https://api.hubapi.com/crm/v3/objects/contacts/batch/read",
            headers=headers,
            json={
                "inputs": [{"id": cid} for cid in batch],
                "properties": [
                    "firstname", "lastname", "email", "phone",
                    "hs_lead_status", "createdate", "lead_source",
                    "notes_last_updated", "rejection_reason"
                ]
            }
        )
        if cresp.status_code == 200:
            for c in cresp.json().get("results", []):
                contact_data[c["id"]] = c.get("properties", {})
        time.sleep(0.1)

    # Step 2b: For deals with no contact association, try to find contacts by name
    unlinked_deal_ids = [did for did in deal_props if did not in deal_contact_ids]
    if unlinked_deal_ids:
        names_to_search = {}
        for did in unlinked_deal_ids:
            dealname = deal_props[did]["dealname"]
            contact_name = dealname.split("/")[0].strip() if "/" in dealname else dealname
            parts = contact_name.split()
            if len(parts) >= 2:
                names_to_search[did] = {"first": parts[0], "last": " ".join(parts[1:])}

        # Batch search by last name, then match first name
        searched_lastnames = {}
        for did, nm in names_to_search.items():
            ln = nm["last"]
            if ln in searched_lastnames:
                continue
            try:
                sresp = requests.post(
                    "https://api.hubapi.com/crm/v3/objects/contacts/search",
                    headers=headers,
                    json={
                        "filterGroups": [{"filters": [{"propertyName": "lastname", "operator": "EQ", "value": ln}]}],
                        "properties": ["firstname", "lastname", "email", "phone", "lead_source", "rejection_reason"],
                        "limit": 100
                    }
                )
                if sresp.status_code == 200:
                    searched_lastnames[ln] = sresp.json().get("results", [])
                else:
                    searched_lastnames[ln] = []
                time.sleep(0.11)
            except Exception:
                searched_lastnames[ln] = []

        # Match and populate contact_data + deal_contact_ids
        for did, nm in names_to_search.items():
            ln = nm["last"]
            fn_lower = nm["first"].lower()
            for c in searched_lastnames.get(ln, []):
                cp = c.get("properties", {})
                if (cp.get("firstname", "") or "").lower() == fn_lower:
                    cid = c["id"]
                    contact_data[cid] = cp
                    deal_contact_ids.setdefault(did, []).append(cid)
                    break

    # Step 3: Build final lead list from deals + contact info
    seen = set()  # Deduplicate by contact ID
    for did, dp in deal_props.items():
        cids = deal_contact_ids.get(did, [])
        if not cids:
            # No contact linked and no match found — use deal name
            dealname = dp["dealname"]
            if "/" in dealname:
                contact_name = dealname.split("/")[0].strip()
            else:
                contact_name = dealname
            deal_date = dp["date"]
            notes_date = ""
            lead_date = deal_date if deal_date != IMPORT_DATE else deal_date
            leads.append({
                "name": contact_name,
                "email": "",
                "phone": "",
                "status": dp["stage"],
                "date": lead_date,
                "lead_source": "",
                "notes": "",
                "rejection_reason": ""
            })
            continue

        for cid in cids:
            if cid in seen:
                continue
            seen.add(cid)
            props = contact_data.get(cid, {})
            contact_name = f"{props.get('firstname', '')} {props.get('lastname', '')}".strip()
            deal_date = dp["date"]
            notes_date = props.get("notes_last_updated", "")[:10] if props.get("notes_last_updated") else ""
            contact_date = props.get("createdate", "")[:10] if props.get("createdate") else ""

            # Date logic: prefer deal date when not import date, then notes, then contact date
            if deal_date and deal_date != IMPORT_DATE:
                lead_date = deal_date
            elif notes_date:
                lead_date = notes_date
            else:
                lead_date = deal_date or contact_date

            leads.append({
                "name": contact_name or dp["dealname"].split("/")[0].strip(),
                "email": props.get("email", ""),
                "phone": props.get("phone", ""),
                "status": dp["stage"],
                "date": lead_date,
                "lead_source": props.get("lead_source", ""),
                "notes": props.get("notes_last_updated", ""),
                "rejection_reason": props.get("rejection_reason", "")
            })

    return leads


def hubspot_get_leads_from_deals(firm_name, headers):
    """Get leads via deal search when company search fails."""
    import requests

    resp = requests.post(
        "https://api.hubapi.com/crm/v3/objects/deals/search",
        headers=headers,
        json={
            "filterGroups": [{
                "filters": [{
                    "propertyName": "dealname",
                    "operator": "CONTAINS_TOKEN",
                    "value": firm_name.split()[0]
                }]
            }],
            "properties": ["dealname", "dealstage", "createdate", "hs_lastmodifieddate", "amount"],
            "limit": 100
        }
    )

    leads = []
    if resp.status_code == 200:
        deals = resp.json().get("results", [])
        for deal in deals:
            aresp = requests.get(
                f"https://api.hubapi.com/crm/v3/objects/deals/{deal['id']}/associations/contacts",
                headers=headers
            )
            if aresp.status_code == 200:
                contact_ids = [r["id"] for r in aresp.json().get("results", [])]
                for cid in contact_ids[:5]:
                    cresp = requests.get(
                        f"https://api.hubapi.com/crm/v3/objects/contacts/{cid}",
                        headers=headers,
                        params={
                            "properties": "firstname,lastname,email,phone,hs_lead_status,createdate,lastmodifieddate,lead_source,rejection_reason"
                        }
                    )
                    if cresp.status_code == 200:
                        props = cresp.json().get("properties", {})
                        activity_date = props.get("lastmodifieddate", "")[:10] if props.get("lastmodifieddate") else ""
                        create_date = props.get("createdate", "")[:10] if props.get("createdate") else ""
                        lead_date = activity_date or create_date
                        leads.append({
                            "name": f"{props.get('firstname', '')} {props.get('lastname', '')}".strip(),
                            "email": props.get("email", ""),
                            "phone": props.get("phone", ""),
                            "status": props.get("hs_lead_status", "Unknown"),
                            "date": lead_date,
                            "lead_source": props.get("lead_source", ""),
                            "notes": "",
                            "rejection_reason": props.get("rejection_reason", "")
                        })

    return leads


# ── Excel Export Helpers ──
def build_all_firms_excel(data):
    """Build Excel workbook with all firms summary data."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "All Firms"

    # Header styling
    header_font = Font(bold=True, color="FFFFFF", size=11)
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    currency_fmt = '#,##0.00'
    pct_fmt = '0.0"%"'
    thin_border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin")
    )

    headers = [
        "Firm Name", "Status", "Retainers Pre-Paid", "Retainer %",
        "Minutes Pre-Paid", "Minutes Used", "Minutes Remaining", "Minutes %",
        "FB Invoiced", "FB Paid", "FB Outstanding",
        "Convoso Calls (Today)", "Convoso Minutes (Today)",
        "HubSpot Signups", "Sheet Signups", "Source"
    ]

    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align
        cell.border = thin_border

    firms = data.get("firms", {})
    convoso = data.get("convoso_summary", {}).get("firms", {})

    row = 2
    for name in sorted(firms.keys(), key=lambda n: firms[n].get("fb_total_outstanding", 0), reverse=True):
        firm = firms[name]
        health = get_firm_health(firm)
        cv = convoso.get(name, {})

        mins_prepaid = firm.get("fb_prepaid_minutes", 0) or 0
        mins_used = firm.get("fb_minutes_used", 0) or 0
        mins_remaining = max(0, mins_prepaid - mins_used)

        values = [
            name,
            health.upper(),
            firm.get("retainers_prepaid", 0),
            firm.get("retainer_remaining_pct"),
            mins_prepaid,
            mins_used,
            mins_remaining,
            firm.get("minutes_remaining_pct"),
            firm.get("fb_total_invoiced", 0),
            firm.get("fb_total_paid", 0),
            firm.get("fb_total_outstanding", 0),
            cv.get("total_calls", 0),
            round(cv.get("total_minutes", 0), 1),
            firm.get("hubspot_signups_since_dec20", 0),
            firm.get("total_signups", 0),
            firm.get("source", "")
        ]

        for col_idx, val in enumerate(values, 1):
            cell = ws.cell(row=row, column=col_idx, value=val if val is not None else "")
            cell.border = thin_border

        # Format currency columns
        for col_idx in [9, 10, 11]:
            ws.cell(row=row, column=col_idx).number_format = currency_fmt

        # Color-code status
        status_cell = ws.cell(row=row, column=2)
        if health == "red":
            status_cell.font = Font(color="FF0000", bold=True)
        elif health == "yellow":
            status_cell.font = Font(color="CC8800", bold=True)
        else:
            status_cell.font = Font(color="228B22", bold=True)

        row += 1

    # Auto-width columns
    for col_idx in range(1, len(headers) + 1):
        max_len = len(str(headers[col_idx - 1]))
        for r in range(2, row):
            val = ws.cell(row=r, column=col_idx).value
            if val is not None:
                max_len = max(max_len, len(str(val)))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 3, 40)

    # Add autofilter
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{row - 1}"

    # Summary sheet
    ws2 = wb.create_sheet("Summary")
    summary = data.get("summary", {})
    summary_rows = [
        ("Report Generated", data.get("generated_at", "")[:19]),
        ("Data Range", f"{data.get('data_range', {}).get('start', '')} to {data.get('data_range', {}).get('end', '')}"),
        ("", ""),
        ("Total Firms", summary.get("total_firms", 0)),
        ("FreshBooks Invoiced", summary.get("total_fb_invoiced", 0)),
        ("FreshBooks Paid", summary.get("total_fb_paid", 0)),
        ("FreshBooks Outstanding", summary.get("total_fb_outstanding", 0)),
        ("HubSpot Signups (Dec 20+)", summary.get("hubspot_signups_since_dec20", 0)),
        ("Firms with Alerts", summary.get("firms_with_alerts", 0)),
        ("Convoso Calls Today", summary.get("convoso_total_calls_today", 0)),
        ("Convoso Minutes Today", summary.get("convoso_total_minutes_today", 0)),
    ]
    for r_idx, (label, val) in enumerate(summary_rows, 1):
        ws2.cell(row=r_idx, column=1, value=label).font = Font(bold=True)
        cell = ws2.cell(row=r_idx, column=2, value=val)
        if isinstance(val, float) and val > 100:
            cell.number_format = currency_fmt

    ws2.column_dimensions["A"].width = 30
    ws2.column_dimensions["B"].width = 25

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output


def build_firm_leads_excel(firm_name, leads, include_rejection=True):
    """Build Excel workbook with firm leads."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Leads"

    header_font = Font(bold=True, color="FFFFFF", size=11)
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_align = Alignment(horizontal="center", vertical="center")
    thin_border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin")
    )

    # Title row
    ws.merge_cells("A1:G1")
    title_cell = ws.cell(row=1, column=1, value=f"{firm_name} - Lead Report")
    title_cell.font = Font(bold=True, size=14)
    ws.cell(row=2, column=1, value=f"Generated: {datetime.now().strftime('%B %d, %Y %I:%M %p')}")
    ws.cell(row=2, column=1).font = Font(italic=True, color="666666")

    headers = ["Name", "Email", "Phone", "Status", "Lead Source", "Date"]
    if include_rejection:
        headers.append("Rejection Reason")
    headers.append("Notes")

    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=4, column=col_idx, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align
        cell.border = thin_border

    for r_idx, lead in enumerate(leads, 5):
        values = [
            lead.get("name", ""),
            lead.get("email", ""),
            lead.get("phone", ""),
            lead.get("status", ""),
            lead.get("lead_source", ""),
            lead.get("date", ""),
        ]
        if include_rejection:
            values.append(lead.get("rejection_reason", ""))
        values.append(lead.get("notes", ""))

        for col_idx, val in enumerate(values, 1):
            cell = ws.cell(row=r_idx, column=col_idx, value=val or "")
            cell.border = thin_border

        # Color-code status
        status_cell = ws.cell(row=r_idx, column=4)
        st = (lead.get("status") or "").lower()
        if "signed" in st or "won" in st:
            status_cell.font = Font(color="228B22", bold=True)
        elif "reject" in st or "lost" in st:
            status_cell.font = Font(color="FF0000")
        elif "pending" in st or "new" in st:
            status_cell.font = Font(color="CC8800")

    last_row = 4 + len(leads)
    for col_idx in range(1, len(headers) + 1):
        max_len = len(str(headers[col_idx - 1]))
        for r in range(5, last_row + 1):
            val = ws.cell(row=r, column=col_idx).value
            if val:
                max_len = max(max_len, len(str(val)))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 3, 45)

    ws.auto_filter.ref = f"A4:{get_column_letter(len(headers))}{last_row}"

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output


def build_client_leads_excel(firm_name, leads):
    """Build client-safe Excel (no billing, no admin data)."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Lead Report"

    header_font = Font(bold=True, color="FFFFFF", size=11)
    header_fill = PatternFill(start_color="2E75B6", end_color="2E75B6", fill_type="solid")
    thin_border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin")
    )

    ws.merge_cells("A1:F1")
    title_cell = ws.cell(row=1, column=1, value=f"{firm_name} - Lead Report")
    title_cell.font = Font(bold=True, size=14)
    ws.cell(row=2, column=1, value=f"Report Date: {datetime.now().strftime('%B %d, %Y')}")
    ws.cell(row=2, column=1).font = Font(italic=True, color="666666")

    # Summary row
    total = len(leads)
    signed = sum(1 for l in leads if "signed" in (l.get("status") or "").lower() or "won" in (l.get("status") or "").lower())
    pending = sum(1 for l in leads if "pending" in (l.get("status") or "").lower() or "new" in (l.get("status") or "").lower())
    rejected = sum(1 for l in leads if "reject" in (l.get("status") or "").lower() or "lost" in (l.get("status") or "").lower())

    ws.cell(row=3, column=1, value=f"Total: {total}  |  Signed: {signed}  |  Pending: {pending}  |  Rejected: {rejected}")
    ws.cell(row=3, column=1).font = Font(bold=True, size=11)

    headers = ["Name", "Phone", "Email", "Status", "Lead Source", "Date"]
    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=5, column=col_idx, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")
        cell.border = thin_border

    for r_idx, lead in enumerate(leads, 6):
        values = [
            lead.get("name", ""),
            lead.get("phone", ""),
            lead.get("email", ""),
            lead.get("status", ""),
            lead.get("lead_source", ""),
            lead.get("date", ""),
        ]
        for col_idx, val in enumerate(values, 1):
            cell = ws.cell(row=r_idx, column=col_idx, value=val or "")
            cell.border = thin_border

        st = (lead.get("status") or "").lower()
        status_cell = ws.cell(row=r_idx, column=4)
        if "signed" in st or "won" in st:
            status_cell.font = Font(color="228B22", bold=True)
        elif "reject" in st or "lost" in st:
            status_cell.font = Font(color="FF0000")

    last_row = 5 + len(leads)
    for col_idx in range(1, len(headers) + 1):
        max_len = len(str(headers[col_idx - 1]))
        for r in range(6, last_row + 1):
            val = ws.cell(row=r, column=col_idx).value
            if val:
                max_len = max(max_len, len(str(val)))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 3, 40)

    ws.auto_filter.ref = f"A5:{get_column_letter(len(headers))}{last_row}"

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output


# ── Routes ──

@app.route("/")
def dashboard():
    """Admin dashboard — main billing view."""
    data = load_billing_data()
    summary = data.get("summary", {})
    firms = data.get("firms", {})
    convoso = data.get("convoso_summary", {})
    tokens = load_share_tokens()

    # Compute aging
    aging = compute_aging(firms)

    # Build firms list with IDs and health
    firms_list = []
    for name, firm in sorted(firms.items(), key=lambda x: x[1].get("fb_total_outstanding", 0), reverse=True):
        fid = firm_id_from_name(name)
        firm["_id"] = fid
        firm["_name"] = name
        firm["_has_share"] = fid in tokens
        firm["_share_token"] = tokens.get(fid, {}).get("token", "")
        firm["_health"] = get_firm_health(firm)
        firms_list.append(firm)

    # Alerts: below 20% on retainers or minutes
    alert_firms = [f for f in firms_list if f.get("retainer_alert") or f.get("minutes_alert")]

    # Overdue firms (outstanding > 30 days)
    overdue_firms = []
    today = datetime.now().date()
    for f in firms_list:
        if f.get("fb_total_outstanding", 0) > 0:
            invoices = f.get("fb_invoices", [])
            for inv in invoices:
                if inv.get("outstanding", 0) > 0:
                    try:
                        inv_date = datetime.strptime(inv.get("date", ""), "%Y-%m-%d").date()
                        if (today - inv_date).days > 30:
                            overdue_firms.append(f)
                            break
                    except Exception:
                        pass

    # Top outstanding
    top_outstanding = sorted(
        [f for f in firms_list if f.get("fb_total_outstanding", 0) > 0],
        key=lambda x: x.get("fb_total_outstanding", 0), reverse=True
    )[:10]

    # Top signups
    top_signups = sorted(
        [f for f in firms_list if f.get("hubspot_signups_since_dec20", 0) > 0],
        key=lambda x: x.get("hubspot_signups_since_dec20", 0), reverse=True
    )[:10]

    # Data quality issues
    dq = load_data_quality()

    return render_template("dashboard.html",
        summary=summary,
        firms=firms_list,
        alert_firms=alert_firms,
        overdue_firms=overdue_firms,
        top_outstanding=top_outstanding,
        top_signups=top_signups,
        convoso=convoso,
        aging=aging,
        data_quality=dq,
        generated_at=data.get("generated_at", ""),
        data_range=data.get("data_range", {})
    )


@app.route("/firm/<firm_id>")
def firm_detail(firm_id):
    """Firm detail page — full report for one firm."""
    data = load_billing_data()
    name, firm = get_firm_by_id(firm_id, data)
    if not firm:
        abort(404)

    tokens = load_share_tokens()
    share_token = tokens.get(firm_id, {}).get("token", "")
    convoso = data.get("convoso_summary", {}).get("firms", {}).get(name, {})
    health = get_firm_health(firm)

    return render_template("firm_detail.html",
        firm=firm,
        firm_name=name,
        firm_id=firm_id,
        share_token=share_token,
        convoso=convoso,
        hubspot_by_stage=firm.get("hubspot_by_stage", {}),
        invoices=firm.get("fb_invoices", []),
        health=health,
        host=request.host
    )


@app.route("/share/<token>")
def client_share(token):
    """Client share page — token-based, client sees only their leads."""
    # Check vendor tokens first
    vendor_tokens = load_vendor_tokens()
    if token in vendor_tokens:
        vt = vendor_tokens[token]
        return render_template("client_share.html",
            firm_name=vt["vendor_name"],
            firm_id=token,
            token=token,
            firm={"signups": 0}
        )

    firm_id = get_firm_by_token(token)
    if not firm_id:
        abort(404)

    data = load_billing_data()
    name, firm = get_firm_by_id(firm_id, data)
    if not firm:
        abort(404)

    return render_template("client_share.html",
        firm_name=name,
        firm_id=firm_id,
        token=token,
        firm=firm
    )


# ── API Endpoints ──

@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Re-run data collection script."""
    script = BASE_DIR / "scripts" / "billing_dashboard_data.py"
    if not script.exists():
        return jsonify({"error": "Data script not found"}), 404

    def run_refresh():
        subprocess.run([sys.executable, str(script)], cwd=str(BASE_DIR))

    thread = Thread(target=run_refresh)
    thread.start()
    return jsonify({"status": "refreshing", "message": "Data refresh started in background"})


@app.route("/api/leads/<firm_id>")
def api_leads(firm_id):
    """Get HubSpot leads for a firm (called via AJAX)."""
    data = load_billing_data()
    name, firm = get_firm_by_id(firm_id, data)
    if not firm:
        return jsonify({"error": "Firm not found"}), 404

    try:
        leads = hubspot_get_leads_for_firm(name)
        return jsonify({"firm": name, "leads": leads, "count": len(leads)})
    except Exception as e:
        return jsonify({"error": str(e), "leads": [], "count": 0}), 500


@app.route("/api/share/leads/<token>")
def api_share_leads(token):
    """Get leads for a share page (client-facing, limited data)."""
    # Check vendor tokens first
    vendor_tokens = load_vendor_tokens()
    if token in vendor_tokens:
        vt = vendor_tokens[token]
        try:
            search_type = vt.get("search_type", "marketing_source")
            if search_type == "deal_name":
                leads = hubspot_get_leads_by_deal_name(vt["filters"])
            else:
                leads = hubspot_get_leads_by_marketing_source(vt["filters"])
            safe_leads = []
            for lead in leads:
                safe_leads.append({
                    "name": lead.get("name", ""),
                    "email": lead.get("email", ""),
                    "phone": lead.get("phone", ""),
                    "status": lead.get("status", "Unknown"),
                    "date": lead.get("date", ""),
                    "lead_source": lead.get("lead_source", ""),
                    "notes": lead.get("notes", ""),
                    "rejection_reason": lead.get("rejection_reason", "")
                })
            return jsonify({"firm": vt["vendor_name"], "leads": safe_leads, "count": len(safe_leads)})
        except Exception as e:
            return jsonify({"error": str(e), "leads": [], "count": 0}), 500

    firm_id = get_firm_by_token(token)
    if not firm_id:
        return jsonify({"error": "Invalid token"}), 404

    data = load_billing_data()
    name, firm = get_firm_by_id(firm_id, data)
    if not firm:
        return jsonify({"error": "Firm not found"}), 404

    try:
        leads = hubspot_get_leads_for_firm(name)
        safe_leads = []
        for lead in leads:
            safe_leads.append({
                "name": lead.get("name", ""),
                "email": lead.get("email", ""),
                "phone": lead.get("phone", ""),
                "status": lead.get("status", "Unknown"),
                "date": lead.get("date", ""),
                "lead_source": lead.get("lead_source", "")
            })
        return jsonify({"firm": name, "leads": safe_leads, "count": len(safe_leads)})
    except Exception as e:
        return jsonify({"error": str(e), "leads": [], "count": 0}), 500


@app.route("/api/share/generate/<firm_id>", methods=["POST"])
def api_generate_share(firm_id):
    """Generate or regenerate a share token for a firm."""
    data = load_billing_data()
    name, firm = get_firm_by_id(firm_id, data)
    if not firm:
        return jsonify({"error": "Firm not found"}), 404

    tokens = load_share_tokens()
    token = uuid.uuid4().hex[:16]
    tokens[firm_id] = {
        "token": token,
        "firm_name": name,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    save_share_tokens(tokens)

    share_url = f"{request.scheme}://{request.host}/share/{token}"
    return jsonify({"token": token, "url": share_url, "firm": name})


@app.route("/api/share/generate-all", methods=["POST"])
def api_generate_all_shares():
    """Generate share tokens for all active firms."""
    data = load_billing_data()
    tokens = load_share_tokens()
    generated = 0

    for name, firm in data.get("firms", {}).items():
        fid = firm_id_from_name(name)
        if fid not in tokens:
            if firm.get("fb_total_invoiced", 0) > 0 or firm.get("hubspot_signups_since_dec20", 0) > 0:
                token = uuid.uuid4().hex[:16]
                tokens[fid] = {
                    "token": token,
                    "firm_name": name,
                    "created_at": datetime.now(timezone.utc).isoformat()
                }
                generated += 1

    save_share_tokens(tokens)
    return jsonify({"status": "ok", "generated": generated, "total": len(tokens)})


# ── Excel Export Endpoints ──

@app.route("/api/export/all")
def api_export_all():
    """Download all firms as Excel."""
    data = load_billing_data()
    output = build_all_firms_excel(data)
    filename = f"HQ_Intake_All_Firms_{datetime.now().strftime('%Y%m%d')}.xlsx"
    return send_file(output, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True, download_name=filename)


@app.route("/api/export/firm/<firm_id>")
def api_export_firm(firm_id):
    """Download firm leads as Excel."""
    data = load_billing_data()
    name, firm = get_firm_by_id(firm_id, data)
    if not firm:
        abort(404)

    try:
        leads = hubspot_get_leads_for_firm(name)
    except Exception:
        leads = []

    output = build_firm_leads_excel(name, leads)
    safe_name = re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_')[:30]
    filename = f"{safe_name}_Leads_{datetime.now().strftime('%Y%m%d')}.xlsx"
    return send_file(output, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True, download_name=filename)


@app.route("/api/export/share/<token>")
def api_export_share(token):
    """Download client-safe leads Excel."""
    firm_id = get_firm_by_token(token)
    if not firm_id:
        abort(404)

    data = load_billing_data()
    name, firm = get_firm_by_id(firm_id, data)
    if not firm:
        abort(404)

    try:
        leads = hubspot_get_leads_for_firm(name)
        safe_leads = [{
            "name": l.get("name", ""),
            "email": l.get("email", ""),
            "phone": l.get("phone", ""),
            "status": l.get("status", ""),
            "lead_source": l.get("lead_source", ""),
            "date": l.get("date", ""),
        } for l in leads]
    except Exception:
        safe_leads = []

    output = build_client_leads_excel(name, safe_leads)
    safe_name = re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_')[:30]
    filename = f"{safe_name}_Lead_Report_{datetime.now().strftime('%Y%m%d')}.xlsx"
    return send_file(output, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True, download_name=filename)


# ── Invoice Endpoints ──

@app.route("/api/invoice/draft/<firm_id>", methods=["POST"])
def api_invoice_draft(firm_id):
    """Create a FreshBooks invoice draft for a firm."""
    import requests as req

    data = load_billing_data()
    name, firm = get_firm_by_id(firm_id, data)
    if not firm:
        return jsonify({"error": "Firm not found"}), 404

    fb_client_id = fb_get_client_id(name)
    if not fb_client_id:
        return jsonify({"error": f"No FreshBooks client found for '{name}'"}), 404

    lines = []

    minutes_used = firm.get("fb_minutes_used", 0)
    prepaid = firm.get("fb_prepaid_minutes", 0)
    if minutes_used > 0 and prepaid > 0:
        overage = max(0, minutes_used - prepaid)
        if overage > 0:
            lines.append({
                "name": "Minute Overage",
                "description": f"Overage minutes: {overage} mins beyond {prepaid} prepaid",
                "unit_cost": {"amount": "1.50", "code": "USD"},
                "qty": overage
            })

    retainer_pct = firm.get("retainer_remaining_pct")
    if retainer_pct is not None and retainer_pct <= 20:
        signups = firm.get("hubspot_signups_since_dec20", 0) or firm.get("total_signups", 0)
        lines.append({
            "name": "Retainer Renewal -- Signed Cases",
            "description": f"Signed cases for {name}",
            "unit_cost": {"amount": "340.00", "code": "USD"},
            "qty": max(1, signups // 10)
        })

    minutes_pct = firm.get("minutes_remaining_pct")
    if minutes_pct is not None and minutes_pct <= 20:
        lines.append({
            "name": "Prepaid Minutes Renewal -- 2,500 Minutes",
            "description": f"Autodialer minute package for {name}",
            "unit_cost": {"amount": "2694.00", "code": "USD"},
            "qty": 1
        })

    if not lines:
        lines.append({
            "name": "Monthly Service Fee",
            "description": f"HQ Intake services for {name}",
            "unit_cost": {"amount": "2694.00", "code": "USD"},
            "qty": 1
        })

    headers = fb_headers()
    if not headers:
        refresh_freshbooks_token()
        headers = fb_headers()
        if not headers:
            return jsonify({"error": "FreshBooks authentication failed"}), 401

    invoice_data = {
        "invoice": {
            "customerid": fb_client_id,
            "create_date": datetime.now().strftime("%Y-%m-%d"),
            "lines": lines,
            "notes": f"Auto-generated invoice for {name} -- HQ Intake Billing Portal",
            "status": 1
        }
    }

    resp = req.post(
        f"https://api.freshbooks.com/accounting/account/{FRESHBOOKS_ACCOUNT_ID}/invoices/invoices",
        headers=headers,
        json=invoice_data
    )

    if resp.status_code in (200, 201):
        inv = resp.json().get("response", {}).get("result", {}).get("invoice", {})
        inv_id = inv.get("id") or inv.get("invoiceid")
        return jsonify({
            "status": "created",
            "invoice_id": inv_id,
            "amount": sum(float(l["unit_cost"]["amount"]) * l["qty"] for l in lines),
            "view_url": f"https://my.freshbooks.com/#/invoice/{inv_id}",
            "firm": name
        })
    else:
        refresh_freshbooks_token()
        headers = fb_headers()
        resp = req.post(
            f"https://api.freshbooks.com/accounting/account/{FRESHBOOKS_ACCOUNT_ID}/invoices/invoices",
            headers=headers,
            json=invoice_data
        )
        if resp.status_code in (200, 201):
            inv = resp.json().get("response", {}).get("result", {}).get("invoice", {})
            inv_id = inv.get("id") or inv.get("invoiceid")
            return jsonify({
                "status": "created",
                "invoice_id": inv_id,
                "amount": sum(float(l["unit_cost"]["amount"]) * l["qty"] for l in lines),
                "view_url": f"https://my.freshbooks.com/#/invoice/{inv_id}",
                "firm": name
            })
        return jsonify({"error": f"FreshBooks API error: {resp.status_code}", "detail": resp.text}), 500


@app.route("/api/invoice/send/<invoice_id>", methods=["POST"])
def api_invoice_send(invoice_id):
    """Send a FreshBooks invoice."""
    import requests as req

    headers = fb_headers()
    if not headers:
        refresh_freshbooks_token()
        headers = fb_headers()
        if not headers:
            return jsonify({"error": "FreshBooks authentication failed"}), 401

    resp = req.put(
        f"https://api.freshbooks.com/accounting/account/{FRESHBOOKS_ACCOUNT_ID}/invoices/invoices/{invoice_id}",
        headers=headers,
        json={"invoice": {"action_email": True, "status": 2}}
    )

    if resp.status_code == 200:
        return jsonify({"status": "sent", "invoice_id": invoice_id})
    else:
        refresh_freshbooks_token()
        headers = fb_headers()
        resp = req.put(
            f"https://api.freshbooks.com/accounting/account/{FRESHBOOKS_ACCOUNT_ID}/invoices/invoices/{invoice_id}",
            headers=headers,
            json={"invoice": {"action_email": True, "status": 2}}
        )
        if resp.status_code == 200:
            return jsonify({"status": "sent", "invoice_id": invoice_id})
        return jsonify({"error": f"Failed to send: {resp.status_code}", "detail": resp.text}), 500


# ── Sales Snapshot (Multi-Client) ──

def load_sales_snapshot_tokens():
    """Load sales snapshot tokens mapping."""
    if SALES_SNAPSHOT_TOKENS_FILE.exists():
        try:
            return json.loads(SALES_SNAPSHOT_TOKENS_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_sales_snapshot_tokens(tokens):
    """Save sales snapshot tokens mapping."""
    SALES_SNAPSHOT_TOKENS_FILE.write_text(json.dumps(tokens, indent=2))


@app.route("/api/sales-snapshot/generate", methods=["POST"])
def api_generate_sales_snapshot():
    """Generate a sales snapshot token for multiple firms.

    POST JSON body: {"firm_names": ["KP Injury Law", "The Law Office of Daniel A. Brown", ...]}
    OR: {"firm_ids": ["abc123", "def456", ...]}
    """
    payload = request.get_json(force=True) or {}
    data = load_billing_data()
    firms_data = data.get("firms", {})

    firm_names = payload.get("firm_names", [])
    firm_ids = payload.get("firm_ids", [])

    # Resolve firm_ids to firm_names if provided
    if firm_ids and not firm_names:
        for fid in firm_ids:
            name, firm = get_firm_by_id(fid, data)
            if name:
                firm_names.append(name)

    # Validate all firm names exist
    valid_names = []
    for name in firm_names:
        if name in firms_data:
            valid_names.append(name)
        else:
            # Try fuzzy match (case-insensitive contains)
            for fn in firms_data:
                if name.lower() in fn.lower() or fn.lower() in name.lower():
                    valid_names.append(fn)
                    break

    if not valid_names:
        return jsonify({"error": "No valid firms found", "provided": firm_names}), 400

    token = uuid.uuid4().hex[:20]
    tokens = load_sales_snapshot_tokens()
    tokens[token] = {
        "firm_names": valid_names,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    save_sales_snapshot_tokens(tokens)

    share_url = f"{request.scheme}://{request.host}/sales-snapshot/{token}"
    return jsonify({"token": token, "url": share_url, "firms": valid_names})


@app.route("/sales-snapshot/<token>")
def sales_snapshot(token):
    """Sales snapshot page -- multi-client overview, token-based, no login required."""
    tokens = load_sales_snapshot_tokens()
    if token not in tokens:
        abort(404)

    token_data = tokens[token]
    firm_names = token_data.get("firm_names", [])

    data = load_billing_data()
    firms_data = data.get("firms", {})

    # Build per-firm context for the template
    firms_context = []
    total_paid_legal_leadz = 0.0
    total_paid_hq_intake = 0.0

    for name in firm_names:
        firm = firms_data.get(name, {})
        # FreshBooks total = Legal Leadz AI payments
        paid_ll = firm.get("fb_total_paid", 0.0) or 0.0
        # Sheets total = HQ Intake payments (from Google Sheets invoicing)
        paid_hq = firm.get("total_invoiced_sheets", 0.0) or 0.0

        total_paid_legal_leadz += paid_ll
        total_paid_hq_intake += paid_hq

        firms_context.append({
            "firm_name": name,
            "paid_legal_leadz": paid_ll,
            "paid_hq_intake": paid_hq,
            "fb_invoice_count": firm.get("fb_invoice_count", 0) or 0,
            "total_signups": firm.get("hubspot_signups_since_dec20", 0) or firm.get("total_signups", 0) or 0,
        })

    generated_at = datetime.now().strftime("%B %d, %Y at %I:%M %p")

    return render_template("sales_snapshot.html",
        token=token,
        firms=firms_context,
        firm_names_json=json.dumps(firm_names),
        total_paid_legal_leadz=total_paid_legal_leadz,
        total_paid_hq_intake=total_paid_hq_intake,
        generated_at=generated_at,
    )


def hubspot_get_signed_deals_for_firm(firm_name):
    """Fast version: only fetch deals in signed stages for a firm.
    Much faster than hubspot_get_leads_for_firm() which fetches ALL deals."""
    import requests as req

    if not HUBSPOT_API_KEY:
        return []

    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    # Signed deal stages
    SIGNED_STAGES = ["closedwon", "closedlost", "3022527196", "3022527198"]

    search_token = firm_name.split()[0]
    deals = []
    after = 0

    for _ in range(10):  # max 1000 signed deals
        body = {
            "filterGroups": [{
                "filters": [
                    {
                        "propertyName": "dealname",
                        "operator": "CONTAINS_TOKEN",
                        "value": search_token
                    },
                    {
                        "propertyName": "dealstage",
                        "operator": "IN",
                        "values": SIGNED_STAGES
                    }
                ]
            }],
            "properties": ["dealname", "dealstage", "createdate"],
            "limit": 100,
        }
        if after:
            body["after"] = str(after)

        for attempt in range(3):
            resp = req.post(
                "https://api.hubapi.com/crm/v3/objects/deals/search",
                headers=headers, json=body
            )
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 2)) + 1)
                continue
            break

        if resp.status_code != 200:
            break

        data = resp.json()
        results = data.get("results", [])
        if not results:
            break
        deals.extend(results)
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    if not deals:
        return []

    # Build lead list from deal names (parse "Name / Case Type - Firm")
    leads = []
    for d in deals:
        props = d.get("properties", {})
        dealname = props.get("dealname", "")
        stage_id = props.get("dealstage", "")
        stage_label = DEAL_STAGE_LABELS.get(stage_id, stage_id)
        date = props.get("createdate", "")[:10] if props.get("createdate") else ""

        # Parse contact name and case type from dealname
        contact_name = dealname.split("/")[0].strip() if "/" in dealname else dealname
        case_type = ""
        if "/" in dealname:
            rest = dealname.split("/", 1)[1].strip()
            # "Case Type - Firm Name" → extract case type
            case_type = rest.split("-")[0].strip() if "-" in rest else rest

        leads.append({
            "name": contact_name,
            "status": stage_label,
            "date": date,
            "case_type": case_type,
        })

    return leads


@app.route("/api/sales-snapshot/leads/<token>/<firm_name>")
def api_sales_snapshot_leads(token, firm_name):
    """Get signed leads for a specific firm within a sales snapshot."""
    tokens = load_sales_snapshot_tokens()
    if token not in tokens:
        return jsonify({"error": "Invalid token"}), 404

    token_data = tokens[token]
    allowed_firms = token_data.get("firm_names", [])

    if firm_name not in allowed_firms:
        return jsonify({"error": "Firm not in snapshot"}), 403

    try:
        leads = hubspot_get_signed_deals_for_firm(firm_name)
        return jsonify({"firm": firm_name, "leads": leads, "count": len(leads)})
    except Exception as e:
        return jsonify({"error": str(e), "leads": [], "count": 0}), 500


@app.route("/sales-snapshot/health")
def sales_snapshot_health():
    return jsonify({"status": "ok"})


# ── Main ──
if __name__ == "__main__":
    if not SHARE_TOKENS_FILE.exists():
        save_share_tokens({})

    port = int(os.environ.get("PORT", 8090))
    print(f"[Billing Portal] Starting on port {port}...")
    print(f"[Billing Portal] Data file: {DATA_FILE}")
    print(f"[Billing Portal] Share tokens: {SHARE_TOKENS_FILE}")
    app.run(host="0.0.0.0", port=port, debug=False)
