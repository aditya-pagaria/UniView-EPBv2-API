#!/usr/bin/env python3
"""
API.py — fetch assets and write artifact/assets_rows.json, then trigger notifier.

Requires: requests
Install: pip install requests
Run: python3 API.py
"""
import sys
import requests
from collections import OrderedDict
from datetime import datetime, timezone
import json
from pathlib import Path

# Module to handle logging — append daily log
import pathlib
from datetime import datetime
log_dir = pathlib.Path(__file__).resolve().parent / "artifact"
log_dir.mkdir(parents=True, exist_ok=True)
utcnow = datetime.now(timezone.utc)
log_file = log_dir / f"API-{utcnow.strftime('%Y-%m-%d')}.log"
_log_f = open(log_file, "a", encoding="utf-8", buffering=1)
sys.stdout = sys.stderr = _log_f

print("=" * 8)
print(datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
print("=" * 8)

# --- Load config (preferred) or use environment vars ---
import os

CONFIG_PATH = Path(__file__).resolve().parent / "artifact" / "config.json"
_config = {}
if CONFIG_PATH.exists():
    try:
        _config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        print(f"Loaded config from {CONFIG_PATH}")
    except Exception as e:
        print(f"Failed to parse config file {CONFIG_PATH}: {e}", file=sys.stderr)

def cfg(name, default=None):
    # check config file, then env, then default
    if name in _config:
        return _config[name]
    return os.environ.get(name, default)

AUTH_URL = cfg("AUTH_URL", "https://login.backup.net/connect/token")
CLIENT_URL = cfg("CLIENT_URL", "https://public-api.backup.net/api/epb/v1/assets?page_size=300")
CLIENT_ID = cfg("CLIENT_ID", None)
CLIENT_SECRET = cfg("CLIENT_SECRET", None)
GRANT_TYPE = "client_credentials"

# notifier script name (same dir)
NOTIFIER_SCRIPT = "send_asset_email.py"

# running options
DRY_RUN = True       # keep DRY_RUN True by default
SAMPLE_COUNT = int(cfg("SAMPLE_COUNT", "5"))  # 0 => all, default same as before

# --- token and fetch functions ---
def get_token():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("CLIENT_ID or CLIENT_SECRET missing; cannot obtain token.", file=sys.stderr)
        return None
    body = {
        "grant_type": GRANT_TYPE,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        r = requests.post(AUTH_URL, data=body, headers=headers, timeout=30)
    except Exception as e:
        print(f"Failed to request token: {e}", file=sys.stderr)
        return None

    if r.status_code != 200:
        print(f"Failed to obtain access token. HTTP {r.status_code}: {r.text}", file=sys.stderr)
        return None

    j = r.json()
    token = j.get("access_token")
    if not token:
        print(f"Failed to obtain access token. Response: {j}", file=sys.stderr)
        return None
    return token

def fetch_assets(token):
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(CLIENT_URL, headers=headers, timeout=30)
    except Exception as e:
        print(f"Failed to fetch assets: {e}", file=sys.stderr)
        return []
    if r.status_code != 200:
        print(f"Failed to fetch assets. HTTP {r.status_code}: {r.text}", file=sys.stderr)
        return []
    try:
        data = r.json()
    except ValueError:
        print("Failed to parse JSON response from assets endpoint.", file=sys.stderr)
        return []
    if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
        return data["items"]
    if isinstance(data, list):
        return data
    for v in data.values() if isinstance(data, dict) else []:
        if isinstance(v, list):
            return v
    return []

# no timezone conversion functions — we keep timestamps exactly as returned (UTC)

def normalize_assets(asset_list):
    """
    Convert list of asset dicts into list of rows (dicts).
    Fields saved: Organization, AssetId, AssetName, Status, LastSuccessfulBackup, lastSuccessfulBackupTimestamp
    LastSuccessfulBackup is the raw timestamp string preserved (or empty string).
    """
    rows = []
    for a in asset_list:
        org = a.get("customerName") or "Unassigned"
        raw_ts = a.get("lastSuccessfulBackupTimestamp")
        # keep raw timestamp (no conversion). We'll attempt to preserve ISO/int as-is.
        last_backup_display = ""
        if raw_ts is not None:
            # Try to keep as ISO string if it's iso-like, else numeric as returned
            last_backup_display = str(raw_ts)
        row = {
            "Organization": org,
            "AssetId": a.get("id"),
            "AssetName": a.get("name"),
            "Status": a.get("status"),
            "LastSuccessfulBackup": last_backup_display,
            "lastSuccessfulBackupTimestamp": raw_ts
        }
        rows.append(row)
    return rows

def sort_and_group(rows):
    rows_sorted = sorted(rows, key=lambda r: (r["Organization"] or "", r["AssetName"] or ""))
    grouped = OrderedDict()
    for r in rows_sorted:
        org = r["Organization"]
        grouped.setdefault(org, []).append(r)
    return rows_sorted, grouped

def format_table(rows, columns):
    headers = columns
    widths = {col: len(col) for col in headers}
    for row in rows:
        for col in headers:
            val = "" if row.get(col) is None else str(row.get(col))
            widths[col] = max(widths[col], len(val))
    sep = "  "
    header_line = sep.join(col.ljust(widths[col]) for col in headers)
    underline = sep.join("-" * widths[col] for col in headers)
    data_lines = []
    for row in rows:
        line = sep.join(
            ("" if row.get(col) is None else str(row.get(col))).ljust(widths[col])
            for col in headers
        )
        data_lines.append(line)
    return "\n".join([header_line, underline] + data_lines)

def print_report(rows_sorted, grouped):
    print()
    print(f"Assets summary — total assets: {len(rows_sorted)}")
    for org, grp_rows in grouped.items():
        org_count = len(grp_rows)
        print()
        print("=" * 80)
        print(f"Organization: {org}    |    Assets: {org_count}")
        print("=" * 80)
        cols = ["AssetName", "AssetId", "Status", "LastSuccessfulBackup"]
        table_text = format_table(grp_rows, cols)
        print(table_text)

def quick_checks(rows_sorted):
    no_backup = [r for r in rows_sorted if not r.get("LastSuccessfulBackup")]
    non_green = [r for r in rows_sorted if r.get("Status") and "offline" in str(r.get("Status")).lower()]
    print()
    print("Quick health checks:")
    print(f"  Assets with no recorded last backup: {len(no_backup)}")
    print(f"  Assets with non-success/attention statuses (offline): {len(non_green)}")

def main():
    token = get_token()
    if not token:
        print("Failed to obtain access token. Exiting.", file=sys.stderr)
        sys.exit(1)

    asset_list = fetch_assets(token)
    if not asset_list:
        print("No assets returned (empty list). Exiting.", file=sys.stderr)
        sys.exit(1)

    rows = normalize_assets(asset_list)
    rows_sorted, grouped = sort_and_group(rows)
    # print_report(rows_sorted, grouped)                                                #uncomment out if you want to save what you're pulling from API to logs
    # quick_checks(rows_sorted)                                                         #uncomment out if you want to save what you're pulling from API to logs

    # save to artifact/assets_rows.json
    out_dir = Path(__file__).resolve().parent / "artifact"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "assets_rows.json"

    rows_to_save = []
    for r in rows:
        rows_to_save.append({
            "Organization": r.get("Organization"),
            "AssetId": r.get("AssetId"),
            "AssetName": r.get("AssetName"),
            "Status": r.get("Status"),
            "LastSuccessfulBackup": r.get("LastSuccessfulBackup"),
            "lastSuccessfulBackupTimestamp": r.get("lastSuccessfulBackupTimestamp")
        })

    with out_file.open("w", encoding="utf-8") as f:
        json.dump(rows_to_save, f, ensure_ascii=False, indent=2)

    print(f"\nWrote {len(rows_to_save)} normalized rows to {out_file}\n")

    # invoke notifier script (same interpreter)
    import subprocess
    base_dir = Path(__file__).resolve().parent
    notifier_path = (base_dir / NOTIFIER_SCRIPT).resolve()

    print(f"Running notifier script: {notifier_path} ...")

    if not notifier_path.exists():
        print(f"Notifier script not found at: {notifier_path}")
    else:
        cmd = [sys.executable, str(notifier_path)]
        try:
            # inherit environment; notifier will read config.json or env
            result = subprocess.run(cmd, check=True)
            print("Email notifier completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Notifier script exited with error code {e.returncode}")
        except Exception as e:
            print(f"Unexpected error while running notifier: {e}")

if __name__ == "__main__":
    main()

