#!/usr/bin/env python3
"""
send_asset_email.py

Reads artifact/assets_rows.json and sends CSV(s) to Backup Radar.

Behavior:
 - First run of the day at 00:30 UTC -> send full CSV (all assets).
 - Other runs -> send CSV containing only changed assets (state diffs).
 - State updated only after successful send (or simulation when DRY_RUN=True).
 - CSV columns exactly: Device,Status,Backup Date,Client,Job
"""
import json
from pathlib import Path
import re
from datetime import datetime, timezone, timedelta
import smtplib
from email.message import EmailMessage
import sys
import traceback
import os
import csv

# ---------------------- CONFIG ----------------------
# Config loaded from artifact/config.json or environment variables
CONFIG_PATH = Path(__file__).resolve().parent / "artifact" / "config.json"
_config = {}
if CONFIG_PATH.exists():
    try:
        _config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Failed to parse config file {CONFIG_PATH}: {e}", file=sys.stderr)

def cfg(name, default=None):
    if name in _config:
        return _config[name]
    return os.environ.get(name, default)

SMTP_HOST = cfg("SMTP_HOST")
SMTP_PORT = int(cfg("SMTP_PORT", 25))
SMTP_USER = cfg("SMTP_USER", "")
SMTP_PASS = cfg("SMTP_PASS", "")
SMTP_FROM = cfg("SMTP_FROM", SMTP_USER or "no-reply@example.com")
SMTP_USE_STARTTLS = bool(cfg("SMTP_USE_STARTTLS", True))
SMTP_USE_SSL = bool(cfg("SMTP_USE_SSL", False))
EMAIL_RECIPIENT = cfg("EMAIL_RECIPIENT")

# Running options
DRY_RUN = True  # keep DRY_RUN True for testing; true doesn't send emails
SAMPLE_COUNT = int(cfg("SAMPLE_COUNT", "0"))  # 0 => all, >0 => first N for tests
FIRST_RUN_FULL = True  # when no state exists, send full (seed)

# Files
WORK_DIR = Path(__file__).resolve().parent / "artifact"
WORK_DIR.mkdir(parents=True, exist_ok=True)
DATA_FILE = WORK_DIR / "assets_rows.json"
STATE_FILE = WORK_DIR / "assets_state.json"
CSV_FILE = WORK_DIR / "current_assets.csv"
LOG_FILE = WORK_DIR / "send.log"

# Email recipient fixed as requested
RECIPIENT = EMAIL_RECIPIENT

# CSV columns (must match exact case/order)
CSV_COLUMNS = ["Device", "Status", "Backup Date", "Client", "Job"]
JOB_NAME = "EPBv2"

# ---------------------- helpers ----------------------
def _sanitize_org_name(org_name: str) -> str:
    if not org_name:
        return "Unassigned"
    s = org_name.strip()
    sanitized = re.sub(r"[^A-Za-z0-9]", "", s)
    return sanitized or "Unassigned"

def _parse_to_utc_datetime(ts):
    """
    Accept epoch (int/float, seconds or millis) or ISO-like string.
    Returns timezone-aware UTC datetime or None.
    """
    if ts is None:
        return None
    # direct numeric types (seconds or milliseconds)
    if isinstance(ts, (int, float)):
        sec = float(ts)
        if sec > 1e12:
            sec /= 1000.0
        try:
            return datetime.fromtimestamp(sec, tz=timezone.utc)
        except Exception:
            return None

    s = str(ts).strip()
    if s == "":
        return None

    # numeric string
    if s.isdigit():
        n = int(s)
        if n > 1e12:
            n = n / 1000.0
        try:
            return datetime.fromtimestamp(n, tz=timezone.utc)
        except Exception:
            return None

    # ISO-like strings, handle trailing Z
    try:
        iso = s
        if iso.endswith("Z"):
            iso = iso[:-1] + "+00:00"
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        # fallback common formats
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                continue
    return None

def _epoch_seconds_or_none(dt_or_ts):
    if dt_or_ts is None:
        return None
    if isinstance(dt_or_ts, datetime):
        return int(dt_or_ts.timestamp())
    dt = _parse_to_utc_datetime(dt_or_ts)
    if dt:
        return int(dt.timestamp())
    return None

def compute_backup_status(row):
    """
    Option B mapping:
      - if Status contains 'in progress' -> 'Warning'
      - elif lastSuccessfulBackupTimestamp <= 12h -> 'Success'
      - elif lastSuccessfulBackupTimestamp >12h and <=23h -> 'Warning'
      - else -> 'Failure'
    """
    status_raw = row.get("Status")
    if status_raw and "in progress" in str(status_raw).lower():
        return "Warning"

    ts = row.get("lastSuccessfulBackupTimestamp")
    dt_utc = _parse_to_utc_datetime(ts)
    if dt_utc:
        now = datetime.now(timezone.utc)
        hours = (now - dt_utc).total_seconds() / 3600.0
        if hours <= 12:
            return "Success"
        if 12 < hours <= 23:
            return "Warning"
        return "Failure"
    return "Failure"

def _build_csv_rows(full_rows):
    """
    Given list of asset rows (dicts), produce CSV rows with required columns.
    """
    csv_rows = []
    for r in full_rows:
        device = r.get("AssetName") or ""
        backup_ts = r.get("lastSuccessfulBackupTimestamp")
        dt = _parse_to_utc_datetime(backup_ts)
        backup_date = dt.isoformat().replace("+00:00", "Z") if dt else ""
        client = r.get("Organization") or ""
        backup_status = compute_backup_status(r)
        csv_rows.append({
            "Device": device,
            "Status": backup_status,
            "Backup Date": backup_date,
            "Client": client,
            "Job": JOB_NAME
        })
    return csv_rows

def _write_csv(rows, csv_path):
    """
    Overwrite csv_path with given rows (list of dicts matching CSV_COLUMNS).
    """
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

def _send_email_real(to_addr, subject, body, attachment_path):
    msg = EmailMessage()
    msg["From"] = SMTP_FROM
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body or "See attached CSV.")

    # attach CSV
    with open(attachment_path, "rb") as af:
        data = af.read()
    msg.add_attachment(data, maintype="text", subtype="csv", filename=attachment_path.name)

    if SMTP_USE_SSL:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            if SMTP_USER:
                smtp.login(SMTP_USER, SMTP_PASS)
            smtp.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            if SMTP_USE_STARTTLS:
                smtp.starttls()
            if SMTP_USER:
                smtp.login(SMTP_USER, SMTP_PASS)
            smtp.send_message(msg)

def _print_email_simulation(to_addr, subject, body, attachment_path):
    print("------------------------------------------------------------")
    print(f"[DRY RUN] To: {to_addr}")
    print(f"[DRY RUN] Subject: {subject}")
    print(f"[DRY RUN] Attachment: {attachment_path}")
    print("[DRY RUN] Body:")
    print(body or "(no body)")
    print("------------------------------------------------------------")

def load_state():
    """
    Return dictionary state. Format:
      {
        "<asset_key>": {"ts": <int_epoch_or_null>, "status": <str_or_null>},
        ...
      }
    Asset key: AssetId (string)
    """
    if not STATE_FILE.exists():
        return {}
    try:
        j = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(j, dict):
            return j
    except Exception:
        pass
    return {}

def save_state(state):
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        tmp.replace(STATE_FILE)
    except Exception as e:
        print(f"Warning: failed to save state: {e}", file=sys.stderr)

def append_log(msg):
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        with LOG_FILE.open("a", encoding="utf-8") as lf:
            lf.write(f"{ts} {msg}\n")
    except Exception:
        pass

# ---------------------- core ----------------------
def main():
    if not DATA_FILE.exists():
        print(f"Data file not found: {DATA_FILE}", file=sys.stderr)
        return 1

    try:
        rows = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Failed to read/parse {DATA_FILE}: {e}", file=sys.stderr)
        return 1

    if not isinstance(rows, list):
        print(f"Data file format invalid: expected list of rows", file=sys.stderr)
        return 1

    to_process = rows if SAMPLE_COUNT == 0 else rows[:SAMPLE_COUNT]

    # detect UTC current time to decide full-run at 21:00
    now_utc = datetime.now(timezone.utc)
    is_daily_full_run = (now_utc.hour == 21) #add "and now_utc.minute == 0" if daily_full_run has to be scheduled down to the minute (not recommended)

    state = load_state()
    initial_run = (not bool(state))

    if initial_run and not FIRST_RUN_FULL:
        print("State missing and FIRST_RUN_FULL=False -> skipping sends this run.")
        save_state({})
        return 0

    sent = 0
    failed = 0
    failures = []
    new_state = dict(state)

    # Decide which assets to include in CSV and which to send
    rows_for_csv = []     # rows (dict CSV form) to write/send this run
    changed_asset_ids = []  # asset ids that will be updated after success

    # If daily full run or initial run-with-FULL -> include all assets
    if initial_run and FIRST_RUN_FULL:
        is_full = True
    else:
        is_full = is_daily_full_run

    if is_full:
        # Build full CSV rows for all to_process
        csv_rows = _build_csv_rows(to_process)
        rows_for_csv = csv_rows
        # mark all for state update on success
        for r in to_process:
            aid = str(r.get("AssetId") or "")
            changed_asset_ids.append(aid)
    else:
        # incremental: include only changed assets (timestamp newer OR status changed)
        for r in to_process:
            try:
                aid = str(r.get("AssetId") or "")
                current_ts_epoch = _epoch_seconds_or_none(r.get("lastSuccessfulBackupTimestamp"))
                current_status = (r.get("Status") or "").strip()

                stored = state.get(aid)
                stored_ts = None
                stored_status = None
                if isinstance(stored, dict):
                    stored_ts = stored.get("ts")
                    stored_status = stored.get("status")

                send_flag = False
                # if timestamp present and newer than stored
                if current_ts_epoch is not None:
                    try:
                        stored_ts_int = int(stored_ts) if stored_ts is not None else None
                    except Exception:
                        stored_ts_int = None
                    if stored_ts_int is None or current_ts_epoch > stored_ts_int:
                        send_flag = True
                # also if status changed (and not empty)
                if not send_flag and current_status and current_status != (stored_status or ""):
                    send_flag = True

                if send_flag:
                    # add to csv rows
                    csv_row = _build_csv_rows([r])[0]
                    rows_for_csv.append(csv_row)
                    changed_asset_ids.append(aid)

            except Exception as e:
                failed += 1
                failures.append(("internal", r.get("AssetName"), str(e)))
                print(f"[EXC] Unexpected error for asset {r.get('AssetName')}: {e}")
                traceback.print_exc()

    # If nothing to send, exit gracefully
    if not rows_for_csv:
        print("No changes to send (no CSV rows). Exiting.")
        append_log("No changes to send this run.")
        return 0

    # Write CSV (overwrite)
    try:
        _write_csv(rows_for_csv, CSV_FILE)
        print(f"Wrote CSV with {len(rows_for_csv)} rows to {CSV_FILE}")
    except Exception as e:
        print(f"Failed to write CSV: {e}", file=sys.stderr)
        return 1

    # Prepare email
    subject_time = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    subject_type = "FULL" if is_full else "INCR"
    subject = f"Backup Report CSV - {subject_type} - {subject_time}"
    body = f"Attached CSV ({subject_type}) generated at {subject_time}."

    # Attempt send (or simulate)
    success = False
    if DRY_RUN:
        _print_email_simulation(RECIPIENT, subject, body, CSV_FILE)
        success = True
        sent += len(rows_for_csv)
        append_log(f"[DRY RUN] {subject} rows={len(rows_for_csv)}")
    else:
        try:
            _send_email_real(RECIPIENT, subject, body, CSV_FILE)
            print(f"[OK] Sent CSV ({len(rows_for_csv)} rows) to {RECIPIENT}")
            success = True
            sent += len(rows_for_csv)
            append_log(f"[SENT] {subject} rows={len(rows_for_csv)}")
        except Exception as e:
            failed += 1
            failures.append(("email", str(e)))
            print(f"[ERR] Failed to send email: {e}")
            traceback.print_exc()
            append_log(f"[ERR] Failed to send email: {e}")

    # Update state only on success
    if success:
        # if full run: update all assets in to_process
        if is_full:
            for r in to_process:
                aid = str(r.get("AssetId") or "")
                current_ts_epoch = _epoch_seconds_or_none(r.get("lastSuccessfulBackupTimestamp"))
                current_status = (r.get("Status") or "").strip()
                new_state[aid] = {
                    "ts": int(current_ts_epoch) if current_ts_epoch is not None else None,
                    "status": current_status or None
                }
        else:
            # only update changed_asset_ids
            id_set = set(changed_asset_ids)
            for r in to_process:
                aid = str(r.get("AssetId") or "")
                if aid in id_set:
                    current_ts_epoch = _epoch_seconds_or_none(r.get("lastSuccessfulBackupTimestamp"))
                    current_status = (r.get("Status") or "").strip()
                    new_state[aid] = {
                        "ts": int(current_ts_epoch) if current_ts_epoch is not None else None,
                        "status": current_status or None
                    }
        save_state(new_state)
        append_log(f"State updated for {len(changed_asset_ids) if not is_full else len(to_process)} assets.")
    else:
        append_log("Send failed; state not updated.")

    # Summary prints
    print()
    print("Email send summary:")
    print(f"  Total assets in file: {len(rows)}")
    if SAMPLE_COUNT and SAMPLE_COUNT > 0:
        print(f"  Sample processed: {len(to_process)}")
    else:
        print(f"  Processed: {len(to_process)}")
    print(f"  Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"  Sent (or simulated): {sent}")
    print(f"  Failed: {failed}")
    if failures:
        print("  Failures (sample):")
        for f in failures[:10]:
            print("   ", f)

    return 0

if __name__ == "__main__":
    sys.exit(main())
