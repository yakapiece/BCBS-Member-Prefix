#!/usr/bin/env python3
"""
BCBS ZIP Code Lookup — All US ZIP Codes
=========================================
Queries the BCBS "Find Your Local Company" API endpoint for every valid US
ZIP code and saves the results to a CSV file.

Endpoint:  POST https://www.bcbs.com/planfinder/zipcode
Body:      zipCode=<5-DIGIT-ZIP>   (application/x-www-form-urlencoded)
Response:
  - HTTP 200 + JSON array  → ZIP is recognised; one or more plans returned
  - HTTP 404 + {}          → ZIP is not in the BCBS database (no match)

ZIP code source
---------------
The script reads ZIP codes from a bundled CSV file (us_zipcodes_list.csv)
derived from the GeoNames postal code dataset, which contains 41,488 unique
5-digit US ZIP codes with city, state, and county metadata.

If the bundled CSV is not present, the script falls back to generating every
numeric value from 00001 to 99999 (brute-force mode).

Resume / checkpoint system
--------------------------
Every time a ZIP code is successfully queried (match or no-match), the script
writes it to a small checkpoint file (<output>.checkpoint).  On the next run,
the script reads that file and skips every ZIP already recorded — so no ZIP
is ever queried twice, regardless of how the previous run ended.

The checkpoint file is automatically deleted when the full run completes
successfully.  Use --reset to delete both files and start completely fresh.

Usage
-----
    python3 bcbs_zipcode_lookup.py [options]

Options
-------
    --delay FLOAT      Seconds between requests (default: 1.0)
    --output FILE      Output CSV path (default: bcbs_zipcode_results.csv)
    --zipfile FILE     Path to ZIP code list CSV (default: us_zipcodes_list.csv)
    --resume           Resume from checkpoint (auto-detected if checkpoint exists)
    --reset            Delete existing output and checkpoint, start fresh
    --verbose          Print every ZIP queried, not just matches
    --status           Show progress summary from existing files and exit
    --state STATE      Only query ZIPs for a specific state abbreviation (e.g. TX)
    --brute-force      Ignore zipfile and iterate 00001–99999 sequentially

Output CSV columns
------------------
    zip_code, city, state_abbr, state_name, county,
    plan_name, plan_index, general_url, individuals_families_url,
    employer_url, medicare_url, contact_us_url, careers_url
"""

import argparse
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_URL = "https://www.bcbs.com/planfinder/zipcode"

REQUEST_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": (
        "https://www.bcbs.com/about-us/blue-cross-blue-shield-system/"
        "state-health-plan-companies"
    ),
    "Origin": "https://www.bcbs.com",
    "Accept": "application/json, text/plain, */*",
}

CSV_FIELDNAMES = [
    "zip_code",
    "city",
    "state_abbr",
    "state_name",
    "county",
    "plan_name",
    "plan_index",
    "general_url",
    "individuals_families_url",
    "employer_url",
    "medicare_url",
    "contact_us_url",
    "careers_url",
]

DEFAULT_ZIPFILE = Path(__file__).parent / "us_zipcodes_list.csv"


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def checkpoint_path(output_path: Path) -> Path:
    """Return the checkpoint file path corresponding to an output CSV path."""
    return output_path.with_suffix(".checkpoint")


def load_checkpoint(ckpt_path: Path) -> set[str]:
    """
    Load the set of already-queried ZIP codes from the checkpoint file.

    The checkpoint file is a plain-text file with one ZIP per line,
    plus optional comment lines starting with '#'.
    """
    queried: set[str] = set()
    if not ckpt_path.exists():
        return queried
    with open(ckpt_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                queried.add(line)
    return queried


def write_checkpoint_entry(ckpt_file, zip_code: str) -> None:
    """Append a single ZIP code to the open checkpoint file and flush."""
    ckpt_file.write(zip_code + "\n")
    ckpt_file.flush()


def show_status(output_path: Path, zip_rows: list[dict]) -> None:
    """Print a human-readable progress summary from existing files."""
    ckpt_path = checkpoint_path(output_path)
    queried = load_checkpoint(ckpt_path)
    total = len(zip_rows)
    total_queried = len(queried)
    pct = total_queried / total * 100 if total else 0

    print(f"\n{'='*60}")
    print(f"  BCBS ZIP Code Lookup — Status Report")
    print(f"{'='*60}")
    print(f"  Total ZIP codes in list:   {total:,}")
    print(f"  Queried so far:            {total_queried:,}  ({pct:.1f}%)")
    print(f"  Remaining:                 {max(0, total - total_queried):,}")
    print(f"  Checkpoint file:           {ckpt_path}")

    if output_path.exists():
        with open(output_path, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        match_count = len(rows)
        unique_zips = len({r["zip_code"] for r in rows})
        unique_plans = len({r["plan_name"] for r in rows})
        print(f"  Output CSV:                {output_path}")
        print(f"  Matched ZIP codes:         {unique_zips:,}")
        print(f"  Total rows (incl. multi):  {match_count:,}")
        print(f"  Unique plan names:         {unique_plans:,}")
    else:
        print(f"  Output CSV:                (not yet created)")

    remaining_zips = [r["zip"] for r in zip_rows if r["zip"] not in queried]
    if remaining_zips:
        print(f"  Next ZIP to query:         {remaining_zips[0]}")
    else:
        print(f"  Run is COMPLETE.")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# ZIP code loading
# ---------------------------------------------------------------------------

def load_zipcodes(zipfile_path: Path, state_filter: str | None = None) -> list[dict]:
    """Load ZIP codes from the GeoNames-derived CSV."""
    rows = []
    with open(zipfile_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if state_filter and row.get("state_abbr", "").upper() != state_filter.upper():
                continue
            rows.append(row)
    return rows


def generate_brute_force_zipcodes() -> list[dict]:
    """Generate every 5-digit number from 00001 to 99999."""
    return [
        {"zip": f"{i:05d}", "city": "", "state_abbr": "", "state_name": "", "county": ""}
        for i in range(1, 100_000)
    ]


# ---------------------------------------------------------------------------
# API query
# ---------------------------------------------------------------------------

def query_zipcode(session: requests.Session, zip_code: str) -> list[dict]:
    """
    POST the ZIP code to the BCBS planfinder API.

    Returns a list of plan dicts on HTTP 200, or an empty list on HTTP 404.
    Raises requests.HTTPError for unexpected status codes.
    """
    resp = session.post(
        API_URL,
        data={"zipCode": zip_code},
        headers=REQUEST_HEADERS,
        timeout=15,
    )

    if resp.status_code == 404:
        return []

    resp.raise_for_status()

    raw = resp.text.strip()
    if not raw or raw == "{}":
        return []

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []

    if isinstance(parsed, dict):
        return [parsed] if parsed else []
    if isinstance(parsed, list):
        return parsed
    return []


def plan_to_row(zip_meta: dict, plan: dict) -> dict:
    """Merge ZIP metadata and plan data into a single CSV row."""
    urls = plan.get("urls", {})
    return {
        "zip_code": zip_meta.get("zip", ""),
        "city": zip_meta.get("city", ""),
        "state_abbr": zip_meta.get("state_abbr", ""),
        "state_name": zip_meta.get("state_name", ""),
        "county": zip_meta.get("county", ""),
        "plan_name": plan.get("name", ""),
        "plan_index": plan.get("index", ""),
        "general_url": urls.get("general", ""),
        "individuals_families_url": urls.get("individualsFamilies", ""),
        "employer_url": urls.get("employer", ""),
        "medicare_url": urls.get("medicare", ""),
        "contact_us_url": urls.get("contactUs", ""),
        "careers_url": urls.get("careers", ""),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Query the BCBS plan finder for every US ZIP code.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Seconds to wait between requests (default: 1.0).",
    )
    parser.add_argument(
        "--output", type=str, default="bcbs_zipcode_results.csv",
        help="Output CSV file path (default: bcbs_zipcode_results.csv)",
    )
    parser.add_argument(
        "--zipfile", type=str, default=str(DEFAULT_ZIPFILE),
        help=f"Path to ZIP code list CSV (default: {DEFAULT_ZIPFILE})",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume from checkpoint (also auto-detected if checkpoint exists).",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Delete existing output CSV and checkpoint file, then start fresh.",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print every ZIP queried, not just those with matches.",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Print a progress summary from existing files and exit.",
    )
    parser.add_argument(
        "--state", type=str, default=None, metavar="STATE",
        help="Only query ZIPs for a specific state abbreviation (e.g. TX, CA, NY).",
    )
    parser.add_argument(
        "--brute-force", action="store_true", dest="brute_force",
        help="Ignore zipfile and iterate every value from 00001 to 99999.",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    ckpt_path = checkpoint_path(output_path)

    # ------------------------------------------------------------------
    # Load ZIP code list (needed for --status too)
    # ------------------------------------------------------------------
    if args.brute_force:
        zip_rows = generate_brute_force_zipcodes()
        print("Brute-force mode: iterating 00001–99999 (99,999 requests).")
    else:
        zipfile_path = Path(args.zipfile)
        if not zipfile_path.exists():
            print(
                f"ERROR: ZIP code file not found: {zipfile_path}\n"
                "Download it from GeoNames (https://download.geonames.org/export/zip/US.zip)\n"
                "or use --brute-force to iterate all numeric values.",
                file=sys.stderr,
            )
            sys.exit(1)
        zip_rows = load_zipcodes(zipfile_path, state_filter=args.state)
        state_msg = f" (state={args.state})" if args.state else ""
        print(f"Loaded {len(zip_rows):,} ZIP codes from {zipfile_path}{state_msg}.")

    total = len(zip_rows)

    # ------------------------------------------------------------------
    # --status: just report and exit
    # ------------------------------------------------------------------
    if args.status:
        show_status(output_path, zip_rows)
        return

    # ------------------------------------------------------------------
    # --reset: wipe existing files and start clean
    # ------------------------------------------------------------------
    if args.reset:
        for p in (output_path, ckpt_path):
            if p.exists():
                p.unlink()
                print(f"Deleted: {p}")
        print("Reset complete.  Starting a fresh run.\n")

    # ------------------------------------------------------------------
    # Auto-detect resume: if a checkpoint file exists, always resume
    # ------------------------------------------------------------------
    if ckpt_path.exists() and not args.reset:
        if not args.resume:
            print(
                f"[auto-resume] Checkpoint file found: {ckpt_path}\n"
                "              Resuming automatically.  Use --reset to start fresh.\n"
            )
        args.resume = True

    # ------------------------------------------------------------------
    # Load checkpoint
    # ------------------------------------------------------------------
    queried_zips: set[str] = set()
    if args.resume:
        queried_zips = load_checkpoint(ckpt_path)
        if queried_zips:
            pct = len(queried_zips) / total * 100
            print(
                f"[resume] {len(queried_zips):,} / {total:,} ZIP codes "
                f"already queried ({pct:.1f}%) — skipping these.\n"
            )

    # ------------------------------------------------------------------
    # Open output CSV
    # ------------------------------------------------------------------
    csv_mode = "a" if args.resume and output_path.exists() else "w"
    csv_file = open(output_path, csv_mode, newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDNAMES)
    if csv_mode == "w":
        writer.writeheader()

    # ------------------------------------------------------------------
    # Open checkpoint file for appending
    # ------------------------------------------------------------------
    ckpt_file = open(ckpt_path, "a", encoding="utf-8")
    if ckpt_path.stat().st_size == 0:
        ckpt_file.write(f"# BCBS zipcode checkpoint — started {datetime.now().isoformat()}\n")
        ckpt_file.flush()

    # ------------------------------------------------------------------
    # Enumerate ZIP codes
    # ------------------------------------------------------------------
    done = 0
    hits = 0
    errors = 0
    session = requests.Session()

    remaining = total - len(queried_zips)
    est_hours = remaining * args.delay / 3600
    print(
        f"Starting enumeration: {remaining:,} ZIP codes remaining "
        f"(delay={args.delay}s, est. {est_hours:.1f} h).\n"
        f"Output:     {output_path.resolve()}\n"
        f"Checkpoint: {ckpt_path.resolve()}\n"
        + "-" * 60
    )

    last_zip = ""
    try:
        for zip_meta in zip_rows:
            zip_code = zip_meta["zip"]
            last_zip = zip_code

            if zip_code in queried_zips:
                done += 1
                continue

            # --- Query ---
            try:
                plans = query_zipcode(session, zip_code)
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "?"
                print(
                    f"[{done+1}/{total}] {zip_code}  "
                    f"HTTP {status} — waiting 10 s then retrying …",
                    file=sys.stderr,
                )
                errors += 1
                time.sleep(10)
                try:
                    plans = query_zipcode(session, zip_code)
                except Exception:
                    plans = []
            except requests.RequestException as exc:
                print(
                    f"[{done+1}/{total}] {zip_code}  Network error: {exc}",
                    file=sys.stderr,
                )
                errors += 1
                plans = []

            # --- Write results ---
            if plans:
                hits += 1
                for plan in plans:
                    writer.writerow(plan_to_row(zip_meta, plan))
                csv_file.flush()
                city_state = (
                    f"{zip_meta.get('city', '')}, {zip_meta.get('state_abbr', '')}"
                    if zip_meta.get("city") else ""
                )
                plan_names = " | ".join(p.get("name", "?") for p in plans)
                print(f"[{done+1}/{total}] {zip_code} {city_state:<28}  ✓  {plan_names}")
            elif args.verbose:
                city_state = (
                    f"{zip_meta.get('city', '')}, {zip_meta.get('state_abbr', '')}"
                    if zip_meta.get("city") else ""
                )
                print(f"[{done+1}/{total}] {zip_code} {city_state:<28}  (no match)")

            # --- Record in checkpoint (ALWAYS, even on no-match) ---
            write_checkpoint_entry(ckpt_file, zip_code)
            done += 1

            # Progress summary every 1,000 ZIPs
            if done % 1000 == 0:
                total_done = len(queried_zips) + done
                pct = total_done / total * 100
                print(
                    f"--- Progress: {total_done:,}/{total:,} ({pct:.1f}%) — "
                    f"{hits:,} matches, {errors:,} errors ---"
                )

            time.sleep(args.delay)

    except KeyboardInterrupt:
        print(
            f"\nInterrupted at ZIP {last_zip} after {done:,} queries this session.\n"
            f"Checkpoint saved to: {ckpt_path.resolve()}\n"
            f"Run again with --resume (or just re-run; it auto-detects the checkpoint)."
        )
    finally:
        csv_file.close()
        ckpt_file.close()

    # ------------------------------------------------------------------
    # Clean up checkpoint on successful full completion
    # ------------------------------------------------------------------
    total_done = len(queried_zips) + done
    if total_done >= total:
        ckpt_path.unlink(missing_ok=True)
        print(f"\nCheckpoint file removed (run complete).")

    print("-" * 60)
    print(
        f"Session complete.  {done:,} ZIP codes queried this session, "
        f"{hits:,} matches found, {errors:,} errors.\n"
        f"Results saved to: {output_path.resolve()}"
    )


if __name__ == "__main__":
    main()
