#!/usr/bin/env python3
"""
BCBS Member ID Prefix Enumerator
=================================
Queries the BCBS "Find Your Local Company" API endpoint for every possible
3-letter member ID prefix (AAA–ZZZ), then saves the results to a CSV file.

Endpoint:  POST https://www.bcbs.com/planfinder/prefix
Body:      prefix=<PREFIX>   (application/x-www-form-urlencoded)
Response:
  - HTTP 200 + JSON array  → prefix is recognized; one or more plans returned
  - HTTP 404 + {}          → prefix is not recognized / no match

API behaviour notes
-------------------
* Only 3-letter prefixes are supported.  1- and 2-letter inputs always return
  HTTP 404.
* The CDN caches 200 responses for 30 days and 404 responses for ~5 seconds.
* The server uses a token-bucket rate limiter (x-dotratelimit-toks-max header)
  with a pool of 10,000 tokens; each request costs 0.00 tokens, so the limiter
  is not a practical constraint.  A conservative inter-request delay of 1.0 s
  is still recommended to be a polite client.
* Total combinations: 26³ = 17,576 prefixes.

Resume / checkpoint system
--------------------------
Every time a prefix is successfully queried (match or no-match), the script
writes the prefix to a small checkpoint file (<output>.checkpoint).  On the
next run with --resume (or if the checkpoint file already exists), the script
reads that file and skips every prefix already recorded there — so no prefix
is ever queried twice, regardless of how the previous run ended.

The checkpoint file is automatically deleted when the full run completes
successfully.  If you want to start completely fresh, delete both the output
CSV and the checkpoint file, or use --reset.

Usage
-----
    python3 bcbs_prefix_lookup.py [options]

Options
-------
    --delay FLOAT      Seconds to wait between requests (default: 1.0)
    --output FILE      Output CSV file path (default: bcbs_prefix_results.csv)
    --resume           Resume from checkpoint (auto-detected if checkpoint exists)
    --reset            Delete existing output and checkpoint, start fresh
    --verbose          Print every prefix, not just matches
    --status           Show progress summary from existing files and exit

Output CSV columns
------------------
    prefix, plan_name, plan_index, general_url, individuals_families_url,
    employer_url, medicare_url, contact_us_url, careers_url
"""

import argparse
import csv
import itertools
import json
import string
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_URL = "https://www.bcbs.com/planfinder/prefix"

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
    "prefix",
    "plan_name",
    "plan_index",
    "general_url",
    "individuals_families_url",
    "employer_url",
    "medicare_url",
    "contact_us_url",
    "careers_url",
]

LETTERS = string.ascii_uppercase  # A–Z
TOTAL_PREFIXES = 26 ** 3  # 17,576


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def checkpoint_path(output_path: Path) -> Path:
    """Return the checkpoint file path corresponding to an output CSV path."""
    return output_path.with_suffix(".checkpoint")


def load_checkpoint(ckpt_path: Path) -> set[str]:
    """
    Load the set of already-queried prefixes from the checkpoint file.

    The checkpoint file is a plain-text file with one prefix per line,
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


def write_checkpoint_entry(ckpt_file, prefix: str) -> None:
    """Append a single prefix to the open checkpoint file and flush."""
    ckpt_file.write(prefix + "\n")
    ckpt_file.flush()


def show_status(output_path: Path) -> None:
    """Print a human-readable progress summary from existing files."""
    ckpt_path = checkpoint_path(output_path)
    queried = load_checkpoint(ckpt_path)
    total_queried = len(queried)
    pct = total_queried / TOTAL_PREFIXES * 100

    print(f"\n{'='*60}")
    print(f"  BCBS Prefix Lookup — Status Report")
    print(f"{'='*60}")
    print(f"  Total prefixes (AAA–ZZZ):  {TOTAL_PREFIXES:,}")
    print(f"  Queried so far:            {total_queried:,}  ({pct:.1f}%)")
    print(f"  Remaining:                 {TOTAL_PREFIXES - total_queried:,}")
    print(f"  Checkpoint file:           {ckpt_path}")

    if output_path.exists():
        with open(output_path, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        match_count = len(rows)
        unique_prefixes = len({r["prefix"] for r in rows})
        unique_plans = len({r["plan_name"] for r in rows})
        print(f"  Output CSV:                {output_path}")
        print(f"  Matched prefixes:          {unique_prefixes:,}")
        print(f"  Total rows (incl. multi):  {match_count:,}")
        print(f"  Unique plan names:         {unique_plans:,}")
    else:
        print(f"  Output CSV:                (not yet created)")

    if queried:
        all_prefixes = list(generate_prefixes())
        remaining = [p for p in all_prefixes if p not in queried]
        if remaining:
            print(f"  Next prefix to query:      {remaining[0]}")
        else:
            print(f"  Run is COMPLETE.")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Prefix generator
# ---------------------------------------------------------------------------

def generate_prefixes():
    """Yield every 3-letter uppercase prefix from AAA to ZZZ."""
    for combo in itertools.product(LETTERS, repeat=3):
        yield "".join(combo)


# ---------------------------------------------------------------------------
# API query
# ---------------------------------------------------------------------------

def query_prefix(session: requests.Session, prefix: str) -> list[dict]:
    """
    POST the prefix to the BCBS planfinder API.

    Returns a list of plan dicts on HTTP 200, or an empty list on HTTP 404.
    Raises requests.HTTPError for unexpected status codes.
    """
    resp = session.post(
        API_URL,
        data={"prefix": prefix},
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


def plan_to_row(prefix: str, plan: dict) -> dict:
    """Flatten a plan dict into a CSV row dict."""
    urls = plan.get("urls", {})
    return {
        "prefix": prefix,
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
        description="Enumerate all AAA–ZZZ BCBS member ID prefixes and save results to CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Seconds to wait between requests (default: 1.0).",
    )
    parser.add_argument(
        "--output", type=str, default="bcbs_prefix_results.csv",
        help="Output CSV file path (default: bcbs_prefix_results.csv)",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume from checkpoint file (also auto-detected if checkpoint exists).",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Delete existing output CSV and checkpoint file, then start fresh.",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print every prefix queried, not just those with matches.",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Print a progress summary from existing files and exit.",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    ckpt_path = checkpoint_path(output_path)

    # ------------------------------------------------------------------
    # --status: just report and exit
    # ------------------------------------------------------------------
    if args.status:
        show_status(output_path)
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
    queried_prefixes: set[str] = set()
    if args.resume:
        queried_prefixes = load_checkpoint(ckpt_path)
        if queried_prefixes:
            pct = len(queried_prefixes) / TOTAL_PREFIXES * 100
            print(
                f"[resume] {len(queried_prefixes):,} / {TOTAL_PREFIXES:,} prefixes "
                f"already queried ({pct:.1f}%) — skipping these.\n"
            )

    # ------------------------------------------------------------------
    # Open output CSV (append if resuming, write fresh otherwise)
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
    # Write a header comment on first use
    if ckpt_path.stat().st_size == 0:
        ckpt_file.write(f"# BCBS prefix checkpoint — started {datetime.now().isoformat()}\n")
        ckpt_file.flush()

    # ------------------------------------------------------------------
    # Enumerate prefixes
    # ------------------------------------------------------------------
    done = 0
    hits = 0
    errors = 0
    session = requests.Session()

    remaining = TOTAL_PREFIXES - len(queried_prefixes)
    est_hours = remaining * args.delay / 3600
    print(
        f"Starting enumeration: {remaining:,} prefixes remaining "
        f"(delay={args.delay}s, est. {est_hours:.1f} h).\n"
        f"Output:     {output_path.resolve()}\n"
        f"Checkpoint: {ckpt_path.resolve()}\n"
        + "-" * 60
    )

    try:
        for prefix in generate_prefixes():
            if prefix in queried_prefixes:
                done += 1
                continue

            # --- Query ---
            try:
                plans = query_prefix(session, prefix)
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "?"
                print(
                    f"[{done+1}/{TOTAL_PREFIXES}] {prefix}  "
                    f"HTTP {status} — waiting 10 s then retrying …",
                    file=sys.stderr,
                )
                errors += 1
                time.sleep(10)
                try:
                    plans = query_prefix(session, prefix)
                except Exception:
                    plans = []
            except requests.RequestException as exc:
                print(
                    f"[{done+1}/{TOTAL_PREFIXES}] {prefix}  Network error: {exc}",
                    file=sys.stderr,
                )
                errors += 1
                plans = []

            # --- Write results ---
            if plans:
                hits += 1
                for plan in plans:
                    writer.writerow(plan_to_row(prefix, plan))
                csv_file.flush()
                plan_names = " | ".join(p.get("name", "?") for p in plans)
                print(f"[{done+1}/{TOTAL_PREFIXES}] {prefix}  ✓  {plan_names}")
            elif args.verbose:
                print(f"[{done+1}/{TOTAL_PREFIXES}] {prefix}  (no match)")

            # --- Record in checkpoint (ALWAYS, even on no-match) ---
            write_checkpoint_entry(ckpt_file, prefix)
            done += 1

            # Progress summary every 500 prefixes
            if done % 500 == 0:
                pct = (len(queried_prefixes) + done) / TOTAL_PREFIXES * 100
                print(
                    f"--- Progress: {len(queried_prefixes)+done:,}/{TOTAL_PREFIXES:,} "
                    f"({pct:.1f}%) — {hits:,} matches, {errors:,} errors ---"
                )

            time.sleep(args.delay)

    except KeyboardInterrupt:
        print(
            f"\nInterrupted at prefix {prefix} after {done:,} queries this session.\n"
            f"Checkpoint saved to: {ckpt_path.resolve()}\n"
            f"Run again with --resume (or just re-run; it auto-detects the checkpoint)."
        )
    finally:
        csv_file.close()
        ckpt_file.close()

    # ------------------------------------------------------------------
    # Clean up checkpoint on successful full completion
    # ------------------------------------------------------------------
    total_done = len(queried_prefixes) + done
    if total_done >= TOTAL_PREFIXES:
        ckpt_path.unlink(missing_ok=True)
        print(f"\nCheckpoint file removed (run complete).")

    print("-" * 60)
    print(
        f"Session complete.  {done:,} prefixes queried this session, "
        f"{hits:,} matches found, {errors:,} errors.\n"
        f"Results saved to: {output_path.resolve()}"
    )


if __name__ == "__main__":
    main()
