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

Usage
-----
    python3 bcbs_prefix_lookup.py [options]

Options
-------
    --delay FLOAT      Seconds to wait between requests (default: 1.0)
    --output FILE      Output CSV file path (default: bcbs_prefix_results.csv)
    --resume           Skip prefixes already present in the output CSV
    --verbose          Print every prefix, not just matches

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
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_URL = "https://www.bcbs.com/planfinder/prefix"

# Mimic the headers sent by the browser when using the interactive form.
# The Origin and Referer headers are important for the server to accept the
# request without a CSRF token.
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

# Total number of 3-letter combinations
TOTAL_PREFIXES = 26 ** 3  # 17,576


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def generate_prefixes():
    """Yield every 3-letter uppercase prefix from AAA to ZZZ."""
    for combo in itertools.product(LETTERS, repeat=3):
        yield "".join(combo)


def query_prefix(session: requests.Session, prefix: str) -> list[dict]:
    """
    POST the prefix to the BCBS planfinder API.

    Returns
    -------
    list[dict]
        A list of plan objects on success (HTTP 200).
        An empty list when the prefix is not recognised (HTTP 404).

    Raises
    ------
    requests.HTTPError
        For any unexpected HTTP status code (not 200 or 404).
    requests.RequestException
        For network-level errors.
    """
    resp = session.post(
        API_URL,
        data={"prefix": prefix},
        headers=REQUEST_HEADERS,
        timeout=15,
    )

    if resp.status_code == 404:
        return []

    resp.raise_for_status()  # raise on any other non-2xx

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
    """Flatten a plan dict into a CSV row."""
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
        "--delay",
        type=float,
        default=1.0,
        help="Seconds to wait between requests (default: 1.0).  "
             "Use 0.5 for faster runs; increase if you see HTTP 429 errors.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="bcbs_prefix_results.csv",
        help="Output CSV file path (default: bcbs_prefix_results.csv)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume an interrupted run by skipping prefixes already in the output CSV.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print every prefix queried, not just those with matches.",
    )
    args = parser.parse_args()

    output_path = Path(args.output)

    # ------------------------------------------------------------------
    # Resume support: load already-queried prefixes from existing CSV
    # ------------------------------------------------------------------
    queried_prefixes: set[str] = set()
    if args.resume and output_path.exists():
        with open(output_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                queried_prefixes.add(row["prefix"])
        print(
            f"[resume] {len(queried_prefixes):,} prefixes already in output file — "
            "these will be skipped."
        )

    # ------------------------------------------------------------------
    # Open CSV for writing (append if resuming, write fresh otherwise)
    # ------------------------------------------------------------------
    file_mode = "a" if args.resume and output_path.exists() else "w"
    csv_file = open(output_path, file_mode, newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDNAMES)
    if file_mode == "w":
        writer.writeheader()

    # ------------------------------------------------------------------
    # Enumerate
    # ------------------------------------------------------------------
    done = 0
    hits = 0
    errors = 0
    session = requests.Session()

    print(
        f"Starting enumeration of {TOTAL_PREFIXES:,} prefixes "
        f"(delay={args.delay}s, output={output_path})"
    )
    print("-" * 60)

    try:
        for prefix in generate_prefixes():
            if prefix in queried_prefixes:
                done += 1
                continue

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
                    f"[{done+1}/{TOTAL_PREFIXES}] {prefix}  "
                    f"Network error: {exc}",
                    file=sys.stderr,
                )
                errors += 1
                plans = []

            if plans:
                hits += 1
                for plan in plans:
                    writer.writerow(plan_to_row(prefix, plan))
                csv_file.flush()
                plan_names = " | ".join(p.get("name", "?") for p in plans)
                print(f"[{done+1}/{TOTAL_PREFIXES}] {prefix}  ✓  {plan_names}")
            elif args.verbose:
                print(f"[{done+1}/{TOTAL_PREFIXES}] {prefix}  (no match)")

            done += 1

            # Progress summary every 500 prefixes
            if done % 500 == 0:
                pct = done / TOTAL_PREFIXES * 100
                print(
                    f"--- Progress: {done:,}/{TOTAL_PREFIXES:,} ({pct:.1f}%) — "
                    f"{hits:,} matches so far ---"
                )

            time.sleep(args.delay)

    except KeyboardInterrupt:
        print("\nInterrupted by user.  Partial results have been saved.")
    finally:
        csv_file.close()

    print("-" * 60)
    print(
        f"Finished.  {done:,} prefixes queried, "
        f"{hits:,} matches found, "
        f"{errors:,} errors.\n"
        f"Results saved to: {output_path.resolve()}"
    )


if __name__ == "__main__":
    main()
