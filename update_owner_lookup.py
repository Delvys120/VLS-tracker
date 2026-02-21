"""
update_owner_lookup.py
─────────────────────────────────────────────────────────────────────────────
Weekly script that downloads the Florida Department of Revenue NAL (Name-
Address-Legal) property roll files for Sumter, Lake, and Marion counties,
then builds a single lookup CSV: data/owner_lookup.csv

This file is then used by main.py to automatically add owner names to any
removed/sold listings — no manual county website lookups needed.

Run schedule: Once per week (Sunday) via GitHub Actions.
─────────────────────────────────────────────────────────────────────────────
"""

import os
import io
import zipfile
import requests
import pandas as pd
from datetime import datetime

# ── Paths ──────────────────────────────────────────────────────────────────
folder_path = os.path.join(os.path.dirname(__file__), 'data')
os.makedirs(folder_path, exist_ok=True)
LOOKUP_FILE = os.path.join(folder_path, 'owner_lookup.csv')

# ── Florida DOR NAL file URL pattern ──────────────────────────────────────
# Files are named: "{County} {##} Final NAL {YEAR}.zip"
# Base URL for the current year's final NAL files:
# https://floridarevenue.com/property/dataportal/Documents/PTO Data Portal/Tax Roll Data Files/NAL/{YEAR}F/
#
# County codes assigned by Florida DOR:
#   Lake    = 35
#   Marion  = 42
#   Sumter  = 61

CURRENT_YEAR = datetime.now().year

# If today is before ~November (final rolls aren't posted yet), use last year.
# Final NAL rolls are posted October-December each year.
if datetime.now().month < 11:
    NAL_YEAR = CURRENT_YEAR - 1
else:
    NAL_YEAR = CURRENT_YEAR

BASE_URL = (
    "https://floridarevenue.com/property/dataportal/Documents/"
    "PTO%20Data%20Portal/Tax%20Roll%20Data%20Files/NAL/{year}F/"
    "{county_name}%20{county_code}%20Final%20NAL%20{year}.zip"
)

COUNTIES = [
    {"name": "Lake",   "code": "35"},
    {"name": "Marion", "code": "42"},
    {"name": "Sumter", "code": "61"},
]

# ── NAL column positions we care about ────────────────────────────────────
# Per Florida DOR NAL User Guide the key fields are:
#   CO_NO       = County number
#   PARCEL_ID   = Parcel ID
#   OWN_NAME    = Owner name (primary)
#   OWN_ADDR1   = Owner mailing address line 1
#   OWN_ADDR2   = Owner mailing address line 2
#   OWN_ADDR3   = Owner mailing address line 3
#   PHY_ADDR1   = Physical/situs address (the property address)
#   PHY_ADDR2   = Physical address line 2
#   PHY_CITY    = Physical city
#   PHY_ZIPCD   = Physical ZIP code

KEEP_COLS = [
    'CO_NO', 'PARCEL_ID',
    'OWN_NAME',
    'OWN_ADDR1', 'OWN_ADDR2', 'OWN_ADDR3',
    'PHY_ADDR1', 'PHY_ADDR2', 'PHY_CITY', 'PHY_ZIPCD',
]


def download_and_parse_nal(county_name, county_code, year):
    """Download a county NAL zip, extract the CSV, return a DataFrame."""
    url = BASE_URL.format(
        year=year,
        county_name=county_name,
        county_code=county_code
    )
    print(f"  [⬇️] Downloading {county_name} County NAL {year}...")
    print(f"       URL: {url}")

    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"  [⚠️] HTTP error for {county_name}: {e}")
        print(f"       The {year} final roll may not be posted yet. Trying {year - 1}...")
        fallback_url = BASE_URL.format(
            year=year - 1,
            county_name=county_name,
            county_code=county_code
        )
        resp = requests.get(fallback_url, timeout=120)
        resp.raise_for_status()
        print(f"  [✅] Fallback to {year - 1} succeeded.")

    # The zip contains one CSV file
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
        if not csv_files:
            raise ValueError(f"No CSV found in ZIP for {county_name}")
        csv_name = csv_files[0]
        print(f"  [📄] Parsing {csv_name} ({len(resp.content) / 1_000_000:.1f} MB download)...")
        with z.open(csv_name) as f:
            # NAL files are large — read only the columns we need
            df = pd.read_csv(
                f,
                dtype=str,          # keep everything as string (parcel IDs have leading zeros)
                low_memory=False,
                encoding='latin-1', # DOR files sometimes use latin-1
                on_bad_lines='skip'
            )

    print(f"  [✅] {county_name}: {len(df):,} parcels loaded, {len(df.columns)} columns")

    # Keep only columns that exist in this file
    available = [c for c in KEEP_COLS if c in df.columns]
    missing = [c for c in KEEP_COLS if c not in df.columns]
    if missing:
        print(f"  [⚠️] Columns not found in {county_name} NAL (will be blank): {missing}")

    df = df[available].copy()
    df['COUNTY'] = county_name

    # Build a clean physical address string for matching against VLS addresses
    df['FULL_PHY_ADDR'] = (
        df.get('PHY_ADDR1', '').fillna('').str.strip() + ' ' +
        df.get('PHY_ADDR2', '').fillna('').str.strip()
    ).str.strip().str.upper()

    df['PHY_CITY'] = df.get('PHY_CITY', '').fillna('').str.strip().str.upper()
    df['OWN_NAME'] = df.get('OWN_NAME', '').fillna('').str.strip()

    return df


def build_lookup(dfs):
    """Merge all county DataFrames into a single lookup table."""
    combined = pd.concat(dfs, ignore_index=True)

    # Normalize address for fuzzy matching later
    combined['FULL_PHY_ADDR'] = combined['FULL_PHY_ADDR'].str.upper().str.strip()

    # Drop rows with no owner name or no physical address (not useful for lookup)
    combined = combined[
        combined['OWN_NAME'].str.len() > 0
    ].reset_index(drop=True)

    print(f"\n[📊] Combined lookup table: {len(combined):,} parcels across {combined['COUNTY'].nunique()} counties")
    return combined


def main():
    print(f"[▶️] Owner lookup update started — targeting NAL year: {NAL_YEAR}")
    print(f"[📅] Run date: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    dfs = []
    for county in COUNTIES:
        try:
            df = download_and_parse_nal(county['name'], county['code'], NAL_YEAR)
            dfs.append(df)
        except Exception as e:
            print(f"  [❌] Failed to load {county['name']} County: {e}")
            print(f"       Skipping this county — lookup will work for the others.")

    if not dfs:
        print("[❌] No county data loaded. Exiting without saving.")
        return

    lookup = build_lookup(dfs)
    lookup.to_csv(LOOKUP_FILE, index=False, encoding='utf-8-sig')
    print(f"[💾] Saved owner_lookup.csv → {len(lookup):,} parcels")
    print(f"[✅] Done. main.py will use this file for owner name lookups.")


if __name__ == '__main__':
    main()
