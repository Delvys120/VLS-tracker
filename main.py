import os
import pandas as pd
import requests
from datetime import datetime
import re
import smtplib
from email.message import EmailMessage
from email.utils import formataddr

# ─────────────────────────────────────────────
# Email config — loaded from GitHub Secrets
# ─────────────────────────────────────────────
EMAIL_ADDRESS = os.getenv('EMAIL_ADDRESS')   # matches yml: EMAIL_ADDRESS
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD') # matches yml: EMAIL_PASSWORD
EMAIL_TO = os.getenv('EMAIL_TO')             # matches yml: EMAIL_TO

if not EMAIL_ADDRESS or not EMAIL_PASSWORD or not EMAIL_TO:
    raise ValueError("❌ Missing EMAIL_ADDRESS, EMAIL_PASSWORD, or EMAIL_TO in GitHub secrets.")

# ─────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────
folder_path = os.path.join(os.path.dirname(__file__), 'data')
os.makedirs(folder_path, exist_ok=True)

tracking_file = os.path.join(folder_path, 'listing_first_seen.csv')

SNAPSHOT_PATTERN = re.compile(r"^VLS_(\d{4}-\d{2}-\d{2})\.csv$")

# ─────────────────────────────────────────────
# Columns to save in daily snapshot
# ─────────────────────────────────────────────
LISTING_COLUMNS = [
    "ULIKey",
    "Address",
    "Village",
    "County",
    "Model",
    "Price",
    "Bedrooms",
    "Baths",
    "SquareFeet",
    "Garage",
    "Pool",
    "Latitude",
    "Longitude",
    "Status",
    "SaleType",
    "YouTubeVideoId",
    "VLSNumber",
]


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def find_latest_snapshot(folder, today_str):
    """Return (filepath, date_str) of the most recent snapshot before today."""
    latest_date = None
    latest_file = None
    for filename in os.listdir(folder):
        match = SNAPSHOT_PATTERN.match(filename)
        if not match:
            continue
        snapshot_date = match.group(1)
        if snapshot_date >= today_str:
            continue
        if latest_date is None or snapshot_date > latest_date:
            latest_date = snapshot_date
            latest_file = os.path.join(folder, filename)
    return latest_file, latest_date


def check_removed_listings_against_vls(removed_df, all_homes):
    """
    Double-check: confirm a listing is truly gone from the full API response
    (not just filtered out). A home that went pending or under contract may
    still appear in all_homes but with a different status — we want those too.
    This catches edge cases where our filter excludes something temporarily.
    """
    all_ulikeys = {home.get("ULIKey") for home in all_homes}
    truly_removed_mask = ~removed_df['ULIKey'].isin(all_ulikeys)
    return removed_df[truly_removed_mask]


def send_email_with_attachments(subject, body, attachments):
    """Send email via Gmail SMTP with CSV attachments."""
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = formataddr(('VLS Tracker Bot', EMAIL_ADDRESS))
    msg['To'] = EMAIL_TO
    msg.set_content(body)

    for file_path in attachments:
        if not os.path.exists(file_path):
            print(f"[⚠️] Attachment not found, skipping: {file_path}")
            continue
        with open(file_path, 'rb') as f:
            file_data = f.read()
        file_name = os.path.basename(file_path)
        msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    print("[▶️] Script started")

    # ── 1. Fetch today's VLS data ──────────────────────────────────────────
    url = "https://api.thevillages.com/hf/search/allhomelisting"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    all_homes = data.get("HomeList", [])
    print(f"[✅] Total homes received from API: {len(all_homes)}")

    filtered_homes = [
        home for home in all_homes
        if home.get("SaleType") == "P" and home.get("ListingStatus") == "A"
    ]
    print(f"[🏡] Filtered PreOwned & Active homes: {len(filtered_homes)}")

    # ── 2. Build today's DataFrame ─────────────────────────────────────────
    today = datetime.now().strftime('%Y-%m-%d')
    today_date = datetime.now().date()

    df_today = pd.DataFrame([{
        "ULIKey":        home.get("ULIKey"),
        "Address":       home.get("Address"),
        "Village":       home.get("Village"),
        "County":        home.get("County"),
        "Model":         home.get("Model"),
        "Price":         home.get("Price", "").replace("$", "").replace(",", "") if home.get("Price") else "",
        "Bedrooms":      home.get("Bedrooms"),
        "Baths":         home.get("Baths"),
        "SquareFeet":    home.get("SquareFeet"),
        "Garage":        home.get("Garage"),
        "Pool":          home.get("Pool"),
        "Latitude":      home.get("GISLat"),
        "Longitude":     home.get("GISLong"),
        "Status":        home.get("ListingStatus"),
        "SaleType":      home.get("SaleType"),
        "YouTubeVideoId":home.get("YouTubeVideoId"),
        "VLSNumber":     home.get("VLSNumber"),
    } for home in filtered_homes], columns=LISTING_COLUMNS)

    # ── 3. Save today's snapshot ───────────────────────────────────────────
    today_filename = f'VLS_{today}.csv'
    today_full_path = os.path.join(folder_path, today_filename)
    df_today.to_csv(today_full_path, index=False, encoding='utf-8-sig')
    print(f"[💾] Today's snapshot saved: {today_filename}")

    # ── 4. Load previous snapshot ──────────────────────────────────────────
    previous_snapshot_path, previous_snapshot_date = find_latest_snapshot(folder_path, today)
    is_first_run = previous_snapshot_path is None

    if is_first_run:
        print("[⚠️] No prior snapshot found — this is a baseline run. Skipping removal check.")
        df_previous = pd.DataFrame(columns=LISTING_COLUMNS)
    else:
        df_previous = pd.read_csv(previous_snapshot_path)
        print(f"[✅] Previous snapshot loaded: VLS_{previous_snapshot_date}.csv ({len(df_previous)} listings)")

    # ── 5. Detect removed listings ─────────────────────────────────────────
    removed_filename = f'VLS_removed_{today}.csv'
    removed_full_path = os.path.join(folder_path, removed_filename)
    expired_count = 0

    if not is_first_run:
        df_previous_active = df_previous[df_previous['Status'] == 'A']
        removed_candidates = df_previous_active[~df_previous_active['ULIKey'].isin(df_today['ULIKey'])]

        if not removed_candidates.empty:
            truly_removed_df = check_removed_listings_against_vls(removed_candidates, all_homes)
            expired_count = len(truly_removed_df)
            truly_removed_df.to_csv(removed_full_path, index=False, encoding='utf-8-sig')
            print(f"[📂] {expired_count} removed listing(s) saved: {removed_filename}")
        else:
            print("[✅] No removed listings detected today.")
            pd.DataFrame(columns=LISTING_COLUMNS).to_csv(removed_full_path, index=False, encoding='utf-8-sig')
    else:
        # Write empty file so attachment always exists
        pd.DataFrame(columns=LISTING_COLUMNS).to_csv(removed_full_path, index=False, encoding='utf-8-sig')

    # ── 6. Update listing age tracking ────────────────────────────────────
    print("[🕒] Updating listing age tracker...")

    if os.path.exists(tracking_file):
        df_tracking = pd.read_csv(tracking_file)
        print(f"[✅] Loaded tracking data: {len(df_tracking)} listings")
    else:
        df_tracking = pd.DataFrame(columns=['ULIKey', 'FirstSeen', 'Address', 'Village', 'Price', 'VLSNumber'])
        print("[🆕] No tracking file found — creating fresh database")

    # Add brand-new listings
    existing_ulikeys = set(df_tracking['ULIKey'].values)
    new_listings = []
    for _, home in df_today.iterrows():
        if home['ULIKey'] not in existing_ulikeys:
            new_listings.append({
                'ULIKey':    home['ULIKey'],
                'FirstSeen': today,
                'Address':   home['Address'],
                'Village':   home['Village'],
                'Price':     home['Price'],
                'VLSNumber': home['VLSNumber'],
            })

    if new_listings:
        df_tracking = pd.concat([df_tracking, pd.DataFrame(new_listings)], ignore_index=True)
        print(f"[➕] Added {len(new_listings)} new listing(s) to tracker")
    else:
        print("[✅] No new listings to add to tracker")

    # Calculate days on market
    df_tracking['FirstSeen'] = pd.to_datetime(df_tracking['FirstSeen'], errors='coerce')
    df_tracking['DaysOnMarket'] = df_tracking['FirstSeen'].apply(
        lambda d: (today_date - d.date()).days if pd.notna(d) else None
    )

    df_tracking.to_csv(tracking_file, index=False, encoding='utf-8-sig')
    print(f"[💾] Tracking database saved: {len(df_tracking)} total listings")

    # ── 7. Build 5-month aged listings report ─────────────────────────────
    active_ulikeys = set(df_today['ULIKey'].tolist())
    aged_listings = df_tracking[
        (df_tracking['ULIKey'].isin(active_ulikeys)) &
        (df_tracking['DaysOnMarket'] >= 150)
    ].sort_values(by='DaysOnMarket', ascending=False)

    aged_filename = f'VLS_5month_{today}.csv'
    aged_full_path = os.path.join(folder_path, aged_filename)
    aged_listings.to_csv(aged_full_path, index=False, encoding='utf-8-sig')

    if not aged_listings.empty:
        print(f"[🏠] {len(aged_listings)} listing(s) on market 5+ months → {aged_filename}")
    else:
        print("[✅] No listings on market 5+ months")

    # ── 8. Send email ──────────────────────────────────────────────────────
    email_subject = f"VLS Tracker Report — {today}"

    if is_first_run:
        email_body = (
            f"🆕 BASELINE RUN — no previous snapshot existed to compare against.\n\n"
            f"Today's snapshot has been saved as {today_filename}.\n"
            f"Tomorrow's run will begin detecting removed/sold listings.\n\n"
            f"─────────────────────────────\n"
            f"Total homes from API:          {len(all_homes)}\n"
            f"PreOwned & Active filtered:    {len(filtered_homes)}\n"
            f"Tracking DB initialized with:  {len(df_tracking)} listings\n"
        )
    else:
        email_body = (
            f"Daily VLS Tracker run complete.\n\n"
            f"─────────────────────────────\n"
            f"Total homes from API:          {len(all_homes)}\n"
            f"PreOwned & Active filtered:    {len(filtered_homes)}\n"
            f"New listings added to tracker: {len(new_listings)}\n"
            f"Total tracked listings:        {len(df_tracking)}\n"
            f"Removed (sold/expired) today:  {expired_count}\n"
            f"Listings on market 5+ months:  {len(aged_listings)}\n"
            f"─────────────────────────────\n\n"
            f"Attachments:\n"
            f"  • {removed_filename} — listings no longer on VLS\n"
            f"  • {aged_filename}    — listings active 150+ days\n"
        )

    attachments = [removed_full_path, aged_full_path]
    send_email_with_attachments(email_subject, email_body, attachments)
    print("[✉️] Email sent successfully.")
    print("[✅] Script complete.")


if __name__ == '__main__':
    main()
