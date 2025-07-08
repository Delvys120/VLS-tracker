import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import re
import smtplib
from email.message import EmailMessage

# Set up folder path
folder_path = os.path.join(os.path.dirname(__file__), 'data')
os.makedirs(folder_path, exist_ok=True)

tracking_file = os.path.join(folder_path, 'listing_first_seen.csv')

def check_removed_listings_against_vls(removed_listings, vls_data):
    removed_ulikeys = removed_listings['ULIKey'].tolist()
    vls_ulikeys = [home.get("ULIKey") for home in vls_data]
    truly_removed = [uli for uli in removed_ulikeys if uli not in vls_ulikeys]
    return truly_removed

def send_email_with_attachments(subject, body, attachments):
    msg = EmailMessage()
    msg["From"] = os.getenv("GMAIL_USER")
    msg["To"] = os.getenv("EMAIL_RECIPIENT")
    msg["Subject"] = subject
    msg.set_content(body)

    for file_path in attachments:
        with open(file_path, "rb") as f:
            file_data = f.read()
            file_name = os.path.basename(file_path)
        msg.add_attachment(file_data, maintype="application", subtype="octet-stream", filename=file_name)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(os.getenv("GMAIL_USER"), os.getenv("GMAIL_APP_PASSWORD"))
        smtp.send_message(msg)

def main():
    print("[▶️] Script started")

    # Fetch today's VLS data
    url = "https://api.thevillages.com/hf/search/allhomelisting"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    all_homes = data.get("HomeList", [])
    print(f"[✅] Total homes received: {len(all_homes)}")

    filtered_homes = [
        home for home in all_homes
        if home.get("SaleType") == "P" and home.get("ListingStatus") == "A"
    ]
    print(f"[🏡] Filtered PreOwned & Active homes: {len(filtered_homes)}")

    today = datetime.now().strftime('%Y-%m-%d')
    today_filename = f'VLS_{today}.csv'
    today_full_path = os.path.join(folder_path, today_filename)

    df_today = pd.DataFrame([{
        "ULIKey": home.get("ULIKey"),
        "Address": home.get("Address"),
        "Village": home.get("Village"),
        "County": home.get("County"),
        "Model": home.get("Model"),
        "Price": home.get("Price").replace("$", "").replace(",", "") if home.get("Price") else "",
        "Bedrooms": home.get("Bedrooms"),
        "Baths": home.get("Baths"),
        "SquareFeet": home.get("SquareFeet"),
        "Garage": home.get("Garage"),
        "Pool": home.get("Pool"),
        "Latitude": home.get("GISLat"),
        "Longitude": home.get("GISLong"),
        "Status": home.get("ListingStatus"),
        "SaleType": home.get("SaleType"),
        "YouTubeVideoId": home.get("YouTubeVideoId"),
        "VLSNumber": home.get("VLSNumber")
    } for home in filtered_homes])

    df_today.to_csv(today_full_path, index=False, encoding='utf-8-sig')
    print(f"[💾] Today's listings saved as {today_filename}")

    vls_files = [f for f in os.listdir(folder_path) if re.match(r'VLS_\d{4}-\d{2}-\d{2}\.csv', f)]
    vls_files = [f for f in vls_files if f != today_filename]

    if not vls_files:
        print("[❌] No previous VLS files found.")
        df_previous = pd.DataFrame()
    else:
        vls_files.sort(reverse=True)
        latest_file = vls_files[0]
        latest_full_path = os.path.join(folder_path, latest_file)
        df_previous = pd.read_csv(latest_full_path)
        print(f"[✅] Latest previous VLS file loaded: {latest_file}")

    expired_files_to_send = []

    if not df_previous.empty:
        df_previous_active = df_previous[df_previous['Status'] == 'A']
        removed = df_previous_active[~df_previous_active['ULIKey'].isin(df_today['ULIKey'])]

        if not removed.empty:
            truly_removed = check_removed_listings_against_vls(removed, all_homes)
            if truly_removed:
                truly_removed_df = removed[removed['ULIKey'].isin(truly_removed)]
                expired_filename = f'VLS expired {today}.csv'
                expired_full_path = os.path.join(folder_path, expired_filename)
                truly_removed_df.to_csv(expired_full_path, index=False, encoding='utf-8-sig')
                print(f"[📂] Truly expired listings saved as {expired_filename}")
                expired_files_to_send.append(expired_full_path)
            else:
                print("[✅] No truly expired listings found today.")
        else:
            print("[✅] No removed listings detected today!")
    else:
        print("[⚠️] No previous file loaded, skipping removal check.")

    print("[🕒] Tracking listings age...")

    if os.path.exists(tracking_file):
        df_tracking = pd.read_csv(tracking_file)
        print(f"[✅] Loaded tracking data for {len(df_tracking)} listings")
    else:
        df_tracking = pd.DataFrame(columns=['ULIKey', 'FirstSeen', 'Address', 'Village', 'Price', 'VLSNumber', 'DaysOnMarket'])
        print("[🆕] Created new tracking database")

    today_date = datetime.now().date()

    new_listings = []
    for _, home in df_today.iterrows():
        if home['ULIKey'] not in df_tracking['ULIKey'].values:
            new_listings.append({
                'ULIKey': home['ULIKey'],
                'FirstSeen': today_date.strftime('%Y-%m-%d'),
                'Address': home['Address'],
                'Village': home['Village'],
                'Price': home['Price'],
                'VLSNumber': home['VLSNumber'],
                'DaysOnMarket': 0
            })

    if new_listings:
        df_tracking = pd.concat([df_tracking, pd.DataFrame(new_listings)], ignore_index=True)
        print(f"[➕] Added {len(new_listings)} new listings to tracking database")

    # Ensure FirstSeen is datetime
    df_tracking['FirstSeen'] = pd.to_datetime(df_tracking['FirstSeen'])
    df_tracking['DaysOnMarket'] = (today_date - df_tracking['FirstSeen'].dt.date).dt.days

    five_months_ago = pd.to_datetime(today_date - timedelta(days=150))

    active_ulikeys = df_today['ULIKey'].tolist()
    aged_listings = df_tracking[
        (df_tracking['ULIKey'].isin(active_ulikeys)) &
        (df_tracking['FirstSeen'] <= five_months_ago)
    ]

    df_tracking.to_csv(tracking_file, index=False, encoding='utf-8-sig')
    print(f"[💾] Updated tracking database with {len(df_tracking)} total listings")

    # Sort tracking data by DaysOnMarket descending for output email
    df_tracking_sorted = df_tracking.sort_values(by='DaysOnMarket', ascending=False)
    df_tracking_sorted.to_csv(tracking_file, index=False, encoding='utf-8-sig')

    # Prepare attachments for email
    attachments = [today_full_path, tracking_file] + expired_files_to_send

    # Compose email body
    body = f"""
[▶️] Script started
[✅] Total homes received: {len(all_homes)}
[🏡] Filtered PreOwned & Active homes: {len(filtered_homes)}
[💾] Today's listings saved as {today_filename}
"""

    if expired_files_to_send:
        body += f"[📂] Truly expired listings saved and attached.\n"

    body += f"[🕒] Tracking listings age...\n"
    body += f"[💾] Updated tracking database with {len(df_tracking)} total listings\n"

    aged_count = len(aged_listings)
    if aged_count > 0:
        body += f"[✅] Listings on market 5+ months: {aged_count}\n"
    else:
        body += f"[✅] No listings have been on the market for 5+ months\n"

    # Send the email
    send_email_with_attachments(f"VLS Tracker Report - {today}", body, attachments)

if __name__ == '__main__':
    main()
