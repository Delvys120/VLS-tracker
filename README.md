# Mount Google Drive
from google.colab import drive
drive.mount('/content/drive')

import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import re
import glob

# Set up folder path in Google Drive
folder_path = '/content/drive/MyDrive/VLS_Tracker'
tracking_file = os.path.join(folder_path, 'listing_first_seen.csv')

# Check files in the folder
print("📂 Files inside VLS_Tracker folder:")
print(os.listdir(folder_path))

# Function to check if removed listings are truly removed from VLS API
def check_removed_listings_against_vls(removed_listings, vls_data):
    # Extract ULIKeys from removed listings
    removed_ulikeys = removed_listings['ULIKey'].tolist()

    # Extract ULIKeys from current VLS data
    vls_ulikeys = [home.get("ULIKey") for home in vls_data]

    # Find ULIKeys that are not in the VLS API (those are the truly removed ones)
    truly_removed = [uli for uli in removed_ulikeys if uli not in vls_ulikeys]

    return truly_removed

# Fetch today's VLS data
print("🌐 Fetching today's VLS data...")
url = "https://api.thevillages.com/hf/search/allhomelisting"
response = requests.get(url)
data = response.json()

all_homes = data.get("HomeList", [])
print(f"✅ Total homes received: {len(all_homes)}")

# Filter PreOwned + Active listings
filtered_homes = [
    home for home in all_homes
    if home.get("SaleType") == "P" and home.get("ListingStatus") == "A"
]

print(f"🏡 Filtered PreOwned & Active homes: {len(filtered_homes)}")

# Save today's data
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
print(f"💾 Today's listings saved as {today_filename}")

# Find the latest VLS_YYYY-MM-DD.csv file (excluding today's file)
vls_files = [f for f in os.listdir(folder_path) if re.match(r'VLS_\d{4}-\d{2}-\d{2}\.csv', f)]
vls_files = [f for f in vls_files if f != today_filename]

if not vls_files:
    print("❌ No previous VLS files found.")
    df_previous = pd.DataFrame()  # Empty DataFrame fallback
else:
    # Sort files by date extracted from filename
    vls_files.sort(reverse=True)
    latest_file = vls_files[0]
    latest_full_path = os.path.join(folder_path, latest_file)

    df_previous = pd.read_csv(latest_full_path)
    print(f"✅ Latest previous VLS file loaded: {latest_file}")

# Continue if we have a previous file
if not df_previous.empty:
    # Only Active listings from previous file
    df_previous_active = df_previous[df_previous['Status'] == 'A']

    # Find homes missing today (removed)
    removed = df_previous_active[~df_previous_active['ULIKey'].isin(df_today['ULIKey'])]

    # Check if there are any removed listings
    if not removed.empty:
        # Cross-reference removed listings with current VLS data
        truly_removed = check_removed_listings_against_vls(removed, all_homes)

        # Create the truly removed listings file with the correct naming convention
        if truly_removed:
            truly_removed_df = removed[removed['ULIKey'].isin(truly_removed)]

            # Create the "VLS expired" filename with today's date as requested
            truly_removed_filename = f'VLS expired {today}.csv'
            truly_removed_full_path = os.path.join(folder_path, truly_removed_filename)

            truly_removed_df.to_csv(truly_removed_full_path, index=False, encoding="utf-8-sig")
            print(f"🗂️ Truly expired listings saved as {truly_removed_filename}")
            print(f"✅ Found {len(truly_removed)} truly expired listings")
        else:
            print("✅ No truly expired listings found today.")
    else:
        print("✅ No removed listings detected today!")
else:
    print("⚠️ Skipping comparison since no previous file was loaded.")

# ===== TRACKING LISTINGS FOR 5+ MONTHS =====
print("\n🕒 Tracking listings age...")

# Create or load the tracking file that records when each listing was first seen
if os.path.exists(tracking_file):
    df_tracking = pd.read_csv(tracking_file)
    print(f"✅ Loaded tracking data for {len(df_tracking)} listings")
else:
    df_tracking = pd.DataFrame(columns=['ULIKey', 'FirstSeen', 'Address', 'Village', 'Price', 'VLSNumber'])
    print("🆕 Created new tracking database")

# Get today's date for tracking purposes
today_date = datetime.now().date()

# Update tracking file with new listings
new_listings = []
for _, home in df_today.iterrows():
    if home['ULIKey'] not in df_tracking['ULIKey'].values:
        new_listings.append({
            'ULIKey': home['ULIKey'],
            'FirstSeen': today_date.strftime('%Y-%m-%d'),
            'Address': home['Address'],
            'Village': home['Village'],
            'Price': home['Price'],
            'VLSNumber': home['VLSNumber']
        })

if new_listings:
    df_new = pd.DataFrame(new_listings)
    df_tracking = pd.concat([df_tracking, df_new], ignore_index=True)
    print(f"➕ Added {len(new_listings)} new listings to tracking database")

# Calculate which listings have been on the market for 5+ months (approximately 150 days)
df_tracking['FirstSeen'] = pd.to_datetime(df_tracking['FirstSeen'])
five_months_ago = pd.to_datetime(today_date - timedelta(days=150))

# Filter for listings that:
# 1. Are still active (exist in today's data)
# 2. Have been on the market for 5+ months
active_ulikeys = df_today['ULIKey'].tolist()
aged_listings = df_tracking[
    (df_tracking['ULIKey'].isin(active_ulikeys)) &
    (df_tracking['FirstSeen'] <= five_months_ago)
]

# Save the tracking file (with new listings added)
df_tracking.to_csv(tracking_file, index=False, encoding='utf-8-sig')
print(f"💾 Updated tracking database with {len(df_tracking)} total listings")

# Save the 5+ month listings if any exist
if not aged_listings.empty:
    # Merge with today's data to get the most current information
    current_aged = df_today[df_today['ULIKey'].isin(aged_listings['ULIKey'])]

    # Add "days on market" column to the output
    current_aged = current_aged.merge(
        df_tracking[['ULIKey', 'FirstSeen']],
        on='ULIKey',
        how='left'
    )

    # Calculate days on market
    current_aged['DaysOnMarket'] = (today_date - current_aged['FirstSeen'].dt.date).dt.days

    # Sort by days on market (descending)
    current_aged = current_aged.sort_values(by='DaysOnMarket', ascending=False)

    # Save the file
    aged_filename = f'5 Month Listings {today}.csv'
    aged_full_path = os.path.join(folder_path, aged_filename)
    current_aged.to_csv(aged_full_path, index=False, encoding='utf-8-sig')
    print(f"🏠 Found {len(current_aged)} listings that have been on the market for 5+ months")
    print(f"📊 Saved as '{aged_filename}'")
else:
    print("✅ No listings have been on the market for 5+ months")
