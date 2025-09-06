import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import os
import smtplib
from email.message import EmailMessage
import pandas as pd
import hashlib

# === CONFIGURATION ===
URL = "https://www.onemodel.co/roles-in-people-analytics-hr-technology"

STORAGE_DIR = "."
LAST_DATE_FILE = "last_update_date.txt"
LAST_HASH_FILE = "last_data_hash.txt"
COMBINED_CSV_FILE = "latest_combined.csv"
EMAIL_NOTIFICATION = True  # Change to True if you want email alerts

# Email settings (if EMAIL_NOTIFICATION is True)
EMAIL_SENDER = os.getenv("GMAIL_USERNAME")  # Enter your address
EMAIL_RECEIVER = "eli.jaffe@nyu.edu"  # Enter receiver address
EMAIL_SUBJECT = "OneModel Page Updated"
EMAIL_PASSWORD = os.getenv("GMAIL_PASSWORD")
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587


def fetch_update_date(soup):

    match = re.search(r"Last update:\s*(\d{1,2}/\d{1,2}/\d{2})", soup.text)
    # match = re.search(r'update: (\d{1,2}+ \d{1,2}, \d{4})', soup.text)
    if match:
        return datetime.strptime(match.group(1), "%m/%d/%y").date()
    return None


def extract_and_combine_tables(soup):
    tables = soup.find_all("table")
    columns = ['Date', 'Loc.', 'Title', 'Company', 'Location', 'Link'] # , 'Level']

    data = pd.DataFrame(columns=columns + ['Level'])

    for i, table in enumerate(tables):
        # Find the nearest heading/title
        title = f"Untitled Table {i+1}"
        for prev in table.find_all_previous():
            if prev.name in ["h1", "h2", "h3", "h4", "strong", "p"]:
                text = prev.get_text(strip=True)
                if text:
                    title = text
                    break

        try:
            df = pd.read_html(str(table), header=None)[0]
            df.columns = columns  # Use first row as header
            df = df.drop(index=0).reset_index(drop=True)
            df["Level"] = title
            data = pd.concat([data, df])

        except Exception as e:
            print(f"⚠️ Failed to parse table {i+1}: {e}")

    return data


def load_previous_state():
    if os.path.exists(LAST_DATE_FILE):
        with open(LAST_DATE_FILE, "r") as f:
            last_date = datetime.strptime(f.read().strip(), "%Y-%m-%d").date()
    else:
        last_date = None

    if os.path.exists(LAST_HASH_FILE):
        with open(LAST_HASH_FILE, "r") as f:
            last_hash = f.read().strip()
    else:
        last_hash = None

    return last_date, last_hash


def save_current_state(update_date, data_df):
    if not os.path.exists(STORAGE_DIR):
        os.makedirs(STORAGE_DIR)

    # Save update date
    with open(LAST_DATE_FILE, "w") as f:
        f.write(update_date.strftime("%Y-%m-%d"))

    # Save combined table
    data_df.to_csv(COMBINED_CSV_FILE, index=False)

    # Save hash of table
    data_hash = hashlib.md5(pd.util.hash_pandas_object(data_df, index=True).values).hexdigest()
    with open(LAST_HASH_FILE, "w") as f:
        f.write(data_hash)
      

def send_email_alert(message_body):
    msg = EmailMessage()
    msg.set_content(message_body)
    msg["Subject"] = "📊 OneModel Page Update Detected"
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
    print("📧 Email alert sent!")


def main():
    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

    print(f"Job started at: {formatted_time}")
    print("🔍 Checking OneModel roles page...")
    response = requests.get(URL)
    soup = BeautifulSoup(response.text, "html.parser")

    latest_update_date = fetch_update_date(soup)
    combined_df = extract_and_combine_tables(soup)

    if combined_df.empty:
        print("❌ No tables found. Aborting.")
        return

    prev_date, prev_hash = load_previous_state()
    current_hash = hashlib.md5(pd.util.hash_pandas_object(combined_df, index=True).values).hexdigest()

    update_detected = False
    messages = []

    # Check for update date change
    if latest_update_date and latest_update_date != prev_date:
        update_detected = True
        messages.append(f"🗓️ Page update date changed: {prev_date} → {latest_update_date}")

    # Check for data change
    if current_hash != prev_hash:
        update_detected = True
        messages.append("📈 Table content has changed since last check.")

    if update_detected:
        print("✅ Changes detected! Saving and notifying...")
        save_current_state(latest_update_date, combined_df)
        if EMAIL_NOTIFICATION:
            send_email_alert("\n".join(messages))
    else:
        print("✅ No changes detected.")

    print(f'Job finished at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')


if __name__ == "__main__":
    main()
