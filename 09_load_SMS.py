import os
import pandas as pd
import re
from sqlalchemy import text
from config import engine

# Paths
data_folder_path = os.path.join(os.path.dirname(__file__), 'data', 'dataset')
sms_folder_path = os.path.join(data_folder_path, 'sms')

def extract_uid(filename):
    match = re.search(r'u\d+', filename)
    return match.group() if match else None

# Column rename map: CSV -> DB
COL_RENAME = {
    "id":                          "record_id",
    "MESSAGES_address":            "messages_address",
    "MESSAGES_body":               "messages_body",
    "MESSAGES_date":               "messages_date",
    "MESSAGES_locked":             "messages_locked",
    "MESSAGES_person":             "messages_person",
    "MESSAGES_protocol":           "messages_protocol",
    "MESSAGES_read":               "messages_read",
    "MESSAGES_reply_path_present": "messages_reply_path_present",
    "MESSAGES_status":             "messages_status",
    "MESSAGES_subject":            "messages_subject",
    "MESSAGES_thread_id":          "messages_thread_id",
    "MESSAGES_type":               "messages_type",
    # MESSAGES_service_center is not in the DB model, will be dropped automatically
}

SMS_KEY = ["uid", "timestamp", "record_id"]

sms_frames = []
for filename in os.listdir(sms_folder_path):
    if not filename.endswith(".csv"):
        continue
    df = pd.read_csv(os.path.join(sms_folder_path, filename))
    df.columns = df.columns.str.strip()
    df["uid"] = extract_uid(filename)
    df = df.rename(columns=COL_RENAME)
    sms_frames.append(df)

if not sms_frames:
    print("No SMS files found.")
else:
    sms_df = pd.concat(sms_frames, ignore_index=True)

    # Drop columns not in DB model (e.g. MESSAGES_service_center)
    db_cols = [
        "uid", "record_id", "device", "timestamp",
        "messages_address", "messages_body", "messages_date",
        "messages_locked", "messages_person", "messages_protocol",
        "messages_read", "messages_reply_path_present", "messages_status",
        "messages_subject", "messages_thread_id", "messages_type",
    ]
    sms_df = sms_df[[c for c in db_cols if c in sms_df.columns]]

    # Cast boolean columns to int (0/1) for PostgreSQL Integer columns
    bool_cols = ["messages_locked", "messages_read", "messages_reply_path_present"]
    for col in bool_cols:
        if col in sms_df.columns:
            sms_df[col] = sms_df[col].map({True: 1, False: 0, "True": 1, "False": 0}).astype("Int64")

    # Fetch existing keys from DB
    with engine.connect() as conn:
        result = conn.execute(text("SELECT uid, timestamp, record_id FROM sms"))
        existing_keys = set(tuple(row) for row in result)

    # Filter to only new rows
    mask = ~sms_df[SMS_KEY].apply(tuple, axis=1).isin(existing_keys)
    new_rows = sms_df[mask]

    if new_rows.empty:
        print("No new SMS rows to insert.")
    else:
        print(f"Inserting {len(new_rows)} new rows (skipping {len(sms_df) - len(new_rows)} duplicates).")
        new_rows.to_sql("sms", engine, if_exists="append", index=False)