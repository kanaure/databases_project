import os
import pandas as pd
import re
from sqlalchemy import text
from config import engine

# Paths
data_folder_path = os.path.join(os.path.dirname(__file__), 'data', 'dataset')
app_folder_path = os.path.join(data_folder_path, 'app_usage')

def extract_uid(filename):
    match = re.search(r'u\d+', filename)
    return match.group() if match else None

# Column rename map: CSV -> DB
COL_RENAME = {
    "id":                                "record_id",
    "RUNNING_TASKS_baseActivity_mClass":  "running_tasks_base_activity_mclass",
    "RUNNING_TASKS_baseActivity_mPackage":"running_tasks_base_activity_mpackage",
    "RUNNING_TASKS_id":                  "running_tasks_id",
    "RUNNING_TASKS_numActivities":       "running_tasks_num_activities",
    "RUNNING_TASKS_numRunning":          "running_tasks_num_running",
    "RUNNING_TASKS_topActivity_mClass":  "running_tasks_top_activity_mclass",
    "RUNNING_TASKS_topActivity_mPackage":"running_tasks_top_activity_mpackage",
}

APP_USAGE_KEY = ["uid", "timestamp", "running_tasks_id"]

app_frames = []
for filename in os.listdir(app_folder_path):
    if not filename.endswith(".csv"):
        continue
    df = pd.read_csv(os.path.join(app_folder_path, filename))
    df.columns = df.columns.str.strip()
    df["uid"] = extract_uid(filename)
    df = df.rename(columns=COL_RENAME)
    app_frames.append(df)

if not app_frames:
    print("No app usage files found.")
else:
    app_df = pd.concat(app_frames, ignore_index=True)

    # Fetch existing keys from DB
    with engine.connect() as conn:
        result = conn.execute(text("SELECT uid, timestamp, running_tasks_id FROM app_usage"))
        existing_keys = set(tuple(row) for row in result)

    # Filter to only new rows
    mask = ~app_df[APP_USAGE_KEY].apply(tuple, axis=1).isin(existing_keys)
    new_rows = app_df[mask]

    if new_rows.empty:
        print("No new app usage rows to insert.")
    else:
        print(f"Inserting {len(new_rows)} new rows (skipping {len(app_df) - len(new_rows)} duplicates).")
        new_rows.to_sql("app_usage", engine, if_exists="append", index=False)