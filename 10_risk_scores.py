import os
import math
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import text
from config import engine

BATCH_SIZE = 500

def to_day_timestamp(unix_ts):
    """Floor a unix timestamp to the start of its UTC day."""
    dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
    return int(datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc).timestamp())

def batched_insert(rows: list[dict], table: str):
    """Insert a list of dicts in batches."""
    if not rows:
        print(f"[{table}] Nothing to insert.")
        return
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i:i + BATCH_SIZE]
        with engine.begin() as conn:
            conn.execute(
                text(f"""
                    INSERT INTO {table} (uid, day_timestamp, sleep_risk, activity_risk, conversation_risk, overall_risk)
                    VALUES (:uid, :day_timestamp, :sleep_risk, :activity_risk, :conversation_risk, :overall_risk)
                    ON CONFLICT (uid, day_timestamp) DO NOTHING
                """),
                chunk
            )
    print(f"[{table}] Inserted up to {len(rows)} rows.")

# ── Create table if it doesn't exist ─────────────────────────────────────────
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS daily_risk (
            risk_id        SERIAL PRIMARY KEY,
            uid            VARCHAR REFERENCES student(uid) NOT NULL,
            day_timestamp  BIGINT NOT NULL,
            sleep_risk     FLOAT,
            activity_risk  FLOAT,
            conversation_risk FLOAT,
            overall_risk   FLOAT,
            UNIQUE (uid, day_timestamp)
        )
    """))
print("Table daily_risk ready.")
# ── Fetch existing (uid, day) pairs already in risk table ─────────────────────
with engine.connect() as conn:
    existing = set(
        (r[0], r[1]) for r in conn.execute(text("SELECT uid, day_timestamp FROM daily_risk"))
    )
print(f"Found {len(existing)} existing risk rows.")


# ── 1. SLEEP RISK (from dark_period) ─────────────────────────────────────────
# sleep duration = end - start (seconds). Thresholds: >=7h low, >=6h medium, <6h high
SLEEP_LOW    = 7 * 3600
SLEEP_MEDIUM = 6 * 3600

with engine.connect() as conn:
    dark_rows = conn.execute(text("SELECT uid, start, \"end\" FROM dark_period")).fetchall()

sleep_risk = {}  # (uid, day_ts) -> risk score
for uid, start, end in dark_rows:
    duration = end - start
    day_ts = to_day_timestamp(start)
    if duration >= SLEEP_LOW:
        score = 0.0
    elif duration >= SLEEP_MEDIUM:
        score = 0.5
    else:
        score = 1.0
    sleep_risk[(uid, day_ts)] = score

print(f"[sleep] Computed {len(sleep_risk)} uid/day pairs.")


# ── 2. ACTIVITY RISK (from activity_reading) ─────────────────────────────────
# Split each day into 10-min intervals. Interval active if >=50% readings are active (1/2/3).
# Thresholds: >=3 active intervals -> low, >=2 -> medium, else high.
INTERVAL_SECS   = 10 * 60
ACTIVE_THRESHOLD = 3
MEDIUM_THRESHOLD = 2

with engine.connect() as conn:
    act_rows = conn.execute(
        text("SELECT uid, activity_inference, timestamp FROM activity_reading")
    ).fetchall()

# Group readings into (uid, day_ts, interval_bucket)
from collections import defaultdict
interval_readings = defaultdict(list)  # (uid, day_ts, bucket) -> [0/1]
for uid, inference, ts in act_rows:
    day_ts = to_day_timestamp(ts)
    bucket = (ts - day_ts) // INTERVAL_SECS
    interval_readings[(uid, day_ts, bucket)].append(1 if inference in (1, 2, 3) else 0)

# Count active intervals per (uid, day)
active_intervals = defaultdict(int)
for (uid, day_ts, _bucket), readings in interval_readings.items():
    if sum(readings) / len(readings) >= 0.5:
        active_intervals[(uid, day_ts)] += 1

# Compute risk scores
activity_risk = {}
for (uid, day_ts), count in active_intervals.items():
    if count >= ACTIVE_THRESHOLD:
        activity_risk[(uid, day_ts)] = 0.0
    elif count >= MEDIUM_THRESHOLD:
        activity_risk[(uid, day_ts)] = 0.5
    else:
        activity_risk[(uid, day_ts)] = 1.0

# Also capture pairs that exist in activity_reading but had zero active intervals
for uid, _inference, ts in act_rows:
    day_ts = to_day_timestamp(ts)
    if (uid, day_ts) not in activity_risk:
        activity_risk[(uid, day_ts)] = 1.0  # zero active intervals = high risk

print(f"[activity] Computed {len(activity_risk)} uid/day pairs.")


# ── 3. CONVERSATION RISK (from conversation) ─────────────────────────────────
# Total talk minutes per (uid, day). >=60 min low, >=30 medium, <30 high.
CONV_LOW    = 60
CONV_MEDIUM = 30

with engine.connect() as conn:
    conv_rows = conn.execute(
        text("SELECT uid, start_timestamp, end_timestamp FROM conversation")
    ).fetchall()

talk_minutes = defaultdict(float)
for uid, start, end in conv_rows:
    day_ts = to_day_timestamp(start)
    talk_minutes[(uid, day_ts)] += (end - start) / 60.0

conversation_risk = {}
for (uid, day_ts), minutes in talk_minutes.items():
    if minutes >= CONV_LOW:
        conversation_risk[(uid, day_ts)] = 0.0
    elif minutes >= CONV_MEDIUM:
        conversation_risk[(uid, day_ts)] = 0.5
    else:
        conversation_risk[(uid, day_ts)] = 1.0

print(f"[conversation] Computed {len(conversation_risk)} uid/day pairs.")


# ── 4. Assemble & insert ──────────────────────────────────────────────────────
# Union of all (uid, day) keys across all three sources
all_keys = set(sleep_risk) | set(activity_risk) | set(conversation_risk)
print(f"Total unique uid/day pairs across all sources: {len(all_keys)}")

rows_to_insert = []
for (uid, day_ts) in all_keys:
    if (uid, day_ts) in existing:
        continue  # skip already-inserted rows

    s = sleep_risk.get((uid, day_ts))
    a = activity_risk.get((uid, day_ts))
    c = conversation_risk.get((uid, day_ts))

    # Overall risk: only computed if all three subscores are present
    if s is not None and a is not None and c is not None:
        overall = round(s * 0.4 + c * 0.35 + a * 0.25, 4)
    else:
        overall = None

    rows_to_insert.append({
        "uid":               uid,
        "day_timestamp":     day_ts,
        "sleep_risk":        s,
        "activity_risk":     a,
        "conversation_risk": c,
        "overall_risk":      overall,
    })

print(f"Inserting {len(rows_to_insert)} new rows (skipping {len(existing)} existing).")
batched_insert(rows_to_insert, "daily_risk")