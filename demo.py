import sqlite3
import pandas as pd

SRC_DB = "datamoney.db"           # Your full DB
DST_DB = "datamoney_demo.db"      # Demo DB to create
MAX_SIZE_MB = 100                 # Target size for demo DB

# Connect to source DB
src_conn = sqlite3.connect(SRC_DB)
dst_conn = sqlite3.connect(DST_DB)

# 1. Determine cutoff date so the demo DB stays under ~100 MB
# We'll start by keeping only 2024 and 2025 data, drop 2023.
# If still too big, we'll keep only the most recent few months
dates_df = pd.read_sql("SELECT DISTINCT date FROM streams", src_conn)
dates_df['date_dt'] = pd.to_datetime(dates_df['date'], format="%m/%d/%Y")
dates_df = dates_df.sort_values('date_dt', ascending=False)

# Start with the most recent 12 months
cutoff_date = dates_df['date_dt'].max() - pd.DateOffset(months=12)
print("Initial cutoff date:", cutoff_date.date())

# 2. Copy tables to new DB
tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", src_conn)
for t in tables['name']:
    if t == 'streams':
        # Keep only recent rows
        df = pd.read_sql(f"SELECT * FROM {t}", src_conn)
        df['date_dt'] = pd.to_datetime(df['date'], format="%m/%d/%Y")
        df_demo = df[df['date_dt'] >= cutoff_date].drop(columns=['date_dt'])
        print(f"{t}: keeping {len(df_demo)} rows")
        df_demo.to_sql(t, dst_conn, index=False, if_exists='replace')
    else:
        # Copy full table (artists, regions, etc.)
        df = pd.read_sql(f"SELECT * FROM {t}", src_conn)
        df.to_sql(t, dst_conn, index=False, if_exists='replace')
        print(f"{t}: copied {len(df)} rows")

dst_conn.close()
src_conn.close()

# Optional: run VACUUM to shrink demo DB
conn = sqlite3.connect(DST_DB)
conn.execute("VACUUM;")
conn.close()
print("Demo DB created:", DST_DB)
