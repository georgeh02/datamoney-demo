import sqlite3
import pandas as pd
import uuid
from datetime import datetime

DB_PATH = "/Users/georgeharrison/Desktop/ARTIST STAT PROJECT/data-money/datamoney.db"

def is_date_string(s):
    if pd.isna(s):
        return False
    try:
        datetime.strptime(str(s), "%m/%d/%Y")
        return True
    except ValueError:
        return False

def get_or_create_id(conn, table, id_column, name):
    """Get id from table by name, or create a new UUID entry."""
    cur = conn.cursor()
    cur.execute(f"SELECT {id_column} FROM {table} WHERE name = ?", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    new_id = str(uuid.uuid4())
    cur.execute(f"INSERT INTO {table} ({id_column}, name) VALUES (?, ?)", (new_id, name))
    conn.commit()
    return new_id

def ingest_file(file_path):
    conn = sqlite3.connect(DB_PATH)
    xls = pd.ExcelFile(file_path, engine="openpyxl")

    # --- STEP 1: get region ---
    summary_df = pd.read_excel(file_path, sheet_name="Report Summary", engine="openpyxl", header=None)
    country_row = summary_df[summary_df.iloc[:, 0] == "Country"]
    region_name = country_row.iloc[0, 1] if not country_row.empty else "Unknown"
    region_id = get_or_create_id(conn, "regions", "region_id", region_name)
    print(f"Region: {region_name}")

    # --- STEP 2: get metric ---
    metric_name = "Streaming On-Demand Audio"
    metric_id = get_or_create_id(conn, "metrics", "metric_id", metric_name)

    # --- STEP 3: loop artist sheets ---
    for sheet in xls.sheet_names:
        if sheet.lower() == "report summary":
            continue

        df = pd.read_excel(file_path, sheet_name=sheet, header=None, engine="openpyxl")

        # artist
        artist_row = df[df[1] == "Artist"]
        if artist_row.empty:
            continue
        artist_name = str(artist_row.iloc[0, 2]).strip()
        artist_id = get_or_create_id(conn, "artists", "artist_id", artist_name)
        print(f"Processing artist: {artist_name}")

        # date columns
        date_row_idx = 6
        raw_dates = df.iloc[date_row_idx, 2:].tolist()
        date_cols = [i for i, val in enumerate(raw_dates, start=2) if is_date_string(val)]
        dates = [df.iat[date_row_idx, c] for c in date_cols]

        # stream values
        sod_row = df[df[1] == metric_name]
        if sod_row.empty:
            print("  No Streaming On-Demand Audio row found.")
            continue
        sod_values = [sod_row.iat[0, c] for c in date_cols]

        print(f"Processing artist: {artist_name}")
        print(f"Raw date columns: {raw_dates}")
        print(f"Detected date columns: {dates}")
        print(f"Stream values: {sod_values}")

        # --- insert into streams table ---
        cur = conn.cursor()
        for date, value in zip(dates, sod_values):
            try:
                cur.execute("""
                    INSERT OR IGNORE INTO streams (stream_id, artist_id, region_id, metric_id, date, count)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (str(uuid.uuid4()), artist_id, region_id, metric_id, date, value))
            except Exception as e:
                print(f"Error inserting {artist_name} {date}: {e}")
        conn.commit()

    conn.close()
    print("Ingestion complete.")

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python3 ingestion.py file1.xlsx [file2.xlsx ...]")
        sys.exit(1)

    for file_path in sys.argv[1:]:
        print(f"Ingesting file: {file_path}")
        ingest_file(file_path)