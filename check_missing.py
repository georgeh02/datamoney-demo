import sqlite3
from datetime import datetime, timedelta
import csv

DB_PATH = "/Users/georgeharrison/Desktop/ARTIST STAT PROJECT/data-money/datamoney.db"
OUTPUT_CSV = "/Users/georgeharrison/Desktop/ARTIST STAT PROJECT/data-money/missing_streams.csv"

START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 9, 18)

def daterange(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Get all artists
    cur.execute("SELECT artist_id, name FROM artists")
    artists = cur.fetchall()

    # Get region IDs
    cur.execute("SELECT region_id, name FROM regions")
    regions = {name: region_id for region_id, name in cur.fetchall()}

    missing_records = []

    for artist_id, artist_name in artists:
        for region_name in ["US", "Global"]:
            region_id = regions.get(region_name)
            if not region_id:
                continue

            for single_date in daterange(START_DATE, END_DATE):
                date_str = single_date.strftime("%m/%d/%Y")
                cur.execute("""
                    SELECT 1 FROM streams
                    WHERE artist_id = ? AND region_id = ? AND date = ?
                """, (artist_id, region_id, date_str))
                if not cur.fetchone():
                    missing_records.append((artist_name, region_name, date_str))

    conn.close()

    # Write all missing records to CSV
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["artist_name", "region", "date"])
        writer.writerows(missing_records)

    print(f"Check complete. Total missing records: {len(missing_records)}")
    print(f"Written to: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()