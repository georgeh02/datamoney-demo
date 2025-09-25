# app.py
import streamlit as st
import sqlite3
import pandas as pd
from datetime import timedelta
from streamlit_slickgrid import slickgrid, FieldType, Formatters

# NOTE: The database path must be correct for the app to run.
# DB_PATH = "/Users/georgeharrison/Desktop/ARTIST STAT PROJECT/data-money/datamoney.db"
# Using a placeholder for demonstration.
DB_PATH = "datamoney.db" 

# --- Page & sidebar ---
st.set_page_config(layout="wide")
st.sidebar.header("Controls")

# --- Load dates from DB ---
try:
    with sqlite3.connect(DB_PATH) as conn:
        df_dates = pd.read_sql("SELECT DISTINCT date FROM streams", conn)
    df_dates['date_dt'] = pd.to_datetime(df_dates['date'], format="%m/%d/%Y")
    date_min = df_dates['date_dt'].min().date()
    date_max = df_dates['date_dt'].max().date()
except Exception as e:
    st.error(f"Error loading dates from database: {e}")
    st.stop()

# --- Sidebar controls ---
selected_date = st.sidebar.date_input(
    "Select Current Date",
    value=date_max,
    min_value=date_min,
    max_value=date_max
)

lookback_days = st.sidebar.slider(
    "Lookback Period (days)", min_value=1, max_value=90, value=7
)
lookback_date = selected_date - timedelta(days=lookback_days)
st.sidebar.write(
    f"Comparing {selected_date.strftime('%m/%d/%Y')} vs "
    f"{lookback_date.strftime('%m/%d/%Y')}"
)

# --- Fetch snapshot ---
def fetch_snapshot(cur_date_str, prev_date_str):
    """
    Fetches and processes stream data for a given date and a lookback date.
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            us_sql = """
            SELECT a.name as artist, s.count
            FROM streams s
            JOIN artists a ON s.artist_id = a.artist_id
            JOIN regions r ON s.region_id = r.region_id
            WHERE r.name = 'US' AND s.date = ?
            """
            us_cur_results = conn.execute(us_sql, (cur_date_str,)).fetchall()
            us_prev_results = conn.execute(us_sql, (prev_date_str,)).fetchall()
            
            us_cur = dict(us_cur_results)
            us_prev = dict(us_prev_results)

            gl_sql = us_sql.replace("'US'", "'Global'") # Correctly replace to get global data
            gl_cur_results = conn.execute(gl_sql, (cur_date_str,)).fetchall()
            gl_prev_results = conn.execute(gl_sql, (prev_date_str,)).fetchall()
            
            gl_cur = dict(gl_cur_results)
            gl_prev = dict(gl_prev_results)

    except Exception as e:
        st.error(f"Error fetching data from database: {e}")
        return pd.DataFrame()

    artists = set(us_cur) | set(us_prev) | set(gl_cur) | set(gl_prev)
    rows = []
    for i, artist in enumerate(sorted(artists), start=1):
        us_now = us_cur.get(artist)
        us_then = us_prev.get(artist)
        gl_now = gl_cur.get(artist)
        gl_then = gl_prev.get(artist)

        def pct_change(now, then):
            if now is None or then is None or then == 0:
                return None
            return round(((now - then) / then) * 100, 2)

        rows.append({
            "id": i,
            "Artist": artist,
            "US Streams": us_now,
            "US Streams Prev": us_then,
            "% Change US": pct_change(us_now, us_then),
            "Global Streams": gl_now,
            "% Change Global": pct_change(gl_now, gl_then),
        })
    return pd.DataFrame(rows)

cur_date_str = selected_date.strftime("%m/%d/%Y")
prev_date_str = lookback_date.strftime("%m/%d/%Y")
df = fetch_snapshot(cur_date_str, prev_date_str).fillna("")

# --- SlickGrid columns ---
columns = [
    {"id": "Artist", "name": "Artist", "field": "Artist", "type": FieldType.string, "sortable": True, "filterable": True},
    {"id": "US Streams", "name": "Streams 🇺🇸", "field": "US Streams", "sortable": True, "type": FieldType.number,
     "formatter": Formatters.decimal, "params": {"thousandSeparator": ",", "minDecimal": 0, "maxDecimal": 0}, "cssClass": "text-right"},
    {"id": "US Streams Prev", "name": "Streams Prev 🇺🇸", "field": "US Streams Prev", "sortable": True, "type": FieldType.number,
     "formatter": Formatters.decimal, "params": {"thousandSeparator": ",", "minDecimal": 0, "maxDecimal": 0}, "cssClass": "text-right"},
    {"id": "% Change US", "name": "% Change 🇺🇸", "field": "% Change US", "sortable": True, "type": FieldType.number,
     "formatter": Formatters.decimal, "params": {"thousandSeparator": ",", "minDecimal": 1, "maxDecimal": 1, "numberSuffix": " %"}, "cssClass": "text-right"},
    {"id": "Global Streams", "name": "Streams 🌎", "field": "Global Streams", "sortable": True, "type": FieldType.number,
     "formatter": Formatters.decimal, "params": {"thousandSeparator": ",", "minDecimal": 0, "maxDecimal": 0}, "cssClass": "text-right"},
    {"id": "% Change Global", "name": "% Change 🌎", "field": "% Change Global", "sortable": True, "type": FieldType.number,
     "formatter": Formatters.decimal, "params": {"thousandSeparator": ",", "minDecimal": 1, "maxDecimal": 1, "numberSuffix": " %"}, "cssClass": "text-right"},
]

options = {
    "enableFiltering": True,
    "enableTextExport": True,
    "enableExcelExport": True,
    "autoHeight": True,
    "darkMode": True,
}

# --- Display grid ---
st.write(f"### Data Money {selected_date.strftime('%m/%d/%Y')} (vs {lookback_days} days prior)")

# The key fix: tie the grid's key to the selected date. This forces a re-render
# whenever the date changes, preventing the "one step behind" issue.
out = slickgrid(df.to_dict("records"), columns, options, key=f"streams_grid_{selected_date}")

# --- You can remove the old state management logic as it's no longer needed ---
# if out is not None:
#     _, _, new_grid_state = out
#     st.session_state.grid_state = new_grid_state
