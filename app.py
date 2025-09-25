# app.py
import streamlit as st
import sqlite3
import pandas as pd
from datetime import timedelta, date
import calendar
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, ColumnsAutoSizeMode, JsCode

DB_PATH = "datamoney_demo.db"

# --- Page & sidebar ---
st.set_page_config(layout="wide")
st.sidebar.header("Settings")

# Custom CSS to remove padding and margins from Streamlit's main container and set Ag-Grid height
st.markdown("""
<style>
.main .block-container {
    padding: 0 !important;
    margin: 0 !important;
}
.ag-grid-wrapper {
    height: 90vh !important;
}
</style>
""", unsafe_allow_html=True)

# --- Load dates from DB ---
try:
    with sqlite3.connect(DB_PATH) as conn:
        df_dates = pd.read_sql("SELECT DISTINCT date FROM streams", conn)
    df_dates['date_dt'] = pd.to_datetime(df_dates['date'], format="%m/%d/%Y")
    date_min = df_dates['date_dt'].min().date()
    date_max = df_dates['date_dt'].max().date()

    df_dates['year'] = df_dates['date_dt'].dt.year
    df_dates['month_year'] = df_dates['date_dt'].dt.strftime('%B %Y')
    all_years = sorted(df_dates['year'].unique(), reverse=True)
    all_months = sorted(df_dates['month_year'].unique(), reverse=True)

except Exception as e:
    st.error(f"Error loading dates from database: {e}")
    st.stop()

# --- Sidebar controls ---
mode = st.sidebar.selectbox(
    "Select Metric",
    ("Daily Streams", "Weekly Streams", "Monthly Streams", "Yearly Streams")
)

# --- Dynamic date selector ---
selected_date = None
selected_lookback_date = None

if mode in ("Daily Streams", "Weekly Streams"):
    selected_date = st.sidebar.date_input(
        "Select Current Period End Date",
        value=date_max,
        min_value=date_min,
        max_value=date_max
    )

    if mode == "Weekly Streams":
        st.sidebar.caption("This will compare the last 7 days ending on this date vs the previous 7-day period.")
        selected_lookback_date = selected_date - timedelta(days=7)
    else:
        # Only show lookback period for Daily Streams
        lookback_option = st.sidebar.selectbox(
            "Select Lookback Period",
            ("7 Days", "2 Weeks", "1 Month", "3 Months", "6 Months", "1 Year", "All Time", "Custom")
        )

        if lookback_option == "All Time":
            selected_lookback_date = date_min
        elif lookback_option == "Custom":
            selected_lookback_date = st.sidebar.date_input(
                "Select Start Date",
                value=selected_date - timedelta(days=7),
                min_value=date_min,
                max_value=selected_date
            )
        else:
            days_map = {
                "7 Days": 7,
                "2 Weeks": 14,
                "1 Month": 30,
                "3 Months": 90,
                "6 Months": 180,
                "1 Year": 365,
            }
            days = days_map.get(lookback_option, 7)
            selected_lookback_date = selected_date - timedelta(days=days)


elif mode == "Monthly Streams":
    selected_month_str = st.sidebar.selectbox(
        "Select Current Month",
        options=all_months
    )
    selected_date = pd.to_datetime(selected_month_str, format='%B %Y').date()

    lookback_month_str = st.sidebar.selectbox(
        "Select Lookback Month",
        options=all_months,
        index=all_months.index(all_months[1]) if len(all_months) > 1 else 0
    )
    selected_lookback_date = pd.to_datetime(lookback_month_str, format='%B %Y').date()

elif mode == "Yearly Streams":
    selected_year = st.sidebar.selectbox(
        "Select Current Year",
        options=all_years,
        index=0
    )
    selected_date = date(selected_year, 1, 1)

    lookback_year = st.sidebar.selectbox(
        "Select Lookback Year",
        options=all_years,
        index=1 if len(all_years) > 1 else 0
    )
    selected_lookback_date = date(lookback_year, 1, 1)

# --- Fetch snapshot ---
def fetch_snapshot(cur_date, prev_date, mode):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            if mode == "Daily Streams":
                cur_range = (cur_date, cur_date)
                prev_range = (prev_date, prev_date)
            elif mode == "Weekly Streams":
                cur_range = (cur_date - timedelta(days=6), cur_date)
                prev_range = (prev_date - timedelta(days=6), prev_date)
            elif mode == "Monthly Streams":
                cur_range_start = cur_date.replace(day=1)
                cur_range_end = cur_date.replace(day=calendar.monthrange(cur_date.year, cur_date.month)[1])
                prev_range_start = prev_date.replace(day=1)
                prev_range_end = prev_date.replace(day=calendar.monthrange(prev_date.year, prev_date.month)[1])
                cur_range = (cur_range_start, cur_range_end)
                prev_range = (prev_range_start, prev_range_end)
            elif mode == "Yearly Streams":
                cur_range = (cur_date.replace(month=1, day=1), cur_date.replace(month=12, day=31))
                prev_range = (prev_date.replace(month=1, day=1), prev_date.replace(month=12, day=31))

            sql_template = """
            SELECT a.name as artist, SUM(s.count) as total_streams
            FROM streams s
            JOIN artists a ON s.artist_id = a.artist_id
            JOIN regions r ON s.region_id = r.region_id
            WHERE r.name = ? AND s.date BETWEEN ? AND ?
            GROUP BY a.name
            """

            us_cur_results = conn.execute(sql_template, ('US', cur_range[0].strftime("%m/%d/%Y"), cur_range[1].strftime("%m/%d/%Y"))).fetchall()
            us_prev_results = conn.execute(sql_template, ('US', prev_range[0].strftime("%m/%d/%Y"), prev_range[1].strftime("%m/%d/%Y"))).fetchall()
            gl_cur_results = conn.execute(sql_template, ('Global', cur_range[0].strftime("%m/%d/%Y"), cur_range[1].strftime("%m/%d/%Y"))).fetchall()
            gl_prev_results = conn.execute(sql_template, ('Global', prev_range[0].strftime("%m/%d/%Y"), prev_range[1].strftime("%m/%d/%Y"))).fetchall()
            
            us_cur = dict(us_cur_results)
            us_prev = dict(us_prev_results)
            gl_cur = dict(gl_cur_results)
            gl_prev = dict(gl_prev_results)

    except Exception as e:
        st.error(f"Error fetching data from database: {e}")
        return pd.DataFrame()

    artists = set(us_cur) | set(us_prev) | set(gl_cur) | set(gl_prev)
    rows = []
    for artist in sorted(artists):
        us_now = us_cur.get(artist)
        us_then = us_prev.get(artist)
        gl_now = gl_cur.get(artist)
        gl_then = gl_prev.get(artist)

        def pct_change(now, then):
            if now is None or then is None or then == 0:
                return None
            return round(((now - then) / then) * 100, 2)

        rows.append({
            "Artist": artist,
            "US Streams": us_now,
            "US Streams Prev": us_then,
            "% Change US": pct_change(us_now, us_then),
            "Global Streams": gl_now,
            "Global Streams Prev": gl_then,
            "% Change Global": pct_change(gl_now, gl_then),
        })
    return pd.DataFrame(rows)

# --- Display grid ---
df = fetch_snapshot(selected_date, selected_lookback_date, mode)

# --- Sidebar display ---
if mode == "Daily Streams":
    st.sidebar.write(
        f"Comparing {selected_date.strftime('%m/%d/%Y')} vs "
        f"{selected_lookback_date.strftime('%m/%d/%Y')}"
    )
elif mode == "Weekly Streams":
    start_cur = selected_date - timedelta(days=6)
    end_cur = selected_date
    start_prev = selected_lookback_date - timedelta(days=6)
    end_prev = selected_lookback_date
    st.sidebar.write(
        f"Comparing {start_cur.strftime('%m/%d/%Y')} to {end_cur.strftime('%m/%d/%Y')} vs "
        f"{start_prev.strftime('%m/%d/%Y')} to {end_prev.strftime('%m/%d/%Y')}"
    )
elif mode == "Monthly Streams":
    start_cur = selected_date.replace(day=1)
    end_cur = selected_date.replace(day=calendar.monthrange(selected_date.year, selected_date.month)[1])
    start_prev = selected_lookback_date.replace(day=1)
    end_prev = selected_lookback_date.replace(day=calendar.monthrange(selected_lookback_date.year, selected_lookback_date.month)[1])
    st.sidebar.write(
        f"Comparing {start_cur.strftime('%B %Y')} vs "
        f"{start_prev.strftime('%B %Y')}"
    )
elif mode == "Yearly Streams":
    st.sidebar.write(
        f"Comparing {selected_date.strftime('%Y')} vs "
        f"{selected_lookback_date.strftime('%Y')}"
    )

# --- Ag-Grid configuration ---
gb = GridOptionsBuilder.from_dataframe(df)

number_formatter = JsCode("""
function(params) {
    if (params.value === undefined || params.value === null) return '';
    return params.value.toLocaleString();
}
""")

pct_formatter = JsCode("""
function(params) {
    if (params.value === undefined || params.value === null) return '';
    return Math.round(params.value).toLocaleString() + '%';
}
""")

pct_style = JsCode("""
function(params) {
    if (params.value === undefined || params.value === null) return {};
    var value = parseFloat(params.value);
    if (value > 0) return {'color': '#1abc9c'};
    else if (value < 0) return {'color': '#e74c3c'};
    else return {'color': 'gray'};
}
""")

gb.configure_column("Artist", filter="agTextColumnFilter", sortable=True)
gb.configure_column("US Streams", header_name="🇺🇸 Current Streams", type=['numericColumn', 'numberColumnFilter'], precision=0, agg_func='sum', valueFormatter=number_formatter)
gb.configure_column("US Streams Prev", header_name="🇺🇸 Previous Streams", type=['numericColumn', 'numberColumnFilter'], precision=0, agg_func='sum', valueFormatter=number_formatter)
gb.configure_column("% Change US", header_name="🇺🇸 % Change", type=['numericColumn', 'numberColumnFilter'], valueFormatter=pct_formatter, cellStyle=pct_style, sort="desc")
gb.configure_column("Global Streams", header_name="🌎 Current Streams", type=['numericColumn', 'numberColumnFilter'], precision=0, agg_func='sum', valueFormatter=number_formatter)
gb.configure_column("Global Streams Prev", header_name="🌎 Previous Streams", type=['numericColumn', 'numberColumnFilter'], precision=0, agg_func='sum', valueFormatter=number_formatter)
gb.configure_column("% Change Global", header_name="🌎 % Change", type=['numericColumn', 'numberColumnFilter'], valueFormatter=pct_formatter, cellStyle=pct_style)

gb.configure_grid_options(domLayout='normal')
gridOptions = gb.build()

AgGrid(
    df,
    gridOptions=gridOptions,
    enable_enterprise_modules=True,
    allow_unsafe_jscode=True,
    update_mode=GridUpdateMode.MODEL_CHANGED,
    data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
    columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
    height=700,
    key="aggrid_key"
)
