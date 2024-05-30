import pandas as pd
import sqlite3
import requests
import io
import gzip
from io import BytesIO
import os

def fetch_data(url, compressed=False):
    print(f"Fetching data from {url}...")
    response = requests.get(url)
    if compressed:
        print("Decompressing data...")
        return gzip.decompress(response.content)
    else:
        return response.content.decode('utf-8')

def save_to_sqlite(df, db_path, table_name):
    print(f"Saving data to {table_name} table in {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table without auto-incremented primary key
    if table_name == 'traffic':
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            month TEXT,
            traffics INTEGER
        );
        """)
    elif table_name == 'weather':
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            month TEXT,
            tavg REAL,
            snow REAL,
            prcp REAL,
            wspd REAL
        );
        """)
    
    # Insert data into table
    for row in df.itertuples(index=False):
        if table_name == 'traffic':
            cursor.execute(f"""
            INSERT INTO {table_name} (month, traffics) VALUES (?, ?)
            """, (row.month, row.traffics))
        elif table_name == 'weather':
            cursor.execute(f"""
            INSERT INTO {table_name} (month, tavg, snow, prcp, wspd) VALUES (?, ?, ?, ?, ?)
            """, (row.month, row.tavg, row.snow, row.prcp, row.wspd))
    
    conn.commit()
    conn.close()


def transform_traffic_data(data):
    print("Transforming traffic data...")
    df = pd.read_csv(io.StringIO(data))
    
    # Filter data for the year 2012
    df['Date'] = pd.to_datetime(df['Date'])
    df_2012 = df[df['Date'].dt.year == 2012]
    
    # Extract the necessary columns (index 7 to 30) and drop rows where all selected columns are empty
    df_2012_subset = df_2012.iloc[:, 7:31].dropna(how='all')
    
    # Add the 'Date' column back for grouping
    df_2012_subset['Date'] = df_2012['Date'].loc[df_2012_subset.index]
    
    # Create 'Month' column for grouping
    df_2012_subset['Month'] = df_2012_subset['Date'].dt.month_name()
    
    # Sum the values for each month, excluding the 'Date' and 'Month' columns from summation
    monthly_sums = df_2012_subset.groupby('Month').sum(numeric_only=True).sum(axis=1)
    
    # Create the final DataFrame
    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    monthly_data = pd.DataFrame({
        'month': monthly_sums.index.str[:3],
        'traffics': monthly_sums.values
    })
    
    # Sorting the month column in the specified order
    monthly_data['month'] = pd.Categorical(monthly_data['month'], categories=month_order, ordered=True)
    monthly_data = monthly_data.sort_values('month').reset_index(drop=True)
    
    return monthly_data

def transform_weather_data(data):
    print("Transforming weather data...")
    selected_columns = [0, 3, 4, 5, 8]
    df = pd.read_csv(BytesIO(data), header=None, usecols=selected_columns)
    df.columns = ['date', 'tavg', 'snow', 'prcp', 'wspd']
    df['date'] = pd.to_datetime(df['date'])
    df_2012 = df[df['date'].dt.year == 2012]
    df_2012 = df_2012.dropna()
    df_2012['month'] = df_2012['date'].dt.strftime('%b')
    monthly_avg = df_2012.groupby('month').mean().reset_index()
    months_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    monthly_avg['month'] = pd.Categorical(monthly_avg['month'], categories=months_order, ordered=True)
    monthly_avg = monthly_avg.sort_values('month')
    monthly_avg = monthly_avg.drop(columns=['date'], errors='ignore')
    monthly_avg[['tavg', 'snow', 'prcp', 'wspd']] = monthly_avg[['tavg', 'snow', 'prcp', 'wspd']].round(2)
    return monthly_avg

def main():
    # Ensure the ../data directory exists
    os.makedirs('../data', exist_ok=True)
    
    db_path = '../data/MADE.sqlite'
    
    # Process traffic data
    traffic_url = "https://data.cityofnewyork.us/api/views/btm5-ppia/rows.csv"
    traffic_data = fetch_data(traffic_url)
    traffic_df = transform_traffic_data(traffic_data)
    save_to_sqlite(traffic_df, db_path, 'traffic')
    print("Monthly traffic data for the year 2012 has been saved to SQLite database.")
    
    # Process weather data
    weather_url = "https://bulk.meteostat.net/v2/hourly/72502.csv.gz"
    weather_data = fetch_data(weather_url, compressed=True)
    weather_df = transform_weather_data(weather_data)
    save_to_sqlite(weather_df, db_path, 'weather')
    print("Monthly averaged data for the year 2012 has been saved to SQLite database.")

if __name__ == "__main__":
    main()
