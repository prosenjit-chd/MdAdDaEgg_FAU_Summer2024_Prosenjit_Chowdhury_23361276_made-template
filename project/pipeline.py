import pandas as pd
import sqlite3
import requests
import io
import gzip
from io import BytesIO
import os

def retrieve_data(source_url, compressed=False):
    print(f"Retrieving data from {source_url}...")
    response = requests.get(source_url)
    if compressed:
        print("Decompressing the data...")
        data = gzip.decompress(response.content)
        return data
    else:
        return response.content.decode('utf-8')

def save_dataframe_to_db(dataframe, database_file, table_name):
    print(f"Storing data in {table_name} table in {database_file}...")
    conn = sqlite3.connect(database_file)
    cur = conn.cursor()
    
    # Create table without auto-incremented primary key
    if table_name == 'traffic':
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            month TEXT,
            traffics INTEGER
        );
        """)
    elif table_name == 'weather':
        cur.execute(f"""
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
    for row in dataframe.itertuples(index=False):
        if table_name == 'traffic':
            cur.execute(f"""
            INSERT INTO {table_name} (month, traffics) VALUES (?, ?)
            """, (row.month, row.traffics))
        elif table_name == 'weather':
            cur.execute(f"""
            INSERT INTO {table_name} (month, tavg, snow, prcp, wspd) VALUES (?, ?, ?, ?, ?)
            """, (row.month, row.tavg, row.snow, row.prcp, row.wspd))
    
    conn.commit()
    conn.close()

def reshape_traffic_data(raw_data):
    print("Reshaping traffic data...")
    data_frame = pd.read_csv(io.StringIO(raw_data))
    
    # Filter data for the year 2012
    data_frame['Date'] = pd.to_datetime(data_frame['Date'])
    data_2012 = data_frame[data_frame['Date'].dt.year == 2012]
    
    # Extract necessary columns (index 7 to 30) and drop rows where all selected columns are empty
    data_2012_filtered = data_2012.iloc[:, 7:31].dropna(how='all')
    
    # Add 'Date' column back for grouping
    data_2012_filtered['Date'] = data_2012['Date'].loc[data_2012_filtered.index]
    
    # Create 'Month' column for grouping
    data_2012_filtered['Month'] = data_2012_filtered['Date'].dt.month_name()
    
    # Sum the values for each month, excluding 'Date' and 'Month' columns from summation
    monthly_totals = data_2012_filtered.groupby('Month').sum(numeric_only=True).sum(axis=1)
    
    # Create the final DataFrame
    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    monthly_traffic = pd.DataFrame({
        'month': monthly_totals.index.str[:3],
        'traffics': monthly_totals.values
    })
    
    # Sorting the month column in the specified order
    monthly_traffic['month'] = pd.Categorical(monthly_traffic['month'], categories=month_order, ordered=True)
    monthly_traffic = monthly_traffic.sort_values('month').reset_index(drop=True)
    
    return monthly_traffic

def reshape_weather_data(raw_data):
    print("Reshaping weather data...")
    columns_required = [0, 3, 4, 5, 8]
    data_frame = pd.read_csv(BytesIO(raw_data), header=None, usecols=columns_required)
    data_frame.columns = ['date', 'tavg', 'snow', 'prcp', 'wspd']
    data_frame['date'] = pd.to_datetime(data_frame['date'])
    data_2012 = data_frame[data_frame['date'].dt.year == 2012]
    data_2012 = data_2012.dropna()
    data_2012['month'] = data_2012['date'].dt.strftime('%b')
    monthly_avg_data = data_2012.groupby('month').mean().reset_index()
    months_sequence = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    monthly_avg_data['month'] = pd.Categorical(monthly_avg_data['month'], categories=months_sequence, ordered=True)
    monthly_avg_data = monthly_avg_data.sort_values('month')
    monthly_avg_data = monthly_avg_data.drop(columns=['date'], errors='ignore')
    monthly_avg_data[['tavg', 'snow', 'prcp', 'wspd']] = monthly_avg_data[['tavg', 'snow', 'prcp', 'wspd']].round(2)
    return monthly_avg_data

def execute_pipeline():
    # Ensure the ../data directory exists
    os.makedirs('../data', exist_ok=True)
    
    database_file_path = '../data/MADE.sqlite'
    
    # Process traffic data
    traffic_data_url = "https://data.cityofnewyork.us/api/views/btm5-ppia/rows.csv"
    raw_traffic_data = retrieve_data(traffic_data_url)
    traffic_data_frame = reshape_traffic_data(raw_traffic_data)
    save_dataframe_to_db(traffic_data_frame, database_file_path, 'traffic')
    print("Monthly traffic data for the year 2012 has been saved to SQLite database.")
    
    # Process weather data
    weather_data_url = "https://bulk.meteostat.net/v2/hourly/72502.csv.gz"
    raw_weather_data = retrieve_data(weather_data_url, compressed=True)
    weather_data_frame = reshape_weather_data(raw_weather_data)
    save_dataframe_to_db(weather_data_frame, database_file_path, 'weather')
    print("Monthly averaged data for the year 2012 has been saved to SQLite database.")

if __name__ == "__main__":
    execute_pipeline()
