import pandas as pd
import sqlite3
import requests
import io
import gzip
from io import BytesIO
import os
import matplotlib.pyplot as plt
import seaborn as sns

# Functions for data retrieval, reshaping, and saving to database
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
    data_frame['Date'] = pd.to_datetime(data_frame['Date'])
    data_2012 = data_frame[data_frame['Date'].dt.year == 2012]
    data_2012_filtered = data_2012.iloc[:, 7:31].dropna(how='all')
    data_2012_filtered['Date'] = data_2012['Date'].loc[data_2012_filtered.index]
    data_2012_filtered['Month'] = data_2012_filtered['Date'].dt.month_name()
    monthly_totals = data_2012_filtered.groupby('Month').sum(numeric_only=True).sum(axis=1)
    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    monthly_traffic = pd.DataFrame({
        'month': monthly_totals.index.str[:3],
        'traffics': monthly_totals.values
    })
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
    os.makedirs('../data', exist_ok=True)
    database_file_path = '../data/MADE.sqlite'
    traffic_data_url = "https://data.cityofnewyork.us/api/views/btm5-ppia/rows.csv"
    raw_traffic_data = retrieve_data(traffic_data_url)
    traffic_data_frame = reshape_traffic_data(raw_traffic_data)
    save_dataframe_to_db(traffic_data_frame, database_file_path, 'traffic')
    print("Monthly traffic data for the year 2012 has been saved to SQLite database.")
    weather_data_url = "https://bulk.meteostat.net/v2/hourly/72502.csv.gz"
    raw_weather_data = retrieve_data(weather_data_url, compressed=True)
    weather_data_frame = reshape_weather_data(raw_weather_data)
    save_dataframe_to_db(weather_data_frame, database_file_path, 'weather')
    print("Monthly averaged data for the year 2012 has been saved to SQLite database.")

# Execute the data pipeline
execute_pipeline()

# Load data from the SQLite database
database_file_path = '../data/MADE.sqlite'
conn = sqlite3.connect(database_file_path)
traffic_df = pd.read_sql_query("SELECT * FROM traffic", conn)
weather_df = pd.read_sql_query("SELECT * FROM weather", conn)
conn.close()

# Merge traffic and weather data
merged_df = pd.merge(traffic_df, weather_df, on='month')

# Convert month names to categorical for proper ordering
months_sequence = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
merged_df['month'] = pd.Categorical(merged_df['month'], categories=months_sequence, ordered=True)

# Create season column
season_mapping = {
    'Jan': 'Winter', 'Feb': 'Winter', 'Mar': 'Spring', 'Apr': 'Spring', 'May': 'Spring',
    'Jun': 'Summer', 'Jul': 'Summer', 'Aug': 'Summer', 'Sep': 'Fall', 'Oct': 'Fall', 'Nov': 'Fall', 'Dec': 'Winter'
}
merged_df['season'] = merged_df['month'].map(season_mapping)

# Plotting functions
def plot_temperature_vs_traffic(df):
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='month', y='tavg', marker='o', label='Avg Temp (°C)')
    plt.twinx()
    sns.barplot(data=df, x='month', y='traffics', alpha=0.6, color='gray', label='Traffic')
    plt.title('Average Temperature vs Traffic')
    plt.show()

def plot_snowfall_vs_traffic(df):
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='month', y='snow', marker='o', label='Avg Snowfall (mm)')
    plt.twinx()
    sns.barplot(data=df, x='month', y='traffics', alpha=0.6, color='gray', label='Traffic')
    plt.title('Average Snowfall vs Traffic')
    plt.show()

def plot_precipitation_vs_traffic(df):
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='month', y='prcp', marker='o', label='Avg Precipitation (mm)')
    plt.twinx()
    sns.barplot(data=df, x='month', y='traffics', alpha=0.6, color='gray', label='Traffic')
    plt.title('Average Precipitation vs Traffic')
    plt.show()

def plot_wind_speed_vs_traffic(df):
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='month', y='wspd', marker='o', label='Avg Wind Speed (km/h)')
    plt.twinx()
    sns.barplot(data=df, x='month', y='traffics', alpha=0.6, color='gray', label='Traffic')
    plt.title('Average Wind Speed vs Traffic')
    plt.show()

def plot_season_vs_traffic(df):
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df, x='season', y='traffics')
    plt.title('Season vs Traffic')
    plt.show()

def plot_all_factors_vs_traffic(df):
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.set_xlabel('Month')
    ax1.set_ylabel('Traffic', color='tab:gray')
    sns.barplot(data=df, x='month', y='traffics', alpha=0.6, color='gray', label='Traffic', ax=ax1)
    ax1.tick_params(axis='y', labelcolor='tab:gray')

    ax2 = ax1.twinx()
    ax2.set_ylabel('Avg Values', color='tab:blue')
    sns.lineplot(data=df, x='month', y='tavg', marker='o', label='Avg Temp (°C)', ax=ax2)
    sns.lineplot(data=df, x='month', y='snow', marker='o', label='Avg Snowfall (mm)', ax=ax2)
    sns.lineplot(data=df, x='month', y='prcp', marker='o', label='Avg Precipitation (mm)', ax=ax2)
    sns.lineplot(data=df, x='month', y='wspd', marker='o', label='Avg Wind Speed (km/h)', ax=ax2)
    ax2.tick_params(axis='y', labelcolor='tab:blue')

    fig.tight_layout()
    plt.title('All Weather Factors vs Traffic')
    fig.legend(loc='upper right')
    plt.show()

# Execute plotting functions
plot_temperature_vs_traffic(merged_df)
plot_snowfall_vs_traffic(merged_df)
plot_precipitation_vs_traffic(merged_df)
plot_wind_speed_vs_traffic(merged_df)
plot_season_vs_traffic(merged_df)
plot_all_factors_vs_traffic(merged_df)
