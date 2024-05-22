#### PLEASE READ README.md FILE BEFORE RUNNING ####

import opendatasets as od
import os
import sqlite3
import gdown
import pandas as pd
import requests
from io import BytesIO
import numpy as np
import pyarrow.parquet as pq


class data_pipeline():
    # Creates DataFrames for the both datasets and calls all the functions required
    def __init__(self):
        print("Starting The Pipepline... ")
        self.weather_df=None
        self.traffic_df=None

        self.Download_CSV_Files()


        #Cleaning Weather DataSet
        self.weather_df=self.weather_df.drop(columns=['tempmax','tempmin','feelslikemax', 'feelslikemin', 'humidity', 'precipprob', 'precipcover', 'preciptype', 'windgust', 'winddir', 'sealevelpressure', 'cloudcover', 'solarradiation', 'solarenergy', 'uvindex', 'severerisk', 'moonphase', 'conditions', 'description', 'icon', 'stations'])
        self.weather_df=self.General_Cleaning(self.weather_df)


        #Cleaning Traffic DataSet
        self.traffic_df=self.traffic_df.drop(columns=['Direction', 'SegmentID'])
        self.traffic_df=self.General_Cleaning(self.traffic_df)

        #Making SQLITE files
        self.Create_Dimention_Tables()
        print("All Tasks Completed! ")
        
    

    def Create_Dimention_Tables(self):
        #Creating Weather Table (Dimention)
        weather_df= pd.DataFrame()
        weather_df['datetime']=self.weather_df['datetime'].unique()
        weather_df = pd.merge(weather_df, self.weather_df[['name','datetime', 'temp', 'feelslike', 'dew', 'precip', 'snow', 'snowdepth', 'visibility', 'sunrise', 'sunset']], on='datetime', how='left')
        self.Create_SQL_Table('Weather',weather_df,'datetime')
        del weather_df
        
        #Creating Traffic Table (Dimention)
        traffic_df= pd.DataFrame()
        traffic_df['ID']=self.traffic_df['ID'].unique()
        traffic_df = pd.merge(traffic_df, self.traffic_df[['ID', 'Roadway Name', 'Date']], on='ID', how='left')
        traffic_df = traffic_df.drop_duplicates(subset='ID')
        self.Create_SQL_Table('Traffic',traffic_df,'ID')
        del traffic_df
    
    # Creates the SQL Table and places it in the /data directory
    def Create_SQL_Table(self,table_name, x_dataframe, primary_key=None, foreign_key=None, foreign_key_dif=None,foreign_key_table=None):
        print(f"Creating an SQLite table for {table_name}, please wait...")
        sql_type_mapping = {'int64': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR(255)', 'bool': 'BOOLEAN', 'datetime64[us]': 'TEXT', 'category': 'VARCHAR(255)'}
        current_dir = os.getcwd()
        data_dir = os.path.join(os.path.dirname(current_dir), 'data')
        os.makedirs(data_dir, exist_ok=True)
        db_path = os.path.join(data_dir, 'MADE.sqlite')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        if primary_key is not None:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({primary_key} {sql_type_mapping[x_dataframe[primary_key].dtype.name]} PRIMARY KEY, "
        else:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        create_table_query += ', '.join([f"{col} {sql_type_mapping[x_dataframe[col].dtype.name]}" for col in x_dataframe.columns if col != primary_key])
        if foreign_key is not None:
            num_of_fk=len(foreign_key)
            create_table_query +=", "
            for iii in range(0,num_of_fk):
                create_table_query += f"FOREIGN KEY ({foreign_key[iii]}) REFERENCES {foreign_key_table[iii]}({foreign_key_dif[iii]})"
                if(num_of_fk>1 and iii<num_of_fk-1):
                    create_table_query +=","
        create_table_query += ")"
        print(create_table_query)
        cursor.execute(create_table_query)
        # Insert data into the table
        if primary_key is not None:
            for index, row in x_dataframe.iterrows():
                insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(row))])})"
                cursor.execute(insert_query, tuple(row))
        else:
            for index, row in x_dataframe.iterrows():
                insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(row))])})"
                cursor.execute(insert_query, tuple(row))

        conn.commit()
        conn.close()
    
    # Cleans data by removing none values and duplicates
    def General_Cleaning(self,x_dataframe):
        print(f"Cleaning DataFrame. Please wait...")
        x_dataframe=x_dataframe.dropna()
        x_dataframe=x_dataframe.drop_duplicates()
        print('DataFrame Cleaned!')
        return x_dataframe
        
    # Downloads the CSV files from their Respective URLs
    def Download_CSV_Files(self):
        
        # Weather
        weather_csv = 'New York 2012-01-01 to 2012-12-31.csv'
        if not os.path.exists(weather_csv):
            dataset_url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=KQAFERHQEGHPQE3MEQYXNDFHZ&taskId=1e6f58df853092fcba96243810d2ec19&zip=false'
            od.download(dataset_url)
            self.weather_df = pd.read_csv(weather_csv)
            print("Weather file downloaded and loaded successfully.")
        else:
            self.weather_df = pd.read_csv(weather_csv)
            print("Weather file already exists. Skipping download.")

        # Traffic
        traffic_csv = 'Traffic_Volume_Counts.csv'
        if not os.path.exists(traffic_csv):
            dataset_url = 'https://data.cityofnewyork.us/api/views/btm5-ppia/rows.csv'
            od.download(dataset_url)
            self.traffic_df = pd.read_csv(traffic_csv)
            print("Traffic file downloaded and loaded successfully.")
        else:
            self.traffic_df = pd.read_csv(traffic_csv)
            print("Traffic file already exists. Skipping download.")
        


def main():
    test_run = data_pipeline()

if __name__ == "__main__":
    main()