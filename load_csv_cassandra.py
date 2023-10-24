"""
load_csv_cassandra.py
Created on Sat October 14 2023
Author: LEDAT
"""
import os
import csv
from cassandra.cluster import Cluster

data_element_counter = 0
directory_path = os.path.join(os.path.dirname(__file__), "data_csv")
cassandra_keyspace_name = 'mykeyspace'
cluster = Cluster(['localhost'])
session = cluster.connect(cassandra_keyspace_name)

def list_files_in_directory(directory_path):
    try:
        # Use os.listdir() to get a list of file names in the directory
        files = os.listdir(directory_path)
        # Return the list of file names
        return files
    except FileNotFoundError:
        print(f"Directory '{directory_path}' does not exist.")
        return []

file_list = list_files_in_directory(directory_path)

if file_list:
    for file in file_list:
        with open(os.path.join(directory_path, file), 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                ride_id = row['ride_id']
                rideable_type = row['rideable_type']
                started_at = row['started_at']
                ended_at = row['ended_at']
                start_station_name = row['start_station_name']
                start_station_id = row['start_station_id']
                end_station_name = row['end_station_name']
                end_station_id = row['end_station_id']
                start_lat = float(row['start_lat']) if row['start_lat'] else None
                start_lng = float(row['start_lng']) if row['start_lng'] else None
                end_lat = float(row['end_lat']) if row['end_lat'] else None
                end_lng = float(row['end_lng']) if row['end_lng'] else None
                member_casual = row['member_casual']

                cf_query = f"INSERT INTO capitalbikeshare (ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                session.execute(cf_query, (
                ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name,
                end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual))
                data_element_counter = data_element_counter + 1
                print('Load success ', data_element_counter, ' data elements from file ', file)
                print('\n')
        print('Congratulations! Total ', data_element_counter, ' data elements total was loaded from file ', file)
        cluster.shutdown()