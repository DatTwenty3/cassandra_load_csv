"""
load_csv_cassandra.py
Created on Sat October 14 2023
Author: LEDAT
"""
import os
import csv

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

data_element_counter = 0
total_data_element_counter_in_file = 0
total_data_element_counter = 0
batch = BatchStatement()
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

file_list = sorted(list_files_in_directory(directory_path))
file_cannot_add = 0
if file_list:
    for file in file_list:
        try:
            with open(os.path.join(directory_path, file), 'r', newline='') as csvfile:
                print ('CSV file:', file, 'adding...')
                reader = csv.DictReader(csvfile)
                max_batch_size = 100
                for row in reader:
                    ride_id = row['ride_id']
                    rideable_type = row['rideable_type']
                    started_at = row['started_at']
                    ended_at = row['ended_at']
                    start_station_name = row['start_station_name']
                    start_station_id = row['start_station_id']
                    end_station_name = row['end_station_name']
                    end_station_id = row['end_station_id']
                    start_lat = float(row['start_lat'])
                    start_lng = float(row['start_lng'])
                    end_lat = float(row['end_lat'])
                    end_lng = float(row['end_lng'])
                    member_casual = row['member_casual']

                    cf_query = f"INSERT INTO capitalbikeshare (ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    batch.add(cf_query, (ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual))
                    data_element_counter = data_element_counter + 1
                    total_data_element_counter_in_file = total_data_element_counter_in_file + 1
                    total_data_element_counter = total_data_element_counter + 1
                    if data_element_counter >= max_batch_size:
                        session.execute(batch)
                        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                        data_element_counter = 0
                        print('1 batch added with batch size', max_batch_size, 'and', total_data_element_counter_in_file, 'data element added from file', file)
                
            if data_element_counter:
                session.execute(batch)
                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                data_element_counter = 0
                print('1 batch added with batch size', max_batch_size, 'and', total_data_element_counter_in_file, 'data element added from file', file)
            total_data_element_counter_in_file = 0
            print('Congratulations! Total ', total_data_element_counter, ' data elements total was loaded')
        except Exception as e:
            print(f"Error: {str(e)}")
            file_cannot_add += 1
            print('file_cannot_add:', file_cannot_add)
            continue
cluster.shutdown()