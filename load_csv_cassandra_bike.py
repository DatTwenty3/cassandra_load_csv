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
directory_path = "build_load_data/result_bike"
# directory_path = os.path.join(os.path.join(os.path.dirname(__file__), os.pardir), "build_load_data/result_bike")
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
            print ('CSV file:', file, 'adding...')
            reader = csv.DictReader(csvfile)
            max_batch_size = 100
            for row in reader:
                bike_number = row['bike_number']
                rideable_type = row['rideable_type']
                country = row['country']
                introduced_date = row['introduced_date']

                cf_query = f"INSERT INTO bikeshare (bike_number, rideable_type, country, introduced_date) VALUES (%s, %s, %s, %s)"
                batch.add(cf_query, (bike_number, rideable_type, country, introduced_date))
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
cluster.shutdown()