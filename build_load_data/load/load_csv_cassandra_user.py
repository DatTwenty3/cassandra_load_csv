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
# directory_path = os.path.join(os.path.dirname(__file__), "build_load_data/result_user")
directory_path = "build_load_data/result_user"
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
                user_id = row['user_id']
                user_name = row['user_name']
                country = row['country']
                sign_up_date = row['sign_up_date']

                cf_query = f"INSERT INTO userbikeshare (user_id, user_name, country, sign_up_date) VALUES (%s, %s, %s, %s)"
                batch.add(cf_query, (user_id, user_name, country, sign_up_date))
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