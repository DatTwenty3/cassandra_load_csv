"""
cassandra_query_lib.py
Created on Sun October 15 2023
Author: LEDAT
"""
from cassandra.cluster import Cluster

global cluster
global session

def connect_to_cassandra(keyspace):
    global cluster
    global session
    # Initialize a connection to the Cassandra cluster
    cluster = Cluster(['localhost'])
    # Connect to the specific keyspace
    session = cluster.connect(keyspace)
    return session

def close_cassandra_connection():
    session.shutdown()
    cluster.shutdown()

def get_ride_id(ride_id):
    try:
        query = f"SELECT * FROM capitalbikeshare WHERE ride_id = '{ride_id}'"
        rows = session.execute(query)
        return rows
    except Exception as e:
        print(f"Error: {str(e)}")
        return str(e)

def get_info_station(station_name):
    try:
        query = f"SELECT * FROM capitalbikeshare WHERE end_station_name = '{station_name}' ALLOW FILTERING"
        rows = session.execute(query)
        return rows
    except Exception as e:
        print(f"Error: {str(e)}")
        return str(e)

def get_rideable_type_in_station(station_name):
    try:
        query = f"SELECT rideable_type FROM capitalbikeshare WHERE end_station_name = '{station_name}' ALLOW FILTERING"
        rows = session.execute(query)
        rideable_types = set()
        for row in rows:
            rideable_types.add(row.rideable_type)
        rideable_types = list(rideable_types)
        return rideable_types
    except Exception as e:
        print(f"Error: {str(e)}")
        return str(e)

def get_type_member_in_station(type_member,station_name):
    try:
        query = f"SELECT * FROM capitalbikeshare WHERE end_station_name = '{station_name}' AND member_casual = '{type_member}' ALLOW FILTERING"
        rows = session.execute(query)
        return rows
    except Exception as e:
        print(f"Error: {str(e)}")
        return str(e)

def insert_ride_data(ride_id, member_casual, rideable_type):
    try:
        query = f"INSERT INTO capitalbikeshare (ride_id, member_casual, rideable_type) VALUES ('{ride_id}', '{member_casual}', '{rideable_type}')"
        session.execute(query)
        print("Insert SUCCESS!")
        print("Ride ID:", ride_id)
        print("Member Casual:", member_casual)
        print("Rideable Type:", rideable_type)
    except Exception as e:
        print(f"Error: {str(e)}")
        return str(e)

def get_ride_history(member_casual):
    try:
        query = f"SELECT member_casual, ride_id, rideable_type FROM capitalbikeshare WHERE member_casual = '{member_casual}' ALLOW FILTERING"
        rows = session.execute(query)
        return rows
    except Exception as e:
        print(f"Error: {str(e)}")
        return str(e)

def get_info_between_dates(start_date, end_date):
    try:
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S.%f%z')
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S.%f%z')
        query = f"SELECT * FROM capitalbikeshare WHERE ended_at >= '{start_date_str}' AND ended_at <= '{end_date_str}' ALLOW FILTERING"
        result = session.execute(query)
        info = [row for row in result]
        return info
    except Exception as e:
        print(f"Error: {str(e)}")
        return str(e)