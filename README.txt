/*
 * 	README.txt
 *
 *  Created on: October, 13th 2023
 *  Author: LEDAT
 */
 
/*[STEP 1]*/
CREATE KEYSPACE mykeyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; --THIS CONFIG FOR NORMAL DATA TO TEST CASSANDRA DB

/*[STEP 2]*/
USE mykeyspace;

/*[STEP 3]*/
CREATE TABLE capitalbikeshare (
    ride_id text PRIMARY KEY,
    rideable_type text,
    started_at timestamp,
    ended_at timestamp,
    start_station_name text,
    start_station_id text,
    end_station_name text,
    end_station_id text,
    start_lat text,
    start_lng text,
    end_lat text,
    end_lng text,
    member_casual text,
    bike_number text

); ---FOR BIKE SHARE BIG-DATA

/*[STEP 4]*/
/*Add csv file into "csv_file" folder and run cassandra_load_csv.py in your local*/