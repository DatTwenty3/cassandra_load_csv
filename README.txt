/*
 * 	README.txt
 *
 *  Created on: October, 13th 2023
 *  Author: LEDAT
 */
 -------------------------IN CQLSH OF CASSANDRA-------------------------
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
    bike_number text,
	user_id text

); /*Table for capitalshare log table*/

CREATE TABLE stationbikeshare (
    station_id text PRIMARY KEY,
    station_name text,
    city text,
    country text
); /*Table for stationbikeshare table*/

CREATE TABLE bikebikeshare (
    bike_number text PRIMARY KEY,
    rideable_type text,
    city text,
    introduced_date timestamp
); /*Table for bikebikeshare table*/

CREATE TABLE userbikeshare (
    bike_number text PRIMARY KEY,
    rideable_type text,
    city text,
    introduced_date timestamp
); /*Table for userbikeshare table*/

 -------------------------IN LOCAL YOUR COMPUTER-------------------------
-- Create a Virtual Environment
python -m venv venv
-- Install Dependencies
pip install -r requirements.txt
-- Add data (csv file) into "data_csv" folder and run load_csv_cassandra.py to load in to capitalbikeshare table
python load_csv_cassandra.py
-- Add data (csv file) into "build_load_data\result_bike" folder and run load_csv_cassandra_bike.py to load in to bikebikeshare table
python build_load_data\load\load_csv_cassandra_bike.py
-- Add data (csv file) into "build_load_data\result_station" folder and run load_csv_cassandra_station.py to load in to stationbikeshare table
python build_load_data\load\load_csv_cassandra_station.py
-- Add data (csv file) into "build_load_data\result_user" folder and run load_csv_cassandra_user.py to load in to userbikeshare table
python build_load_data\load\load_csv_cassandra_user.py

 -------------------------GOOGLE DRIVE LINK-------------------------
--Link Google Drive to Download CSV File of project: https://drive.google.com/drive/folders/1DXYWnltWdsCKhyO_6ZWqOySNhvJHHbgM?usp=sharing