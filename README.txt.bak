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
    start_lat float,
    start_lng float,
    end_lat float,
    end_lng float,
    member_casual text
); ---FOR BIKE SHARE BIG-DATA

/*[STEP 4]*/
/*Create a folder named data_csv and place the data01.csv file in that folder*/
/*Import this folder into "File" tag in Cassandra docker*/

/*[STEP 3]*/
/*RUN IT IN CQLSH TO IMPORT CSV*/
COPY mykeyspace.capitalbikeshare (ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual) from '/data_csv/data01.csv' WITH HEADER = TRUE AND MAXATTEMPTS = 10  AND MAXBATCHSIZE = 10 AND INGESTRATE = 10000 AND CHUNKSIZE = 50;