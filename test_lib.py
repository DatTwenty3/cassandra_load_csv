"""
test_lib.py
Created on Sun October 15 2023
Author: LEDAT
"""
import cassandra_query_lib as cql
from datetime import datetime

cql.connect_to_cassandra('mykeyspace')

"""
#Test get_ride_id()
rows = cql.get_ride_id('FE175CF5182FE003')
print("Test for get_ride_id()\n")
print("--------------------------\n")
for row in rows:
    print("Ride ID:", row.ride_id)
    print("Start Station:", row.start_station_name)
    print("End Station:", row.end_station_name)
    print("\n")
"""

"""
#Test get_info_station()
print("Test for get_info_station()\n")
print("--------------------------\n")
rows = cql.get_info_station('New Hampshire Ave & 24th St NW')
for row in rows:
    print("Ride ID:", row.ride_id)
    print("Member Casual:", row.member_casual)
    print("Ridealbe type:", row.rideable_type)
    print("\n")
"""

"""
#Test get_rideable_type_in_station()
print("Test for get_rideable_type_in_station()\n")
print("--------------------------\n")
rows = cql.get_rideable_type_in_station('Hardy Rec Center')
print("Already bike in station is:\n")
for row in rows:
    print(row)
    print("\n")
"""

"""
#Test get_type_member_in_station()
print("Test for get_type_member_in_station()\n")
print("--------------------------\n")
member = 'casual'
station_name = 'New Hampshire Ave & 24th St NW'
rows = cql.get_type_member_in_station(member,station_name)
print("Member type", member, "in", station_name, "station is:\n")
for row in rows:
    print("Ride ID:", row.ride_id)
    print("\n")
"""

"""
#Test insert_ride_data()
print("Test for insert_ride_data()\n")
print("--------------------------\n")

ride_id = 'ABC1234657'
member_casual = 'casual'
rideable_type = 'classic_bike'
cql.insert_ride_data(ride_id, member_casual, rideable_type)
print("--------------------------\n")

rows = cql.get_ride_id('ABC1234657')
for row in rows:
    print("Ride ID:", row.ride_id)
    print("Member Casual:", row.member_casual)
    print("Rideable Type:", row.rideable_type)
    print("\n")
"""

""""
#Test get_ride_history()
print("Test for get_ride_history()\n")
print("--------------------------\n")
member = 'casual'
rows = cql.get_ride_history(member)
for row in rows:
    print("Ride ID:", row.ride_id)
    print("Rideable Type:", row.rideable_type)
    print("\n")
"""

""""
#Test get_info_between_dates()
print("Test for filter_info_between_dates()\n")
print("--------------------------\n")
start_date = datetime(2023, 7, 1)
end_date = datetime(2023, 7, 15, 23, 59, 59)
rows = cql.get_info_between_dates(start_date, end_date)
for row in rows:
    print("Ride ID:", row.ride_id)
    print("Member Casual:", row.member_casual)
    print("Rideable Type:", row.rideable_type)
    print("Time:", row.ended_at)
    print("\n")
"""

"""
rows = cql.get_station_name()
for row in rows:
    print(row, "with ID", cql.get_station_id(row))
"""

"""
day = datetime(2023, 7, 1)
rows = cql.get_bike_day(day)
for row in rows:
    print("Ride ID:", row.ride_id, "|", row.started_at)
"""

ride_id = '123'
if cql.ride_id_is_exist(ride_id):
    print(ride_id, "OK")
else:
    print(ride_id, "not OK")

ride_id = 'FE175CF5182FE003'
if cql.ride_id_is_exist(ride_id):
    print(ride_id, "OK")
else:
    print(ride_id, "not OK")