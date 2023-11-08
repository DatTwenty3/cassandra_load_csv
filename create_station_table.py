import pandas as pd
import os
import numpy as np

directory_path = os.path.join(os.path.dirname(__file__), "data_csv")
table_list = []
for i in range (1,8):
    file_name = f"20230{i}-capitalbikeshare-tripdata.csv"
    print(file_name)
    table_list.append(pd.read_csv(os.path.join(directory_path, file_name), engine="pyarrow"))
# print(len(pd.read_csv(os.path.join(directory_path, "202307-capitalbikeshare-tripdata.csv"), engine="pyarrow")))
table = pd.concat(table_list)
# print(len(table))
table_start = table[["start_station_id", "start_station_name"]].drop_duplicates()
table_end = table[["end_station_id", "end_station_name"]].drop_duplicates()
table_start = table_start.rename(columns={
    "start_station_id": "station_id",
    "start_station_name": "station_name",
})
table_end = table_end.rename(columns={
    "end_station_id": "station_id",
    "end_station_name": "station_name",
})
station = pd.concat([table_start, table_end]).drop_duplicates(subset=["station_id"]).reset_index(drop=True)
df = station
us_city = ["Birmingham", "Clanton", "Dothan", "Greenville", "Calexico"]
can_city = ["Edmonton", "Calgary", "Richmond", "Victoria", "Banff"]
city = us_city + can_city

for i in range(0, len(df), 5):
    # Get the current block of 10 rows using iloc
    max = i+5
    if len(df) < max:
        max = len(df)
    block_index = range(i, max)
    
    # Assign values to a specific column in the current block of 10 rows
    # For example, assigning 'X' to Column2
    city_name = np.random.choice(city)
    df.loc[block_index, 'city'] = city_name  # Use .loc to modify the original DataFrame
    if city_name in us_city:
        df.loc[block_index, 'country'] = "US"
    else:
        df.loc[block_index, 'country'] = "CAN"
print(df)
df.to_csv("station_table_2023.csv", index=False)

# print(len(station))
# print(station[station.duplicated(subset=["station_id"], keep=False)])
# print(table_start)
# print(table_end)
# print(table[["start_station_name", "start_station_id"]])