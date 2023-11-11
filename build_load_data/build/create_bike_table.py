import pandas as pd
import os
import numpy as np

directory_path = os.path.join(os.path.dirname(__file__), "combined_csv_new.csv")
table = pd.read_csv(directory_path, engine="pyarrow")
bike = table[["bike_number", "rideable_type"]].drop_duplicates(subset=["bike_number"]).reset_index(drop=True)
print(bike)
print(len(bike))

df = bike
country = ["US", "CAN"]
date = ["2023-05-12", "2023-01-01", "2023-02-10", "2023-03-25", "2023-04-12", "2023-06-18", "2023-07-07"]
for i in range(0, len(df), 5):
    # Get the current block of 10 rows using iloc
    max = i+5
    if len(df) < max:
        max = len(df)
    block_index = range(i, max)
    
    # Assign values to a specific column in the current block of 10 rows
    # For example, assigning 'X' to Column2
    country_name = np.random.choice(country)
    df.loc[block_index, 'country'] = country_name  # Use .loc to modify the original DataFrame
    df.loc[block_index, 'introduced_date'] = np.random.choice(date)
bike.to_csv("build_load_data/result_bike/bike_station_2023.csv", index=False)