import pandas as pd
import os
import numpy as np

# directory_path = os.path.join(os.path.dirname(__file__), "./../../data_csv/combined_csv_new.csv")
table = pd.read_csv("combined_csv_new.csv", engine="pyarrow")
# Generate random user IDs
user_ids = ['U00' + str(np.random.randint(1, 9999)).zfill(4) for _ in range(table.shape[0])]
table['user_id'] = user_ids
table.to_csv("combined_csv_bike_number_user_id.csv", index=False)