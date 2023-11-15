import pandas as pd
import os
import numpy as np

from datetime import datetime, timedelta
np.random.seed(42)

# Function to generate a list of random dates and times with a specific length
def generate_random_dates(start_date, end_date, length):
    random_dates = []
    for _ in range(length):
        random_date = start_date + timedelta(
            seconds=np.random.randint(0, int((end_date - start_date).total_seconds())))
        formatted_date = random_date.strftime("%Y-%m-%d")
        random_dates.append(formatted_date)
    return random_dates

# Define the start and end dates
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 7, 30)

# Specify the length of the column
column_length = 2000

# Generate a column of random dates and times
random_dates = generate_random_dates(start_date, end_date, column_length)

# Print the generated dates and times
print(random_dates)



directory_path = os.path.join(os.path.dirname(__file__), "./../../data_csv/combined_csv_new.csv")
table = pd.read_csv(directory_path, engine="pyarrow")
# Generate random user IDs
user_ids = ['U00' + str(np.random.randint(1, 9999)).zfill(4) for _ in range(table.shape[0])]
table['user_id'] = user_ids
print(table['user_id'])
asian_names = [
    "Aarav", "Aisha", "Amita", "Anaya", "Bao", "Chen", "Dae-Hyun", "Dao", "Divya", "Esra",
    "Farida", "Feng", "Gia", "Hanako", "Haruki", "Hikari", "Hiroshi", "Il-hyun", "Ji-eun", "Jia",
    "Jin", "Kai", "Kamala", "Kenji", "Kumiko", "Li Wei", "Lina", "Mai", "Manami", "Mei",
    "Min-jun", "Nori", "Nuan", "Osamu", "Priya", "Qiu", "Rahul", "Rina", "Rohit", "Sakura",
    "Sanjay", "Sarika", "Shan", "Shanti", "Shin", "Sora", "Subira", "Sumiko", "Takumi", "Tanaka",
    "Tariq", "Thi", "Tuan", "Uma", "Vijay", "Vivian", "Wei", "Xiu", "Xue", "Yasmine",
    "Ying", "Yoko", "Yoshi", "Yuan", "Yui", "Zara", "Zhi", "Arun", "Ayesha", "Binh",
    "Chandra", "Dinh", "Donghai", "Etsuko", "Fumiko", "Gan", "Hanh", "Haruka", "Hideki",
    "Hitomi", "Huan", "Jiro", "Kazuki", "Kimiko", "Kwan", "Lei", "Li Mei", "Lin", "Lucia",
    "Makoto", "Mika", "Natsuki", "Noboru", "Phuong", "Qi", "Ren", "Rong", "Ryo", "Satomi",
    "Seo-Yeon", "Shiro", "Tao", "Tian", "Tomoko", "Toshiro", "Vidya", "Xia", "Xiu Mei", "Yoshiro",
    "Yuan", "Zhen"
]

english_names = [
    "Liam", "Olivia", "Noah", "Emma", "Sophia", "Ava", "Isabella", "Mia", "Aiden", "Amelia",
    "Oliver", "Harper", "Elijah", "Evelyn", "James", "Abigail", "Benjamin", "Charlotte", "Lucas", "Luna",
    "Mason", "Lily", "Logan", "Chloe", "Alexander", "Layla", "Ethan", "Riley", "Jacob", "Zoe",
    "Michael", "Mila", "Daniel", "Aria", "Henry", "Scarlett", "Jackson", "Lillian", "Sebastian", "Nora",
    "Jack", "Zara", "William", "Ellie", "Avery", "Hannah", "Ryan", "Addison", "David", "Victoria",
    "Muhammad", "Grace", "Matthew", "Stella", "Aarav", "Sofia", "Dylan", "Madison", "Nathan", "Lila",
    "Carter", "Leah", "Luke", "Hazel", "Jayden", "Aubrey", "John", "Ellie", "Grayson", "Bella",
    "Nicholas", "Natalie", "Isaac", "Lily", "Julian", "Aurora", "Caleb", "Zara", "Sebastian", "Violet",
    "Chase", "Ariana", "Wyatt", "Skylar", "Jonathan", "Claire", "Jaxon", "Sophie", "Levi", "Sadie",
    "Christian", "Anna", "Hunter", "Lucy", "Isaiah", "Brielle", "Owen", "Quinn", "Eli", "Penelope",
    "Aaron", "Maya", "Charles", "Luna", "Grayson", "Emily", "Josiah", "Aaliyah", "Cameron", "Paisley"
]


user_table = pd.DataFrame()
user_table["user_id"] = table['user_id'].drop_duplicates()
user_table["user_name"] = np.random.choice(english_names+asian_names, size=len(user_table))
user_table["country"] = np.random.choice(["US", "CAN"], size=len(user_table))
user_table["sign_up_date"] = np.random.choice(random_dates, size=len(user_table))
user_table['user_password'] = user_table['user_name'].apply(lambda x: x[2:] + "@123")
user_table.to_csv("build_load_data/result_user/user_table_2023.csv", index=False)