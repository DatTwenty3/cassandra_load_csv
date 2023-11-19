# cassandra_load_csv project
## Create a Virtual Environment
`python -m venv venv`
## Install Dependencies
`pip install -r requirements.txt`
## Run load_csv_cassandra.py to load in to capitalbikeshare table
`python load_csv_cassandra.py`
## Run load_csv_cassandra_bike.py to load in to bikebikeshare table
`python build_load_data\load\load_csv_cassandra_bike.py`
## Run load_csv_cassandra_station.py to load in to stationbikeshare table
`python build_load_data\load\load_csv_cassandra_station.py`
## Run load_csv_cassandra_user.py to load in to userbikeshare table
`python build_load_data\load\load_csv_cassandra_user.py`