import os
import requests
import zipfile
import pandas as pd

###
# DOWNLOADING LARGER BASE DATA IF NOT ALREADY PRESENT ON THE MACHINE
###
raw_data_path = "./data/raw"

csv_filenames = [
        "2011-capitalbikeshare-tripdata.csv",
        "2012Q1-capitalbikeshare-tripdata.csv",
        "2012Q2-capitalbikeshare-tripdata.csv",
        "2012Q3-capitalbikeshare-tripdata.csv",
        "2012Q4-capitalbikeshare-tripdata.csv",
    ]

for file in csv_filenames:
    csv_filepath = os.path.join(raw_data_path, file)
    zip_filename = "data.zip"  # Temporary zip file name
    zip_filepath = os.path.join(raw_data_path, zip_filename)

    if not os.path.exists(csv_filepath):
        print(f"{file} not found. Downloading Data...")
        ZIP_URL = "https://s3.amazonaws.com/capitalbikeshare-data/2011-capitalbikeshare-tripdata.zip" if "2011" in file else "https://s3.amazonaws.com/capitalbikeshare-data/2012-capitalbikeshare-tripdata.zip"
    
        response = requests.get(ZIP_URL)
        if response.status_code == 200:
            print(zip_filepath)
            with open(zip_filepath, "wb") as zip_file:
                print("lade lade schreibe schreibe")
                zip_file.write(response.content)
        
            print("Extracting contents...")
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall(raw_data_path)
        
            os.remove(zip_filepath)
        else:
            print(f"Failed to download data ({file}) with status code: {response.status_code}")
    else:
        print(f"{file} already exists. No action needed.")

###
# LOAD, MERGE AND PREPROCESSING
###

print("Loading Trip Data...")
Q1_2012 = pd.read_csv('data/raw/2012Q1-capitalbikeshare-tripdata.csv', parse_dates=['Start date', 'End date'])
Q2_2012 = pd.read_csv('data/raw/2012Q2-capitalbikeshare-tripdata.csv', parse_dates=['Start date', 'End date'])
Q3_2012 = pd.read_csv('data/raw/2012Q3-capitalbikeshare-tripdata.csv', parse_dates=['Start date', 'End date'])
Q4_2012 = pd.read_csv('data/raw/2012Q4-capitalbikeshare-tripdata.csv', parse_dates=['Start date', 'End date'])
print("Trip Data Loaded!")

print("Q1 2012 size:", len(Q1_2012))
print("Q2 2012 size:", len(Q2_2012))
print("Q3 2012 size:", len(Q3_2012))
print("Q4 2012 size:", len(Q4_2012))

trips_2012 = pd.concat([Q1_2012, Q2_2012, Q3_2012, Q4_2012], ignore_index=True)

expected_2012_size = len(Q1_2012) + len(Q2_2012) + len(Q3_2012) + len(Q4_2012)
print("\n2012 Merge Check:")
print(f"Expected size: {expected_2012_size}")
print(f"Actual size: {len(trips_2012)}")
print(f"2012 merge successful: {expected_2012_size == len(trips_2012)}")

trips_2011 = pd.read_csv('data/raw/2011-capitalbikeshare-tripdata.csv', parse_dates=['Start date', 'End date'])
print("\n2011 data size:", len(trips_2011))

trips = pd.concat([trips_2011, trips_2012], ignore_index=True)

expected_total_size = len(trips_2011) + len(trips_2012)
print("\nFinal Merge Check:")
print(f"Expected size: {expected_total_size}")
print(f"Actual size: {len(trips)}")
print(f"Final merge successful: {expected_total_size == len(trips)}")

print("\nColumn Consistency Check:")
print(f"2011 columns: {sorted(trips_2011.columns)}")
print(f"2012 columns: {sorted(trips_2012.columns)}")
print(f"Final columns: {sorted(trips.columns)}")
print(f"Column consistency: {sorted(trips_2011.columns) == sorted(trips_2012.columns) == sorted(trips.columns)}")

# TRIP DATA
trips.columns = trips.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_')

trips['start_yr'] = trips['start_date'].dt.year
trips['start_yr_bin'] = trips['start_date'].dt.year - 2011
trips['start_mnth'] = trips['start_date'].dt.month
trips['start_day'] = trips['start_date'].dt.day
trips['start_hr'] = trips['start_date'].dt.hour

trips['end_yr'] = trips['end_date'].dt.year
trips['end_yr_bin'] = trips['end_date'].dt.year - 2011
trips['end_mnth'] = trips['end_date'].dt.month
trips['end_day'] = trips['end_date'].dt.day
trips['end_hr'] = trips['end_date'].dt.hour

# WEATHER DATA
weather = pd.read_csv('data/raw/weather-hour.csv', parse_dates=['dteday'])

# we want the actual year
weather['yr_bin'] = weather['yr']
weather['yr'] = weather['dteday'].dt.year
weather['day'] = weather['dteday'].dt.day

print("Merging trip and weather data...")
df = pd.merge(
    trips, weather,
    left_on=['start_yr', 'start_mnth', 'start_day', 'start_hr'],
    right_on=['yr', 'mnth', 'day', 'hr'],
    how='left',
    validate="many_to_one"
)

df.drop(columns=['yr', 'day', 'hr', 'yr_bin', 'start_yr_bin', 'end_yr_bin'], inplace=True)

print("Trip-Weather Merge Check:")
print(f"Trips count: {len(trips)}")
print(f"Merged dataset count: {len(df)}")
print(f"Merge preserves all trips: {len(trips) == len(df)}")

df.to_parquet('data/final/weather_trips_combined.parquet', compression='brotli')