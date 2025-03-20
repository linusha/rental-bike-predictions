#%%
import pandas as pd

Q1_2012 = pd.read_csv('data/raw/2012Q1-capitalbikeshare-tripdata.csv', parse_dates=['Start date', 'End date'])
Q2_2012 = pd.read_csv('data/raw/2012Q2-capitalbikeshare-tripdata.csv', parse_dates=['Start date', 'End date'])
Q3_2012 = pd.read_csv('data/raw/2012Q3-capitalbikeshare-tripdata.csv', parse_dates=['Start date', 'End date'])
Q4_2012 = pd.read_csv('data/raw/2012Q4-capitalbikeshare-tripdata.csv', parse_dates=['Start date', 'End date'])

print("Q1 2012 size:", len(Q1_2012))
print("Q2 2012 size:", len(Q2_2012))
print("Q3 2012 size:", len(Q3_2012))
print("Q4 2012 size:", len(Q4_2012))

trips_2012 = pd.concat([Q1_2012, Q2_2012, Q3_2012, Q4_2012], ignore_index=True)
trips_2012.to_csv('data/processed/2012-capitalbikeshare-tripdata.csv', index=False)

expected_2012_size = len(Q1_2012) + len(Q2_2012) + len(Q3_2012) + len(Q4_2012)
print("\n2012 Merge Check:")
print(f"Expected size: {expected_2012_size}")
print(f"Actual size: {len(trips_2012)}")
print(f"2012 merge successful: {expected_2012_size == len(trips_2012)}")

trips_2011 = pd.read_csv('data/raw/2011-capitalbikeshare-tripdata.csv', parse_dates=['Start date', 'End date'])
print("\n2011 data size:", len(trips_2011))

trips = pd.concat([trips_2011, trips_2012], ignore_index=True)
trips.to_parquet('data/final/2011-2012-capitalbikeshare-tripdata.parquet', compression='snappy')

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

weather = pd.read_csv('data/raw/weather-hour.csv',
                    parse_dates=['dteday'])

# clean column names // weather already cleaned
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

# we want the actual year
weather['yr_bin'] = weather['yr']
weather['yr'] = weather['dteday'].dt.year
weather['day'] = weather['dteday'].dt.day

#%%
print("\nMerging trip and weather data...")
print(f"Trips data shape before merge: {trips.shape}")
print(f"Weather data shape: {weather.shape}")

df = pd.merge(
    trips, weather,
    left_on=['start_yr', 'start_mnth', 'start_day', 'start_hr'],
    right_on=['yr', 'mnth', 'day', 'hr'],
    how='left',
    validate="many_to_one"
)

#%%

print("\nTrip-Weather Merge Check:")
print(f"Trips count: {len(trips)}")
print(f"Merged dataset count: {len(df)}")
print(f"Merge preserves all trips: {len(trips) == len(df)}")

df.to_parquet('data/final/weather_trips_combined.parquet', compression='snappy')