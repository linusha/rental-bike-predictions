import pandas as pd

# Helper function to clean station names
def clean_station_name(name_series):
    return (name_series
            .str.lower()
            .str.strip()
            .str.replace(r'\s+', ' ', regex=True)
            .str.replace(r'[^\w\s]', '', regex=True))

trips = pd.read_parquet('data/final/weather_trips_combined.parquet')
locations = pd.read_csv('data/raw/Capital_Bikeshare_Locations.csv')

# Clean column names
trips.columns = trips.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_')
locations.columns = locations.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_')

# Create a longer format of trips data with both start and end points
start_trips = trips.rename(columns={'start_station': 'station_name'})
start_trips['row_type'] = 'start'

end_trips = trips.rename(columns={'end_station': 'station_name'})
end_trips['row_type'] = 'end'

# Bring start and end trips together
trips_long = pd.concat([start_trips, end_trips])

trips_long['station_name_clean'] = clean_station_name(trips_long['station_name'])
locations['station_name_clean'] = clean_station_name(locations['name'])

# Merge trips with locations
trips_locations = trips_long.merge(locations,
                                   how='left',
                                   on='station_name_clean')

stations_in_trips = set(trips_long['station_name_clean'].unique())
stations_in_locations = set(locations['station_name_clean'].unique())

# Stations in locations but not in trips
locations_only = stations_in_locations - stations_in_trips
print(f"\nUnique stations in locations but not in trips: {len(locations_only)} out of {len(stations_in_locations)} ({len(locations_only) / len(stations_in_locations):.2%})")

# Stations in trips but not in locations
trips_only = stations_in_trips - stations_in_locations
print(f"Unique stations in trips but not in locations: {len(trips_only)} out of {len(stations_in_trips)} ({len(trips_only) / len(stations_in_trips):.2%})")

trips_locations.to_parquet('data/final/weather_trips_locations_combined.parquet', compression='brotli')