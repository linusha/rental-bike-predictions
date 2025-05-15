# %%
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import time
import os
import glob
import shutil
import requests

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

locs = pd.read_csv("raw/Capital_Bikeshare_Locations.csv")
locs["TZ"] = "auto"  # automatically infer timezone from location

# define time range here
FROM = "2023-01-01"
TO = "2023-12-31"

# Home assistant instance
HA_TOKEN = os.environ.get("HA_TOKEN")
HA_URL = os.environ.get("HA_URL")


def new_ip(token, ha_url):

    print("Retrieving new IP")

    url = ha_url + "/api/services/automation/trigger"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    data = {"entity_id": "automation.fritz_box_new_ip"}

    try:
        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            print("New IP triggered successfully.")

        else:
            print(f"Failed to trigger new IP. Status code: {response.status_code}")
            print(f"Response: {response.text}")

            return False

    except requests.exceptions.RequestException as e:
        print(f"Error triggering new IP: {e}")
        return False

    return True


# Create checkpoint directory if it doesn't exist
checkpoint_dir = "checkpoints/weather"
os.makedirs(checkpoint_dir, exist_ok=True)

# Check for existing checkpoints
existing_checkpoints = glob.glob(f"{checkpoint_dir}/location_*.parquet")
processed_locations = set(
    [int(os.path.basename(f).split("_")[1].split(".")[0]) for f in existing_checkpoints]
)
print(f"Found {len(processed_locations)} existing location checkpoints")

url = "https://archive-api.open-meteo.com/v1/archive"
# %%

# Process in batches of 10 location as request for all is too large
BATCH_SIZE = 100
all_hourly_dfs = []

# Check if we have a batch checkpoint
batch_checkpoint_file = "checkpoints/weather/batch_checkpoint.parquet"
if os.path.exists(batch_checkpoint_file):
    print(f"Loading existing batch checkpoint from {batch_checkpoint_file}")
    all_hourly_dfs = [pd.read_parquet(batch_checkpoint_file)]
else:
    # Only load individual location checkpoints if no batch checkpoint exists
    for loc_idx in processed_locations:
        if loc_idx < len(locs):  # Make sure the location index is valid
            checkpoint_file = f"{checkpoint_dir}/location_{loc_idx}.parquet"
            print(f"Loading checkpoint for location {loc_idx}")
            loc_df = pd.read_parquet(checkpoint_file)
            all_hourly_dfs.append(loc_df)

# Loop through location batches
for i in range(0, len(locs), BATCH_SIZE):
    batch_locs = locs.iloc[i : i + BATCH_SIZE]
    print(
        f"Processing batch {i//BATCH_SIZE + 1}/{-(-len(locs)//BATCH_SIZE)}, locations {i} to {min(i+BATCH_SIZE-1, len(locs)-1)}"
    )

    # Configuring params for just this batch
    # The order of variables in hourly or daily is important to assign them correctly below
    batch_params = {
        "latitude": list(batch_locs["LATITUDE"]),
        "longitude": list(batch_locs["LONGITUDE"]),
        "start_date": FROM,
        "end_date": TO,
        "daily": ["sunrise", "sunset"],
        "hourly": [
            "temperature_2m",
            "weather_code",
            "rain",
            "precipitation",
            "snowfall",
            "cloud_cover",
            "wind_gusts_10m",
            "wind_speed_10m",
        ],
        "timezone": list(batch_locs["TZ"]),
    }

    # Make API request for this batch
    try:
        batch_responses = openmeteo.weather_api(url, params=batch_params)
    except Exception as e:

        if "limit exceeded" in str(e).lower():
            print("Limit exceeded. Have to get new IP.")

            success = new_ip(HA_TOKEN, HA_URL)

            if success:
                print("Retrying batch request after new IP")
                try:
                    batch_responses = openmeteo.weather_api(url, params=batch_params)
                except Exception as e:
                    print(
                        f"Error fetching data for batch {i//BATCH_SIZE + 1} after new IP: {e}"
                    )
                    continue
            else:
                print("Failed to trigger new IP, skipping this batch")
                continue

        else:
            print(f"Error fetching data for batch {i//BATCH_SIZE + 1}: {e}")
            raise

    # Process each location in the batch
    for j, response in enumerate(batch_responses):
        loc_idx = i + j

        # Skip if we already processed this location
        if loc_idx in processed_locations:
            print(f"Skipping already processed location {loc_idx}")
            continue

        # Process hourly data (code from openmeteo)
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_weather_code = hourly.Variables(1).ValuesAsNumpy()
        hourly_rain = hourly.Variables(2).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(3).ValuesAsNumpy()
        hourly_snowfall = hourly.Variables(4).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(5).ValuesAsNumpy()
        hourly_wind_gusts_10m = hourly.Variables(6).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(7).ValuesAsNumpy()

        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )
        }

        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["weather_code"] = hourly_weather_code
        hourly_data["rain"] = hourly_rain
        hourly_data["precipitation"] = hourly_precipitation
        hourly_data["snowfall"] = hourly_snowfall
        hourly_data["cloud_cover"] = hourly_cloud_cover
        hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
        hourly_data["wind_speed_10m"] = hourly_wind_speed_10m

        # Process daily data (code from openmeteo)
        daily = response.Daily()
        daily_sunrise = daily.Variables(0).ValuesInt64AsNumpy()
        daily_sunset = daily.Variables(1).ValuesInt64AsNumpy()

        daily_data = {
            "date": pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left",
            )
        }

        daily_data["sunrise"] = daily_sunrise
        daily_data["sunset"] = daily_sunset

        # Create dataframes
        hourly_df = pd.DataFrame(data=hourly_data)
        daily_df = pd.DataFrame(data=daily_data)

        # Add location information
        hourly_df["location_id"] = batch_locs.iloc[j]["STATION_ID"]  # Add station ID for later reference
        hourly_df["latitude"] = batch_locs.iloc[j]["LATITUDE"]
        hourly_df["longitude"] = batch_locs.iloc[j]["LONGITUDE"]

        # Join daily data to hourly data
        # Extract just the date part from the datetime for joining
        hourly_df["date_only"] = hourly_df["date"].dt.date
        daily_df["date_only"] = daily_df["date"].dt.date

        # Merge hourly and daily dataframes
        merged_df = pd.merge(
            hourly_df,
            daily_df[["date_only", "sunrise", "sunset"]],
            on="date_only",
            how="left",
        )

        # Drop the temporary date_only column
        merged_df.drop("date_only", axis=1, inplace=True)

        # Save checkpoint for this location
        checkpoint_file = f"{checkpoint_dir}/location_{loc_idx}.parquet"
        merged_df.to_parquet(checkpoint_file, index=False)
        processed_locations.add(loc_idx)

        # Add to list of dataframes
        all_hourly_dfs.append(merged_df)

        print(f"Processed location {loc_idx} with {len(merged_df)} hourly records")

        # Save batch checkpoint every 15 locations
        if len(all_hourly_dfs) % 15 == 0:
            print("Saving batch checkpoint...")
            batch_df = pd.concat(all_hourly_dfs, ignore_index=True)
            batch_df.to_parquet(batch_checkpoint_file, index=False)

        time.sleep(0.1)

# Combine all location dataframes
final_hourly_df = pd.concat(all_hourly_dfs, ignore_index=True)
print(
    f"Final hourly dataframe contains {len(final_hourly_df)} rows from {len(all_hourly_dfs)} locations"
)

# Save to parquet
final_hourly_df.to_parquet(
    "final/weather_hourly_all_locations_2023.parquet",
    index=False,
    compression="brotli",
)

# Clean up checkpoints
shutil.rmtree(checkpoint_dir)
print("Checkpoints cleaned up")