import pandas as pd

Q1_2012 = pd.read_csv('data/raw/2012Q1-capitalbikeshare-tripdata.csv')
Q3_2012 = pd.read_csv('data/raw/2012Q3-capitalbikeshare-tripdata.csv')
Q2_2012 = pd.read_csv('data/raw/2012Q2-capitalbikeshare-tripdata.csv')
Q4_2012 = pd.read_csv('data/raw/2012Q4-capitalbikeshare-tripdata.csv')

print("Q1 2012 size:", len(Q1_2012))
print("Q2 2012 size:", len(Q2_2012))
print("Q3 2012 size:", len(Q3_2012))

print("Q4 2012 size:", len(Q4_2012))
trips_2012 = pd.concat([Q1_2012, Q2_2012, Q3_2012, Q4_2012], ignore_index=True)
trips_2012.to_csv('data/2012-capitalbikeshare-tripdata.csv', index=False)

expected_2012_size = len(Q1_2012) + len(Q2_2012) + len(Q3_2012) + len(Q4_2012)
print("\n2012 Merge Check:")
print(f"Expected size: {expected_2012_size}")
print(f"Actual size: {len(trips_2012)}")
print(f"2012 merge successful: {expected_2012_size == len(trips_2012)}")

trips_2011 = pd.read_csv('data/raw/2011-capitalbikeshare-tripdata.csv')
print("\n2011 data size:", len(trips_2011))

trips = pd.concat([trips_2011, trips_2012], ignore_index=True)
trips.to_csv('data/2011-2012-capitalbikeshare-tripdata.csv', index=False)

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