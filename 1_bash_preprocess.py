import datetime
import pandas as pd
import sys

def time_it(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.datetime.now()
        print(f"{func.__name__} started")
        result = func(*args, **kwargs)
        end_time = datetime.datetime.now()
        print(f"{func.__name__} finished: {end_time - start_time}")
        return result
    return wrapper

@time_it
def read_csv(csv_file):
    return pd.read_csv(csv_file)

@time_it
def clean_data(df):
    return df[(df['passenger_count'] != 0) & 
              (df['trip_distance'] != 0) &
              (df['fare_amount'] >= 0) &
              (df['total_amount'] >= 0)].copy()

@time_it
def convert_dates(df):
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    return df

@time_it
def calculate_trip_duration(df):
    df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime'])
    return df

if __name__ == "__main__":
    start_time = datetime.datetime.now()

    if len(sys.argv) != 2:
        print("Usage: python _serial_preprocess.py <csv_file>")
        sys.exit(1)

    source_csv = sys.argv[1]  # Get CSV filepath from command-line argument
    source_filename = source_csv.split("/")[-1]
    output_path = f"/mnt/c/Users/natha/code/ParallelPython/bash_output/processed_output_{source_filename}"
    # print(output_path)

    taxi = read_csv(source_csv)
    cleaned_taxi = clean_data(taxi)
    cleaned_taxi = convert_dates(cleaned_taxi)
    cleaned_taxi = calculate_trip_duration(cleaned_taxi)
    print("Processing finished. Writing to file...")

    #cleaned_taxi.to_csv(output_path, index=False)  # Output to a file
    print(f"File {output_path} written successfully. Time Elapsed: {datetime.datetime.now() - start_time}")
