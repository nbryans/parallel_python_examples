import datetime
import pandas as pd
import multiprocessing

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

def process_chunk(df):
    cleaned_taxi = clean_data(df)
    cleaned_taxi = convert_dates(cleaned_taxi)
    cleaned_taxi = calculate_trip_duration(cleaned_taxi)
    return cleaned_taxi

if __name__ == "__main__":
    source_csv = "/mnt/c/Users/natha/code/ParallelPython/data/yellow_tripdata_2015-01.csv"

    taxi = read_csv(source_csv)

    # Split the DataFrame into chunks (adjust num_processes as needed)
    num_processes = 4
    chunks = [taxi[i::num_processes] for i in range(num_processes)]
    del taxi

    # Create a pool of processes
    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(process_chunk, chunks)
        # pool.map(process_chunk, chunks)

    # Concatenate the results
    cleaned_taxi = pd.concat(results)

    print("Processing finished.")