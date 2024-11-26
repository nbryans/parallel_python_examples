import datetime
import dask.dataframe as dd

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
    return dd.read_csv(csv_file)  # Use dask.dataframe to read the CSV

@time_it
def clean_data(df):
    return df[(df['passenger_count'] != 0) &
              (df['trip_distance'] != 0) &
              (df['fare_amount'] >= 0) &
              (df['total_amount'] >= 0)].copy()

@time_it
def convert_dates(df):
    df['tpep_pickup_datetime'] = dd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = dd.to_datetime(df['tpep_dropoff_datetime'])
    return df

@time_it
def calculate_trip_duration(df):
    df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime'])
    return df

if __name__ == "__main__":
    # start_time = datetime.datetime.now()
    source_csv = "/mnt/c/Users/natha/code/ParallelPython/data/yellow_tripdata_2015-01.csv"

    taxi = read_csv(source_csv)
    start_time = datetime.datetime.now()

    cleaned_taxi = clean_data(taxi)
    cleaned_taxi = convert_dates(cleaned_taxi)
    cleaned_taxi = calculate_trip_duration(cleaned_taxi)

    # Trigger computation to get the result
    cleaned_taxi = cleaned_taxi.compute()
    # cleaned_taxi.visualize()

    print(f"Dask Processing finished. Time Elapsed: {datetime.datetime.now() - start_time}")