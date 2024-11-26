import datetime
import polars as pl
from polars import col

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
    return pl.read_csv(csv_file)  # Use polars.read_csv to read the CSV

@time_it
def clean_data(df):
    return df.filter(
        (col('passenger_count') != 0) &
        (col('trip_distance') != 0) &
        (col('fare_amount') >= 0) &
        (col('total_amount') >= 0)
    )

@time_it
def convert_dates(df):
    df = df.with_columns([
        pl.col('tpep_pickup_datetime').str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S'),
        pl.col('tpep_dropoff_datetime').str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S')
    ])
    return df

@time_it
def calculate_trip_duration(df):
    df = df.with_columns(
        (pl.col('tpep_dropoff_datetime') - pl.col('tpep_pickup_datetime')).alias('trip_duration')
    )
    return df

if __name__ == "__main__":
    source_csv = "/mnt/c/Users/natha/code/ParallelPython/data/yellow_tripdata_2015-01.csv"

    taxi = read_csv(source_csv)
    start_time = datetime.datetime.now()

    cleaned_taxi = clean_data(taxi)
    cleaned_taxi = convert_dates(cleaned_taxi)
    cleaned_taxi = calculate_trip_duration(cleaned_taxi)

    print(f"Polars Processing finished. Time Elapsed: {datetime.datetime.now() - start_time}")