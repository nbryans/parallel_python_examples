from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import TimestampType
import datetime


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
def read_csv(spark, csv_file):
    return spark.read.csv(csv_file, header=True, inferSchema=True)


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
    return df.withColumn(
        'tpep_pickup_datetime',
        to_timestamp('tpep_pickup_datetime')
    ).withColumn(
        'tpep_dropoff_datetime',
        to_timestamp('tpep_dropoff_datetime')
    )


@time_it
def calculate_trip_duration(df):
    return df.withColumn(
        'trip_duration',
        col('tpep_dropoff_datetime').cast(TimestampType()) - col('tpep_pickup_datetime').cast(TimestampType())
    )


if __name__ == "__main__":
    # start_time = datetime.datetime.now()
    source_csv = "/mnt/c/Users/natha/code/ParallelPython/data/yellow_tripdata_2015-01.csv"

    spark = SparkSession.builder.appName("TaxiPreprocessing").getOrCreate()

    taxi = read_csv(spark, source_csv)

    start_time = datetime.datetime.now()

    cleaned_taxi = clean_data(taxi)
    cleaned_taxi = convert_dates(cleaned_taxi)
    cleaned_taxi = calculate_trip_duration(cleaned_taxi)

    # If you need to save the output
    # cleaned_taxi.write.csv("output_path", header=True)

    spark.stop()
    print(f"Spark Processing finished. Time Elapsed: {datetime.datetime.now() - start_time}")

