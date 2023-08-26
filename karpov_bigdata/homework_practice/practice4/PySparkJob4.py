import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def process(spark, flights_path, airlines_path, airports_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param airports_path: путь до датасета c аэропортами
    :param result_path: путь с результатами преобразований
    """
    flights_df = spark.read.parquet(flights_path) \
        .select(f.col('AIRLINE').alias('AIRLINE_SHORT'),
                f.col('TAIL_NUMBER'),
                f.col('ORIGIN_AIRPORT'),
                f.col('DESTINATION_AIRPORT'))
    airlines_df = spark.read.parquet(airlines_path) \
        .select(f.col('IATA_CODE'),
                f.col('AIRLINE').alias('AIRLINE_NAME'))
    airports_origin_df = spark.read.parquet(airports_path) \
        .select(f.col('IATA_CODE').alias('ORIGIN_IATA_CODE'),
                f.col('AIRPORT').alias('ORIGIN_AIRPORT_NAME'),
                f.col('COUNTRY').alias('ORIGIN_COUNTRY'),
                f.col('LATITUDE').alias('ORIGIN_LATITUDE'),
                f.col('LONGITUDE').alias('ORIGIN_LONGITUDE'))
    airports_destination_df = spark.read.parquet(airports_path) \
        .select(f.col('IATA_CODE').alias('DESTINATION_IATA_CODE'),
                f.col('AIRPORT').alias('DESTINATION_AIRPORT_NAME'),
                f.col('COUNTRY').alias('DESTINATION_COUNTRY'),
                f.col('LATITUDE').alias('DESTINATION_LATITUDE'),
                f.col('LONGITUDE').alias('DESTINATION_LONGITUDE'))

    join_df = flights_df \
        .join(other=airlines_df,
              on=airlines_df['IATA_CODE'] == f.col('AIRLINE_SHORT'),
              how='inner') \
        .join(other=airports_origin_df,
              on=airports_origin_df['ORIGIN_IATA_CODE'] == flights_df['ORIGIN_AIRPORT'],
              how='inner') \
        .join(other=airports_destination_df,
              on=airports_destination_df['DESTINATION_IATA_CODE'] == flights_df['DESTINATION_AIRPORT'],
              how='inner') \
        .select(airlines_df['AIRLINE_NAME'],
                flights_df['TAIL_NUMBER'],
                airports_origin_df['ORIGIN_COUNTRY'],
                airports_origin_df['ORIGIN_AIRPORT_NAME'],
                airports_origin_df['ORIGIN_LATITUDE'],
                airports_origin_df['ORIGIN_LONGITUDE'],
                airports_destination_df['DESTINATION_COUNTRY'],
                airports_destination_df['DESTINATION_AIRPORT_NAME'],
                airports_destination_df['DESTINATION_LATITUDE'],
                airports_destination_df['DESTINATION_LONGITUDE'])

    join_df.show()
    join_df.write.parquet(result_path)


def main(flights_path, airlines_path, airports_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, airports_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--airports_path', type=str, default='airports.parquet', help='Please set airports datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path, airlines_path, airports_path, result_path)
