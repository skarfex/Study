import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def process(spark, flights_path, airlines_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param result_path: путь с результатами преобразований
    """
    flights_df = spark.read.parquet(flights_path) \
        .select(f.col('AIRLINE').alias('AIRLINE_SHORT'),
                f.col('TAIL_NUMBER'),
                f.col('DIVERTED'),
                f.col('CANCELLED'),
                f.col('DISTANCE'),
                f.col('AIR_TIME'),
                f.col('CANCELLATION_REASON'))
    airlines_df = spark.read.parquet(airlines_path) \
        .select(f.col('IATA_CODE'),
                f.col('AIRLINE').alias('AIRLINE_NAME'))


    join_df = flights_df \
        .join(other=airlines_df,
              on=airlines_df['IATA_CODE'] == f.col('AIRLINE_SHORT'),
              how='inner') \
        .groupBy(airlines_df['AIRLINE_NAME']) \
        .agg(f.sum(f.when((flights_df['DIVERTED'] == 0) & (flights_df['CANCELLED'] == 0), 1).otherwise(0)).alias('correct_count'),
             f.sum(f.col('DIVERTED')).alias('diverted_count'),
             f.sum(f.col('CANCELLED')).alias('cancelled_count'),
             f.avg(f.col('DISTANCE')).alias('avg_distance'),
             f.avg(f.col('AIR_TIME')).alias('avg_air_time'),
             f.sum(f.when(flights_df['CANCELLATION_REASON'] == 'A', 1).otherwise(0)).alias('airline_issue_count'),
             f.sum(f.when(flights_df['CANCELLATION_REASON'] == 'B', 1).otherwise(0)).alias('weather_issue_count'),
             f.sum(f.when(flights_df['CANCELLATION_REASON'] == 'C', 1).otherwise(0)).alias('nas_issue_count'),
             f.sum(f.when(flights_df['CANCELLATION_REASON'] == 'D', 1).otherwise(0)).alias('security_issue_count')) \
        .select(f.col('AIRLINE_NAME'),
                f.col('correct_count'),
                f.col('diverted_count'),
                f.col('cancelled_count'),
                f.col('avg_distance'),
                f.col('avg_air_time'),
                f.col('airline_issue_count'),
                f.col('weather_issue_count'),
                f.col('nas_issue_count'),
                f.col('security_issue_count'))

    join_df.show()
    join_df.write.parquet(result_path)


def main(flights_path, airlines_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob5').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    result_path = args.result_path
    main(flights_path, airlines_path, result_path)

"""
для запуска из консоли
python PySparkJob5.py --flights_path=./data/flights.parquet --airlines_path=./data/airlines.parquet --result_path=result5
"""