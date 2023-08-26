import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def process(spark, flights_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """
    top_airport_with_delay = spark.read.parquet(flights_path) \
        .groupBy(f.col('ORIGIN_AIRPORT')) \
        .agg(f.min(f.col('DEPARTURE_DELAY')).alias('min_delay'),
             f.avg(f.col('DEPARTURE_DELAY')).alias('avg_delay'),
             f.max(f.col('DEPARTURE_DELAY')).alias('max_delay'),
             f.corr(f.col('DEPARTURE_DELAY'),f.col('DAY_OF_WEEK')).alias('corr_delay2day_of_week')) \
        .select(f.col('ORIGIN_AIRPORT'),
                f.col('avg_delay'),
                f.col('min_delay'),
                f.col('max_delay'),
                f.col('corr_delay2day_of_week')) \
        .orderBy(f.col('max_delay').desc()) \
        .where(f.col('max_delay') > 1000)

    top_airport_with_delay.show(truncate=False)
    top_airport_with_delay.write.parquet(result_path)


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob3').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
