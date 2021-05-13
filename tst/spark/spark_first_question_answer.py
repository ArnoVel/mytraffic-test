import os
from pyspark.sql import SparkSession
from src.spark import spark_first_question as sfq

# get correct data path
dir_path = os.path.dirname(os.path.realpath(__file__))
data_path = os.path.join(dir_path, '../../data/data.csv')


if __name__ == '__main__':
    # init spark
    spark = SparkSession.builder \
        .master("local") \
        .appName("NYC Taxi data") \
        .getOrCreate()
    # get data into dataframe
    df = spark.read.format("csv").load(data_path, header=True)

    average_speed = sfq.compute_average_speed(df)

    average_speed.show()
