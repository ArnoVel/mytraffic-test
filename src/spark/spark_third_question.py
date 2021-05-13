import pandas as pd
from datetime import datetime
from pyspark.ml.feature import Bucketizer

def compute_hour_of_day(df):
    """
    Takes a pyspark DataFrame and computes the hour at which the ride happened using the appropriate columns for each row.

    Args:
        df (pyspark.sql.dataframe.DataFrame): input DataFrame with datetimes
    Returns:
        pyspark.sql.dataframe.DataFrame: column with the corresponding hour as integers
    """
    hours = df.rdd.map(
        lambda row: (datetime.strptime(row[3],'%Y-%m-%d %H:%M:%S').hour, )
        )

    return hours.toDF(["hour"])

def compute_count_per_timeslot(df):
    """
    Takes a pyspark DataFrame and computes the number of taxi rides for each time interval (of 4 hours)

    Args:
        df (pyspark.sql.dataframe.DataFrame): input DataFrame with datetimes
    Returns:
        pyspark.sql.dataframe.DataFrame: column with the corresponding intervals and counts
    """
    # compute hour of day for each row
    hours = compute_hour_of_day(df)
    # very small value before hour 0
    before_midnight = -1e-09
    # cut using hour intervals
    bucketizer = Bucketizer(
        splits=[ before_midnight, 4, 8, 12, 16, 20, 24 ],
        inputCol="hour",
        outputCol="buckets")

    timeslots = bucketizer.setHandleInvalid("keep").transform(hours)

    trsf = {0.0:"(0h - 4h]", 1.0:"(4h - 8h]",
            2.0:"(8h - 12h]",3.0:"(12h - 16h]",
            4.0:"(16h - 20h]",5.0:"(0h - 4h]"
            }
    timeslots = timeslots.rdd.map(lambda row: (trsf[row[1]], )).toDF(["hour bucket"])

    count_per_timeslot = timeslots.groupBy("hour bucket").count()

    return count_per_timeslot
