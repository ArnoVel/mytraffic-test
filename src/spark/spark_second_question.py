from datetime import datetime
from pyspark.sql.functions import asc

def compute_weekday(df):
    """
    Takes a pyspark DataFrame and computes the weekday using the appropriate columns for each row.

    Args:
        df (pyspark.sql.dataframe.DataFrame): input DataFrame with datetimes
    Returns:
        pyspark.sql.dataframe.DataFrame: column with the corresponding weekday as integers
    """
    days = df.rdd.map(
        lambda row: (datetime.strptime(row[3],'%Y-%m-%d %H:%M:%S').weekday(), )
        )

    return days.toDF(["day"])

def compute_count_per_weekday(df):
    """
    Takes a pyspark DataFrame and computes the number of taxi rides for each weekday.

    Args:
        df (pyspark.sql.dataframe.DataFrame): input DataFrame with datetimes
    Returns:
        pyspark.sql.dataframe.DataFrame: dataframe with the corresponding weekday and count as integers
    """
    # find weekday for each row
    days = compute_weekday(df)
    # use groupby statement to get counts
    days = compute_weekday(df)
    count_per_weekday = days.groupBy("day").count().sort(asc("day"))

    return count_per_weekday
