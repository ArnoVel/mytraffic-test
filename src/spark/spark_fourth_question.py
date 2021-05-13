from pyspark.sql.functions import asc
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window

from src.spark.spark_first_question import compute_distance
from src.spark.spark_second_question import compute_weekday

def compute_distance_per_weekday(df):
    """
    Takes a pyspark DataFrame and computes the number of km traveled for each weekday.

    Args:
        df (pyspark.sql.dataframe.DataFrame): input DataFrame with datetimes
    Returns:
        pyspark.sql.dataframe.DataFrame: dataframe with the corresponding weekday and distances
    """
    # compute distance and weekday
    days = compute_weekday(df)
    distances = compute_distance(df)
    # add row index to join on
    days = days.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
    distances = distances.withColumn('row_index', row_number().over(Window.orderBy(monotonically_increasing_id())))
    # perform join using index
    df = distances.join(days, on=["row_index"]).drop("row_index")

    # group by day and sum distance traveled
    distance_per_weekday = df.groupBy("day").sum("distance").sort(asc("day"))

    return distance_per_weekday
