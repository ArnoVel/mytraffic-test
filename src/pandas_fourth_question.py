import os
import pandas as pd

from src.pandas_first_question import compute_distance
from src.pandas_second_question import compute_weekday

def compute_distance_per_weekday(df):
    """
    Takes a pandas DataFrame and computes the number of km traveled for each weekday.

    Args:
        df (pd.DataFrame): input DataFrame with datetimes
    Returns:
        pd.DataFrame: dataframe with the corresponding weekday and distances
    """
    # compute distance and weekday
    days = compute_weekday(df)
    distances = compute_distance(df)
    df['day'] = days
    df['distance'] = distances
    # use groupby statement to get distance sum
    dfgb = df[['distance', 'day']].groupby('day').sum()

    return dfgb
