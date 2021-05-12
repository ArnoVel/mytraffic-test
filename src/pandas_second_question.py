import pandas as pd
from datetime import datetime

def compute_weekday(df):
    """
    Takes a pandas DataFrame and computes the weekday using the appropriate columns for each row.

    Args:
        df (pd.DataFrame): input DataFrame with datetimes
    Returns:
        pd.Series: column with the corresponding weekday as integers
    """
    days = df['pickup_datetime'].apply(
        lambda datetime_as_str: datetime.strptime(datetime_as_str,'%Y-%m-%d %H:%M:%S').weekday()
        )

    return days

def compute_count_per_weekday(df):
    """
    Takes a pandas DataFrame and computes the number of taxi rides for each weekday.

    Args:
        df (pd.DataFrame): input DataFrame with datetimes
    Returns:
        pd.DataFrame: dataframe with the corresponding weekday and count as integers
    """
    # find weekday for each row
    days = compute_weekday(df)
    df['day'] = days
    # use groupby statement to get counts
    dfgb = df[['id', 'day']].groupby('day').count().rename(columns={'id':'count'})

    return dfgb
