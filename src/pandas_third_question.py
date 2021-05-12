import pandas as pd
from datetime import datetime

def compute_hour_of_day(df):
    """
    Takes a pandas DataFrame and computes the hour at which the ride happened using the appropriate columns for each row.

    Args:
        df (pd.DataFrame): input DataFrame with datetimes
    Returns:
        pd.Series: column with the corresponding hour as integers
    """
    hours = df['pickup_datetime'].apply(
        lambda datetime_as_str: datetime.strptime(datetime_as_str,'%Y-%m-%d %H:%M:%S').hour
        )

    return hours

def compute_count_per_timeslot(df):
    """
    Takes a pandas DataFrame and computes the number of taxi rides for each time interval (of 4 hours)

    Args:
        df (pd.DataFrame): input DataFrame with datetimes
    Returns:
        pd.Series: column with the corresponding intervals and counts
    """
    # compute hour of day for each row
    hours = compute_hour_of_day(df)
    # very small value before hour 0
    before_midnight = -1e-09
    # cut using hour intervals
    timeslots = pd.cut(hours, [before_midnight, 4, 8, 12, 16, 20, 24])
    df['timeslot'] = timeslots
    # use groupby statement to get counts per interval
    count_per_timeslot = df[['timeslot', 'id']].groupby('timeslot').count().rename(columns={'id':'count'})

    return count_per_timeslot
