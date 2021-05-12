import pandas as pd
from datetime import datetime

def compute_weekday(df):
    days = df['pickup_datetime'].apply(
        lambda datetime_as_str: datetime.strptime(datetime_as_str,'%Y-%m-%d %H:%M:%S').weekday()
        )

    return days

def compute_count_per_weekday(df):
    # find weekday for each row
    days = compute_weekday(df)
    df['day'] = days
    # use groupby statement to get counts
    dfgb = df[['id', 'day']].groupby('day').count().rename(columns={'id':'count'})

    return dfgb
