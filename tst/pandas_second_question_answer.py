import os
import pandas as pd

from src import pandas_second_question as psq

# get correct data path
dir_path = os.path.dirname(os.path.realpath(__file__))
data_path = os.path.join(dir_path, '../data/data.csv')

if __name__ == '__main__':
    # load into pandas dataframe
    df = pd.read_csv(data_path)

    # compute count per weekday
    count_per_weekday = psq.compute_count_per_weekday(df)

    print(count_per_weekday)
