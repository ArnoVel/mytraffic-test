import os
import pandas as pd

from src import pandas_third_question as ptq

# get correct data path
dir_path = os.path.dirname(os.path.realpath(__file__))
data_path = os.path.join(dir_path, '../data/data.csv')

if __name__ == '__main__':
    # load into pandas dataframe
    df = pd.read_csv(data_path)

    # compute count per timeslot
    count_per_timeslot = ptq.compute_count_per_timeslot(df)

    print(count_per_timeslot)
