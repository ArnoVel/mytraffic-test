import os
import pandas as pd

from src import pandas_first_question as pfq

# get correct data path
dir_path = os.path.dirname(os.path.realpath(__file__))
data_path = os.path.join(dir_path, '../data/data.csv')

if __name__ == '__main__':
    # load into pandas dataframe
    df = pd.read_csv(data_path)

    # compute avg distance
    average_speed = pfq.compute_average_speed(df)

    print(average_speed)
