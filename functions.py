import numpy as np
import pandas as pd
from tempdriftcomp import *


def limit_data_by_time(data: pd.DataFrame, time_col: str, start_time, end_time) -> pd.DataFrame:
    """
    Returns a Pandas DataFrame that is limited to the specified time range.
    """
    data = data.loc[(data[time_col] >= start_time) & (data[time_col] <= end_time)]
    data.reset_index(drop=True, inplace=True)

    return data


def random_sample(data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Returns a random sample of n rows from the input DataFrame.
    """
    # from random import sample
    data = data.sample(n=n)

    return data


def get_absolute_inclination(x: pd.Series, y: pd.Series) -> pd.Series:
    """
    Computes the absolute data from x and y values.
    """
    # Calculate the square of x and y values
    x_squared = np.square(x)
    y_squared = np.square(y)

    # Calculate the square root of the sum of x^2 and y^2
    return np.sqrt(x_squared + y_squared)


def get_inclination_direction(x: pd.Series, y: pd.Series) -> pd.Series:
    """
    Calculates the data direction in degrees from the given x and y values.
    """
    radian = np.arctan2(-x, -y)
    data_dir = np.degrees(radian)
    data_dir[data_dir < 0] += 360
    return data_dir
