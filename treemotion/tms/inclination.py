import numpy as np
import pandas as pd


def get_absolute_inclination(x: pd.Series, y: pd.Series) -> pd.Series:
    """
    Computes the absolute data from x and y values.

    Parameters:
    x (pd.Series): A pandas Series containing numerical values.
    y (pd.Series): A pandas Series containing numerical values.

    Returns:
    pd.Series: A pandas Series containing the computed absolute values.
    """
    # Calculate the square of x and y values
    x_squared = np.square(x)
    y_squared = np.square(y)

    # Calculate the square root of the sum of x^2 and y^2
    absolute_values = np.sqrt(x_squared + y_squared)

    return absolute_values


def get_inclination_direction(x: pd.Series, y: pd.Series) -> pd.Series:
    """
    Calculates the inclination direction in degrees from the given x and y values.

    Parameters:
        x (pd.Series): A Pandas Series containing x values.
        y (pd.Series): A Pandas Series containing y values.

    Returns:
        pd.Series: A Pandas Series containing inclination direction in degrees.
    """
    inclination_direction = np.degrees(np.arctan2(y, x))
    return (inclination_direction + 360) % 360
