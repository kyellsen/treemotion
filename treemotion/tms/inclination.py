import numpy as np
import pandas as pd



def calc_abs_inclino(x: pd.Series, y: pd.Series) -> pd.Series:
    """
    Calculates the total inclination from the x and y components.

    Parameters:
    x (pd.Series): A pandas Series containing x-component values.
    y (pd.Series): A pandas Series containing y-component values.

    Returns:
    pd.Series: A pandas Series containing the total inclination values, computed by applying the Pythagorean theorem to the x and y components.
    """
    return np.sqrt(x ** 2 + y ** 2)


def calc_inclination_direction(x: pd.Series, y: pd.Series) -> pd.Series:
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
