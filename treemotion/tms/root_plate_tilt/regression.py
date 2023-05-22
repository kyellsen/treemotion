import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from typing import Optional

from utils.dataframe_utils import validate_df

REGRESSION_METHODS = ['linear', 'polynomial']


def calculate_regression(df: pd.DataFrame, column1: str, column2: str,
                         regression_method: str = 'linear',
                         degree: Optional[int] = None,
                         max_window: Optional[int] = None):
    """
    Calculate the regression between two columns in a dataframe.

    Parameters:
    df: DataFrame to calculate regression on.
    column1, column2: Names of the columns to calculate regression between.
    regression_method: Regression method. Default is 'linear'. Options are 'linear', 'polynomial'.
    degree: Degree of the polynomial if regression method is 'polynomial'. Ignored if method is 'linear'.
    max_window: Window in seconds to calculate max. Default is None, meaning all values will be used.

    Returns:
    Regression result.
    """
    _validate_regression_inputs(df, column1, column2, regression_method, degree)

    df_copy = df.copy()

    if max_window is not None:
        df_copy.set_index('datetime', inplace=True)
        df_copy = df_copy.resample(f'{max_window}S').max()
        df_copy.reset_index(inplace=True)

    x = df_copy[column1].values.reshape(-1, 1)
    y = df_copy[column2].values

    if regression_method == 'linear':
        model = LinearRegression()
        model.fit(x, y)
        result = model.coef_[0], model.intercept_

    else:  # regression_method == 'polynomial'
        coefficients = np.polyfit(x.flatten(), y, degree)
        result = coefficients

    return result


def _validate_regression_inputs(df: pd.DataFrame, column1: str, column2: str, regression_method: str,
                                degree: int) -> None:
    """
    Validate the input dataframe, column names, regression method and degree.

    Parameters:
    df: The dataframe to be validated.
    column1, column2: The columns to be validated.
    regression_method: The regression method to be validated.
    degree: The degree to be validated if regression method is polynomial.

    Raises:
    ValueError: If the dataframe, columns, regression method or degree is invalid.
    """
    validate_df(df, columns=[column1, column2])

    if regression_method not in REGRESSION_METHODS:
        raise ValueError(f"Invalid regression method: {regression_method}")

    if regression_method == 'polynomial' and degree < 1:
        raise ValueError("Degree must be at least 1 for polynomial regression.")
