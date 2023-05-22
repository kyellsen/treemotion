import pandas as pd
import numpy as np
from scipy.stats import pearsonr, spearmanr, kendalltau
from typing import Optional

CORRELATION_METHODS = {
    'pearson': pearsonr,
    'spearman': spearmanr,
    'kendall': kendalltau,
}





def find_optimal_shift(df1: pd.DataFrame, df2: pd.DataFrame, column1: str, column2: str,
                       max_shift: int, correlation_method: str = 'pearson',
                       max_window: Optional[int] = None) -> int:
    """
    Find the shift that maximizes the correlation between two columns in two dataframes.

    Parameters:
    df1, df2: DataFrames to calculate correlations between.
    column1, column2: Names of the columns to correlate.
    max_shift: Maximum shift to calculate correlations for.
    correlation_method: Correlation method. Default is 'pearson'. Options are 'pearson', 'spearman', 'kendall'.
    max_window: Window in seconds to calculate max. Default is None, meaning all values will be used.

    Returns:
    Shift that maximizes the correlation.
    """
    _validate_inputs(df1, df2, column1)
    _validate_inputs(df1, df2, column2)

    corr_func = CORRELATION_METHODS.get(correlation_method)
    if corr_func is None:
        raise ValueError(f"Invalid correlation method: {correlation_method}")

    best_shift = 0
    best_corr = -np.inf

    for shift in range(-max_shift, max_shift + 1):
        df2_shifted = df2.shift(periods=shift)
        merged_df = df1.merge(df2_shifted, how='inner', on='datetime')

        if max_window is not None:
            merged_df.set_index('datetime', inplace=True)
            merged_df = merged_df.resample(f'{max_window}S').max()
            merged_df.reset_index(inplace=True)

        correlation = corr_func(merged_df[column1], merged_df[column2])[0]
        if correlation > best_corr:
            best_corr = correlation
            best_shift = shift

        logger.info(f"Shift: {shift}, Correlation: {correlation}")

    logger.info(f"Best shift is {best_shift} with {correlation_method} correlation {best_corr}.")
    return best_shift


def synchronize_dataframes(df_tilt: pd.DataFrame, df_wind: pd.DataFrame, max_shift: int,
                           interpolation_method: str = 'linear',
                           correlation_method: str = 'pearson',
                           max_window: Optional[int] = None) -> pd.DataFrame:
    """
    Synchronize two dataframes based on a datetime column, such that the correlation
    between a column in each dataframe is maximized.

    Parameters:
    df_tilt: DataFrame containing tilt values.
    df_wind: DataFrame containing wind measurement.
    max_shift: Maximum shift for synchronization, in minutes.
    interpolation_method: Method to use for interpolation. Default is 'linear'.
    correlation_method: Method to use for correlation. Default is 'pearson'.
    max_window: Window in seconds to calculate max for correlation. Default is None, meaning all values will be used.

    Returns:
    Synchronized DataFrame.
    """
    _validate_inputs(df_tilt, df_wind, 'windgeschwindigkeit')
    _validate_inputs(df_tilt, df_wind, 'neigung')

    try:
        df_wind_interpolated = df_wind.resample('50L').interpolate(method=interpolation_method)

        optimal_shift = find_optimal_shift(df_wind_interpolated, df_tilt, 'windgeschwindigkeit', 'neigung',
                                           max_shift * 60, correlation_method, max_window)

        df_tilt_shifted = df_tilt.shift(periods=optimal_shift)
        logger.info(f"Shifted tilt values by {optimal_shift}.")

        result_df = df_wind_interpolated.merge(df_tilt_shifted, how='inner', on='datetime')

        logger.info("Synchronization successful.")
        return result_df

    except Exception as e:
        logger.error(f"Error during synchronization: {e}")
        raise
