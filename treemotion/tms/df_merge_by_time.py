import pandas as pd
from typing import Tuple

from kj_core import get_logger
from kj_core.utils.runtime_manager import dec_runtime

logger = get_logger(__name__)


def merge_dfs_by_time(df_high_freq: pd.DataFrame, df_low_freq: pd.DataFrame) -> pd.DataFrame:
    """
    Merges two DataFrames based on their time index. The lower frequency DataFrame is reindexed to match
    the higher frequency DataFrame's index.

    :param df_high_freq: The higher frequency DataFrame.
    :param df_low_freq: The lower frequency DataFrame.
    :return: A merged DataFrame.
    """
    # Reindex the lower frequency DataFrame to match the higher frequency DataFrame's index
    df_low_freq = df_low_freq.reindex(df_high_freq.index, method='nearest')
    logger.info("Reindexed df_low_freq to match df_high_freq.")

    # Perform an outer join between the two DataFrames
    merged_data = df_high_freq.merge(df_low_freq, left_index=True, right_index=True, how='outer')
    logger.info("Merged dataframes using an outer join.")
    return merged_data


def cut_df_to_match_length(df_to_cut: pd.DataFrame, df_reference: pd.DataFrame) -> pd.DataFrame:
    """
    Cuts a DataFrame to match the time range of a reference DataFrame, including a buffer period.

    :param df_to_cut: DataFrame to be cut.
    :param df_reference: Reference DataFrame.
    :return: A trimmed DataFrame.
    :raises ValueError: If either DataFrame does not have a DatetimeIndex.
    """
    if not isinstance(df_to_cut.index, pd.DatetimeIndex) or not isinstance(df_reference.index, pd.DatetimeIndex):
        raise ValueError("Both df_to_cut and df_reference must have a DatetimeIndex.")

    # Determine the relevant time range of df_reference plus buffer
    buffer_period = df_reference.index.max() - df_reference.index.min()
    start_time = df_reference.index.min() - buffer_period
    end_time = df_reference.index.max() + buffer_period

    # Limit df_to_cut to the relevant time range with buffer
    df_cut = df_to_cut.loc[start_time:end_time]
    logger.info(f"Trimmed df_to_cut from '{start_time}' to '{end_time}'.")
    return df_cut


def calc_correlation_at_shift(df: pd.DataFrame, column1: str, column2: str, shift: int) -> float:
    """
    Calculates the correlation between two columns of a DataFrame at a given shift.

    :param df: DataFrame containing the columns to correlate.
    :param column1: Name of the first column.
    :param column2: Name of the second column.
    :param shift: The shift applied to the second column.
    :return: The correlation coefficient.
    """
    if column1 not in df.columns or column2 not in df.columns:
        raise ValueError(f"Columns '{column1}' and/or '{column2}' not found in DataFrame.")

    shifted_series = df[column2].shift(shift)
    return df[column1].corr(shifted_series)


@dec_runtime
def calc_optimal_shift(df: pd.DataFrame, column1: str, column2: str, max_shift: int, step: int = 1) -> Tuple[float, int]:
    """
    Calculates the optimal shift between two time series within a specified range,
    to maximize the correlation between two columns in a DataFrame.

    :param df: DataFrame containing the time series data.
    :param column1: Name of the first column in the DataFrame.
    :param column2: Name of the second column in the DataFrame.
    :param max_shift: The maximum absolute shift value to consider.
    :param step: The step size to use when iterating over shift values.
    :return: A tuple containing the maximum correlation and the corresponding optimal shift.
    """
    if df.empty:
        raise ValueError("The DataFrame is empty.")
    if not isinstance(max_shift, int) or not isinstance(step, int):
        raise TypeError("max_shift and step must be integers.")
    if max_shift < 0 or step < 1:
        raise ValueError("max_shift must be non-negative and step must be positive.")

    max_correlation = 0
    optimal_shift = 0

    # Iterate over the range of shift values to find the optimal shift
    for shift in range(-max_shift, max_shift + 1, step):
        correlation = calc_correlation_at_shift(df, column1, column2, shift)
        if correlation is not None and abs(correlation) > abs(max_correlation):
            max_correlation = correlation
            optimal_shift = shift

    logger.info(f"Calculated optimal shift: {optimal_shift} with max correlation: {max_correlation}")
    return max_correlation, optimal_shift
