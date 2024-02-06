import numpy as np
import pandas as pd
from scipy.signal import correlate
from typing import Tuple, Union

from kj_logger import get_logger
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
    logger.debug("Reindexed df_low_freq to match df_high_freq.")

    # Perform an outer join between the two DataFrames
    merged_data = df_high_freq.merge(df_low_freq, left_index=True, right_index=True, how='outer')
    logger.debug("Merged dataframes using an outer join.")
    return merged_data


def cut_to_match_length(data_to_cut: Union[pd.DataFrame, pd.Series],
                        data_reference: Union[pd.DataFrame, pd.Series]) -> Union[pd.DataFrame, pd.Series]:
    """
    Trims a DataFrame or Series to match the time range of a reference DataFrame or Series.

    Adjusts the `data_to_cut` to fit within the time range of `data_reference`, applying a buffer period
    equivalent to the time span of `data_reference`.

    Parameters:
        data_to_cut (Union[pd.DataFrame, pd.Series]): The data to be trimmed.
        data_reference (Union[pd.DataFrame, pd.Series]): The reference data to align the time range with.

    Returns:
        Union[pd.DataFrame, pd.Series]: The trimmed DataFrame or Series.

    Raises:
        ValueError: If either `data_to_cut` or `data_reference` does not have a DatetimeIndex,
                    or if `data_to_cut` type is not supported.
    """
    # Validate that both inputs have a DatetimeIndex
    if not isinstance(data_to_cut.index, pd.DatetimeIndex) or not isinstance(data_reference.index, pd.DatetimeIndex):
        logger.error("Both inputs must have a DatetimeIndex.")
        raise ValueError("Both inputs must have a DatetimeIndex.")

    # Calculate the buffer period based on the reference data's time range
    buffer_period = data_reference.index.max() - data_reference.index.min()
    start_time = data_reference.index.min() - buffer_period
    end_time = data_reference.index.max() + buffer_period

    # Attempt to trim `data_to_cut` to match the calculated time range
    try:
        data_cut = data_to_cut.loc[start_time:end_time]
        logger.debug(f"Trimmed data from '{start_time}' to '{end_time}'.")
    except KeyError as e:
        logger.error(f"Error trimming data: {e}")
        raise ValueError(f"Error trimming data: {e}")

    return data_cut

def align_series(series1: pd.Series, series2: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """
    Aligns two time series based on their DateTimeIndex, ensuring they have the same start and end dates.

    Parameters:
    - series1 (pd.Series): First time series.
    - series2 (pd.Series): Second time series.

    Returns:
    - Tuple[pd.Series, pd.Series]: Tuple containing the aligned series1 and series2.
    """
    # Find the latest start date and the earliest end date between the two series
    start_date = max(series1.index.min(), series2.index.min())
    end_date = min(series1.index.max(), series2.index.max())

    # Align both series to the same start and end dates
    series1_aligned = series1[start_date:end_date]
    series2_aligned = series2[start_date:end_date]

    return series1_aligned, series2_aligned

def calc_optimal_shift(series1: pd.Series, series2: pd.Series, max_shift: int = None) -> Tuple[int, float, float]:
    """
    Calculates the optimal get_shifted_trunk_data and correlations between two time series data to determine
    how aligned they are. This version first aligns the series based on their DateTimeIndex.

    Parameters:
    - series1 (pd.Series): Reference time series.
    - series2 (pd.Series): Time series to compare with the reference.
    - max_shift (int, optional): Maximum number of indices to get_shifted_trunk_data series2 for finding the best alignment.
                                 Defaults to half the length of series1 if None.

    Returns:
    - Tuple[int, float, float]: A tuple containing the optimal get_shifted_trunk_data (int), correlation without get_shifted_trunk_data (float),
                                and correlation with optimal get_shifted_trunk_data (float).
    """
    # Align the series based on their DateTimeIndex, ensure that differences in the DateTimeIndex are reflected
    series1, series2 = align_series(series1, series2)

    # Issue a warning if NaNs or infinities are present in the time series
    if series1.isna().any() or series2.isna().any():
        raise ValueError("One or both time series contain NaNs. This could impact the correlation calculation.")

    if np.isinf(series1).any() or np.isinf(series2).any():
        raise ValueError("One or both time series contain infinity values. This could impact the correlation calculation.")

    # Determine the maximum get_shifted_trunk_data if not specified
    max_shift = max_shift or len(series1) // 2

    # Calculate cross-correlation using the FFT method for efficiency
    corr = correlate(series1 - series1.mean(), series2 - series2.mean(), mode='full', method='auto') # direct or fft

    # Determine the midpoint of the correlation array
    mid_point = len(corr) // 2
    lag = np.arange(-mid_point, mid_point + 1)

    # Calculate correlation at zero get_shifted_trunk_data
    correlation_no_shift = corr[mid_point] / (np.std(series1) * np.std(series2) * len(series1))

    # Identify the optimal get_shifted_trunk_data within the allowable range
    valid_range_start, valid_range_end = mid_point - max_shift, mid_point + max_shift + 1
    valid_range = corr[valid_range_start:valid_range_end]
    optimal_shift_index = np.argmax(np.abs(valid_range))
    optimal_shift = lag[valid_range_start:valid_range_end][optimal_shift_index]
    correlation_optimal_shift = valid_range[optimal_shift_index] / (np.std(series1) * np.std(series2) * len(series1))

    # logger.debug(f"Optimal get_shifted_trunk_data: {optimal_shift}, "
    #              f"Correlation without get_shifted_trunk_data: {correlation_no_shift:.4f}, "
    #              f"Correlation at optimal get_shifted_trunk_data: {correlation_optimal_shift:.4f}")

    return int(optimal_shift), float(correlation_no_shift), float(correlation_optimal_shift)
