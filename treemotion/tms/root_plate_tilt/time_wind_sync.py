import pandas as pd
import numpy as np

from utils.dataframe_utils import validate_df

from utils.log import get_logger

logger = get_logger(__name__)


def resample(df, resample_freq):
    """
    Resample a DataFrame based on a time column and find the maximum of a value column.

    :param df: DataFrame to resample.
    :param value_column: Name of the column containing value data.
    :param resample_freq: Frequency string for resampling.
    :return: Resampled DataFrame.
    """
    logger.info(f'Resampling {df}')
    df_resampled = df.resample(resample_freq)

    return df_resampled


def find_best_lag(tms_df, wind_df, tms_column, wind_column, max_lag):
    """
    Calculate cross correlation to find the best lag within a maximum lag.

    :param tms_df: TMS DataFrame.
    :param wind_df: Wind DataFrame.
    :param tms_column: Name of the value column in the TMS DataFrame.
    :param wind_column: Name of the value column in the Wind DataFrame.
    :param max_lag: Maximum lag in seconds.
    :return: Lag for the best cross correlation.
    """
    logger.info(f'Calculating cross correlation...')
    max_lag_samples = max_lag * 20  # Convert from seconds to samples
    cross_correlation = np.correlate(tms_df[tms_column], wind_df[wind_column], 'full')
    mid_point = len(cross_correlation) // 2
    start = max(0, mid_point - max_lag_samples)
    end = min(len(cross_correlation), mid_point + max_lag_samples)
    lag_max_correlation = start + np.argmax(cross_correlation[start:end]) - mid_point

    return lag_max_correlation


def sync_df_high_freq(df1, df2, value_column, resample_freq, output_freq, lag):
    """
    Shift, interpolate and merge two DataFrames, keeping the high frequency of df1.

    :param df1: First DataFrame.
    :param df2: Second DataFrame.
    :param value_column: Name of the column to shift and interpolate.
    :param resample_freq: Frequency string for resampling.
    :param output_freq: Frequency string for the output.
    :param lag: Lag for shifting the data.
    :return: Synchronized DataFrame at high frequency.
    """
    logger.info(f'Shifting, interpolating, and merging dataframes at high frequency...')
    df2 = resample(df2, resample_freq)
    df2_shifted = df2.shift(lag)
    df2_shifted = df2_shifted.resample(output_freq).interpolate()

    df1 = df1.sort_index()
    df2_shifted = df2_shifted.sort_index()

    df_merged = pd.merge_asof(df1, df2_shifted, left_index=True, right_index=True)

    return df_merged


def sync_df_low_freq(df1, df2, value_column, resample_freq, output_freq, lag):
    """
    Downsample, shift and merge two DataFrames, keeping the low frequency of df2.

    :param df1: First DataFrame.
    :param df2: Second DataFrame.
    :param value_column: Name of the column to downsample and shift.
    :param resample_freq: Frequency string for resampling.
    :param output_freq: Frequency string for the output.
    :param lag: Lag for shifting the data.
    :return: Synchronized DataFrame at low frequency.
    """
    logger.info(f'Downsampling, shifting, and merging dataframes at low frequency...')
    df1 = resample(df1, resample_freq)
    df1_shifted = df1.shift(lag)
    df1_shifted = df1_shifted.resample(output_freq).asfreq()
    df_merged = pd.merge_asof(df1_shifted.sort_index(), df2.sort_index(), left_index=True, right_index=True)

    return df_merged


def sync_df_custom_freq(tms_df, wind_df, tms_column, wind_column, resample_freq, output_freq,
                        max_lag):
    """
    Resample, shift and merge two DataFrames at a custom frequency.

    :param tms_df: First DataFrame.
    :param wind_df: Second DataFrame.
    :param tms_column: Name of the value column in the first DataFrame.
    :param wind_column: Name of the value column in the second DataFrame.
    :param resample_freq: Frequency string for resampling.
    :param output_freq: Frequency string for the output.
    :param max_lag: Maximum lag in seconds.
    :return: Synchronized DataFrame at a custom frequency.
    """
    logger.info(f'Resampling, shifting, and merging dataframes at custom frequency...')
    tms_df_resampled = resample(tms_df, resample_freq)
    wind_df_resampled = resample(wind_df, resample_freq)
    lag = find_best_lag(tms_df_resampled, wind_df_resampled, tms_column, wind_column, max_lag)
    df1_resampled_shifted = tms_df_resampled.shift(lag)
    df1_resampled_shifted = df1_resampled_shifted.resample(output_freq).interpolate()
    df2_resampled_shifted = wind_df_resampled.shift(lag)
    df2_resampled_shifted = df2_resampled_shifted.resample(output_freq).interpolate()
    df_merged = pd.merge_asof(df1_resampled_shifted.sort_index(),
                              df2_resampled_shifted.sort_index(), left_index=True, right_index=True)

    return df_merged
