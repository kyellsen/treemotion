import pandas as pd
from scipy.signal import find_peaks


def find_max_peak(df: pd.DataFrame, value_col: str, time_col: str) -> dict:
    """
    Finds the highest peak in a given Pandas DataFrame.

    Parameters:
    df (DataFrame): The DataFrame in which to search for the peak.
    value_col (str): The name of the column in the DataFrame containing the values.
    time_col (str): The name of the column in the DataFrame containing the timestamps.

    Returns:
    dict: A dictionary with the index, time, and value of the found peak.
    """

    # Select the maximum peak
    max_peak_index = df[value_col].idxmax()

    return {
        'peak_index': max_peak_index,
        'peak_time': df[time_col].iloc[max_peak_index],
        'peak_value': df[value_col].iloc[max_peak_index]
    }


def find_n_peaks(df: pd.DataFrame, value_col: str, time_col: str, n_peaks: int,
                 sample_rate: float, min_time_diff: float = None,
                 prominence: int = None) -> dict:
    """
    Finds the n highest peaks in a given Pandas DataFrame.

    Parameters:
    df (DataFrame): The DataFrame in which to search for peaks.
    value_col (str): The name of the column in the DataFrame containing the values.
    time_col (str): The name of the column in the DataFrame containing the timestamps.
    n_peaks (int): The number of highest peaks to return.
    sample_rate (float): The sampling rate of the data (in Hertz).
    min_time_diff (float, optional): The minimum time difference (in seconds) between two peaks.
    prominence (int, optional): The prominence value to be used for peak detection.

    Returns:
    dict: A dictionary with the indices, times, and values of the found peaks.

    Raises:
    ValueError: If n_peaks is less than or equal to 0,
                if sample_rate is less than or equal to 0,
                if min_time_diff is present and less than or equal to 0.
    """

    # Check if the inputs are valid
    if n_peaks <= 0:
        raise ValueError('n_peaks must be greater than 0')

    if sample_rate <= 0:
        raise ValueError('sample_rate must be greater than 0')

    if min_time_diff is not None and min_time_diff <= 0:
        raise ValueError('min_time_diff must be greater than 0')

    # Convert min_time_diff to distance in indices
    if min_time_diff is not None:
        min_samples_diff = min_time_diff * sample_rate
    else:
        min_samples_diff = None

    # Find all peaks
    peaks, _ = find_peaks(df[value_col], distance=min_samples_diff, prominence=prominence)

    # Sort the peaks by their values and keep only the top n_peaks
    peaks = sorted(peaks, key=lambda x: df[value_col].iloc[x], reverse=True)[:n_peaks]

    return {
        'peak_index': peaks,
        'peak_time': df[time_col].iloc[peaks].tolist(),
        'peak_value': df[value_col].iloc[peaks].tolist()
    }


# def merge_peak_dicts(peak_dicts):
#     """
#     Merges a list of 'peak' dictionaries.
#     """
#     return {
#         'peak_index': [index for peaks in peak_dicts for index in peaks['peak_index']],
#         'peak_time': [time for peaks in peak_dicts for time in peaks['peak_time']],
#         'peak_value': [value for peaks in peak_dicts for value in peaks['peak_value']]
#     }
