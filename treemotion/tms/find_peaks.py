import pandas as pd
import numpy as np
from scipy.signal import find_peaks


def find_max_peak(series: pd.Series) -> tuple:
    """
    Finds the highest peak in a given Pandas Series with a DateTimeIndex.

    Parameters:
    series (pd.Series): The Series in which to search for the peak.

    Returns:
    tuple: A tuple with the time (index) of the peak and the peak value.
    """

    # Select the maximum peak
    return series.idxmax(), series.max()


def find_n_peaks(series: pd.Series, count: int, sample_rate: float,
                 min_time_diff: float = None, prominence: int = None) -> pd.Series:
    """
    Finds the n largest peaks in a given Pandas Series with a DateTimeIndex and returns a single pd.Series
    where the values are the peak values and the index is the datetime of each peak, sorted by datetime.

    Parameters:
    series (pd.Series): The Series in which to search for peaks.
    count (int): The number of largest peaks to return.
    sample_rate (float): The sampling rate of the data (in Hertz).
    min_time_diff (float, optional): The minimum time difference (in seconds) between two peaks.
    prominence (int, optional): The prominence value to be used for peak detection.

    Returns:
    pd.Series: A Series where the index is the datetime of each peak and the values are the peak values,
               sorted by datetime.

    Raises:
    ValueError: If count is less than or equal to 0,
                if sample_rate is less than or equal to 0,
                if min_time_diff is present and less than or equal to 0.
    """

    if count <= 0:
        raise ValueError('count must be greater than 0')
    if sample_rate <= 0:
        raise ValueError('sample_rate must be greater than 0')
    if min_time_diff is not None and min_time_diff <= 0:
        raise ValueError('min_time_diff must be greater than 0')

    min_samples_diff = np.ceil(min_time_diff * sample_rate) if min_time_diff is not None else None
    peaks, _ = find_peaks(series, distance=min_samples_diff, prominence=prominence)

    # Select the 'count' largest peaks by their values
    peak_values = series.iloc[peaks]
    largest_peaks = peak_values.nlargest(count)
    sorted_largest_peaks = largest_peaks.sort_index()

    return sorted_largest_peaks

