import pandas as pd
from scipy.signal import find_peaks


def find_max_peak(df: pd.DataFrame, value_col: str, time_col: str) -> dict:
    """
    Findet den höchsten Peak in einem gegebenen Pandas DataFrame.

    Parameter:
    df (DataFrame): Der DataFrame, in dem Peak gesucht werden sollen.
    values_col (str): Der Name der Spalte im DataFrame, die die Werte enthält.
    time_col (str): Der Name der Spalte im DataFrame, die die Zeitstempel enthält.

    Returns:
    dict: Ein Wörterbuch mit dem Index, der Zeit und dem Wert des gefundenen Peaks.
    """

    # Auswahl des maximalen Peaks
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
    Findet die n höchsten Peaks in einem gegebenen Pandas DataFrame.

    Parameter:
    df (DataFrame): Der DataFrame, in dem Peaks gesucht werden sollen.
    values_col (str): Der Name der Spalte im DataFrame, die die Werte enthält.
    time_col (str): Der Name der Spalte im DataFrame, die die Zeitstempel enthält.
    n_peaks (int): Die Anzahl der höchsten Peaks, die zurückgegeben werden sollen.
    sample_rate (float): Die Abtastrate der Daten (in Hertz).
    min_time_diff (float, optional): Die minimale Zeitdifferenz (in Sekunden) zwischen zwei Peaks.
    prominence (int, optional): Der prominente Wert, der für die Peak-Erkennung verwendet werden soll.

    Returns:
    dict: Ein Wörterbuch mit den Indizes, Zeiten und Werten der gefundenen Peaks.

    Raises:
    ValueError: Wenn n_peaks kleiner oder gleich 0 ist,
                wenn sample_rate kleiner oder gleich 0 ist,
                wenn min_time_diff vorhanden ist und kleiner oder gleich 0 ist.
    """

    # Überprüfen Sie, ob die Eingaben gültig sind
    if n_peaks <= 0:
        raise ValueError('n_peaks muss größer als 0 sein')

    if sample_rate <= 0:
        raise ValueError('sample_rate muss größer als 0 sein')

    if min_time_diff is not None and min_time_diff <= 0:
        raise ValueError('min_time_diff muss größer als 0 sein')

    # Umrechnung von min_time_diff in Abstand in Indizes
    if min_time_diff is not None:
        min_samples_diff = min_time_diff * sample_rate
    else:
        min_samples_diff = None

    # Finden Sie alle Peaks
    peaks, _ = find_peaks(df[value_col], distance=min_samples_diff, prominence=prominence)

    # Sortieren Sie die Peaks nach ihren Werten und behalten Sie nur die Top-n_peaks
    peaks = sorted(peaks, key=lambda x: df[value_col].iloc[x], reverse=True)[:n_peaks]

    return {
        'peak_index': peaks,
        'peak_time': df[time_col].iloc[peaks].tolist(),
        'peak_value': df[value_col].iloc[peaks].tolist()
    }


def merge_peak_dicts(peak_dicts):
    """
    Führt eine Liste von 'peak' Wörterbüchern zusammen.
    """
    return {
        'peak_index': [index for peaks in peak_dicts for index in peaks['peak_index']],
        'peak_time': [time for peaks in peak_dicts for time in peaks['peak_time']],
        'peak_value': [value for peaks in peak_dicts for value in peaks['peak_value']]
    }
