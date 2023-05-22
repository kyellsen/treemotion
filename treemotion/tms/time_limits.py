from datetime import datetime
import pandas as pd

def validate_time_format(time_str: str):
    """
    Überprüft und passt gegebenenfalls das Zeitformat an.

    :param time_str: Die zu validierende Zeit als Zeichenkette.
    :return: Die angepasste Zeit als Zeichenkette.
    """
    time_format_1 = "%Y-%m-%d %H:%M:%S.%f"
    time_format_2 = "%Y-%m-%d %H:%M:%S"

    try:
        datetime.strptime(time_str, time_format_1)
    except ValueError:
        try:
            datetime.strptime(time_str, time_format_2)
        except ValueError as e:
            return e
        else:
            time_str += '.000000'

    return time_str


def limit_df_by_time(data: pd.DataFrame, time_col: str, start_time, end_time) -> pd.DataFrame:
    """
    Returns a Pandas DataFrame that is limited to the specified time range.

    Parameters:
    -----------
    data : pandas DataFrame
        The original data frame.

    time_col : str
        Name of the column containing the timestamp.

    start_time : str
        Start time in the format 'yyyy-mm-dd hh:mm:ss'.

    end_time : str
        End time in the format 'yyyy-mm-dd hh:mm:ss'.

    Returns:
    --------
    pandas DataFrame
        A new Pandas DataFrame that contains only rows that have a timestamp
        within the specified range.
    """
    data = data.loc[(data[time_col] >= start_time) & (data[time_col] <= end_time)]
    data.reset_index(drop=True, inplace=True)

    return data


def optimal_time_frame(duration: int, peaks: dict) -> dict:
    """
    Findet den optimalen Zeitrahmen mit der gegebenen Dauer, sodass möglichst viele der Peaks
    innerhalb dieses Zeitrahmens liegen.

    Parameters:
    -----------
    duration : int
        Dauer des optimalen Zeitfensters in Sekunden'.

    peaks : dict
        Ein Wörterbuch mit den Indizes, Zeiten und Werten der gefundenen Peaks.

    Returns:
    --------
    dict
        Ein Wörterbuch mit Startzeit, Endzeit und Dauer des optimalen Zeitfensters.
    """

    # Überprüfen, ob die Peaks-Liste leer ist
    if not peaks['peak_times']:
        raise ValueError("Die Peaks-Liste ist leer.")

    peak_time_list = peaks['peak_times']

    # Initialisieren der Variablen für den optimalen Zeitrahmen
    optimal_start_time = peak_time_list[0]
    optimal_end_time = optimal_start_time + pd.Timedelta(seconds=duration)
    max_peak_count = 0

    # Durchlaufen der Zeitpunkte mit einem gleitenden Fenster der Dauer duration
    for start_time in peak_time_list:
        end_time = start_time + pd.Timedelta(seconds=duration)

        # Zählen der Peaks im aktuellen Zeitfenster
        peak_count = sum(start_time <= time <= end_time for time in peak_time_list)

        # Aktualisieren des optimalen Zeitrahmens, falls im aktuellen Fenster mehr Peaks liegen
        if peak_count > max_peak_count:
            max_peak_count = peak_count
            optimal_start_time = start_time
            optimal_end_time = end_time

    # Umwandeln der optimalen Zeiten in das korrekte Format
    start_time = optimal_start_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    end_time = optimal_end_time.strftime("%Y-%m-%d %H:%M:%S.%f")

    return {
        'start_time': start_time,
        'end_time': end_time,
        'duration': duration
    }
