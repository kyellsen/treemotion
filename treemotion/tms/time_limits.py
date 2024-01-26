import pandas as pd

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
    if not peaks['peak_time']:
        raise ValueError("Die Peaks-Liste ist leer.")

    peak_time_list = peaks['peak_time']

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
