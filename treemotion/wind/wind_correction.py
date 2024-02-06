import numpy as np
import pandas as pd


def wind_station_to_site(df, distance, direction, front_speed_factor):
    # Addiere auf die Windrichtung die Ausrichtung der Achse von Punkt A nach B. Werte belieb zwischen 0 und 359

    df['wind_direction_10min_avg'] = (df['wind_direction_10min_avg'] + direction) % 360
    # Konvertiere den Winkel von Grad in Radiant
    angle_radians = np.radians(df['wind_direction_10min_avg'])

    # Anwenden des Kosinussatzes
    df['distance'] = np.sqrt(2 * distance ** 2 * (1 - np.cos(angle_radians)))

    df['front_speed'] = df['wind_speed_10min_avg'] * front_speed_factor

    df['time_shift_s'] = df['distance'] / df['front_speed']

    # Unterscheidung zwischen positiver und negativer Zeitverschiebung je nach Windrichtung
    df.loc[(df['wind_direction_10min_avg'] >= 270) | (df['wind_direction_10min_avg'] <= 90), 'time_shift_s'] *= 1
    df.loc[(df['wind_direction_10min_avg'] > 90) & (df['wind_direction_10min_avg'] < 270), 'time_shift_s'] *= -1

    # Konvertiere die Zeitverschiebung zu einem Timedelta
    df['time_shift'] = pd.to_timedelta(df['time_shift_s'], unit='s')

    # Addiere die Verschiebung auf die Zeit
    df['datetime'] = df['datetime'] + df['time_shift']

    return df
