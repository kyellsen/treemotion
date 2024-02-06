from pathlib import Path
import numpy as np
import pandas as pd

from typing import Union

from kj_logger import get_logger

logger = get_logger(__name__)

RENAME_DICT = {
    'STATIONS_ID': 'station_id',
    'MESS_DATUM': 'datetime',
    'QN_file1': 'quality_level_wind_avg',
    'FF_10': 'wind_speed_10min_avg',
    'DD_10': 'wind_direction_10min_avg',
    'QN_file2': 'quality_level_wind_extremes',
    'FX_10': 'wind_speed_max_10min',
    'FNX_10': 'wind_speed_min_10min',
    'FMX_10': 'wind_speed_max_10min_moving_avg',
    'DX_10': 'wind_direction_max_wind_speed'
}

def extract_wind_df(filepath_wind: Path, filepath_wind_extreme: Path):
    """
    Loads and prepares the dataframes from the provided txt files.

    :param filepath_wind: Name of the first txt file.
    :param filepath_wind_extreme: Name of the second txt file.
    :return: Merged DataFrame with prepared data.
    """
    # Read each file into a DataFrame
    df1 = pd.read_csv(filepath_wind, sep=';', index_col=False, skipinitialspace=True)
    df2 = pd.read_csv(filepath_wind_extreme, sep=';', index_col=False, skipinitialspace=True)

    # Ensure 'MESS_DATUM' is in the correct date format
    df1['MESS_DATUM'] = pd.to_datetime(df1['MESS_DATUM'], format='%Y%m%d%H%M')
    df2['MESS_DATUM'] = pd.to_datetime(df2['MESS_DATUM'], format='%Y%m%d%H%M')

    # Merge the two DataFrames based on the 'STATIONS_ID' and 'MESS_DATUM' columns
    merged_df = pd.merge(df1, df2, on=['STATIONS_ID', 'MESS_DATUM'], suffixes=('_file1', '_file2'))

    # Remove the 'eor' columns
    merged_df = merged_df.drop(['eor_file1', 'eor_file2'], axis=1)

    merged_df.rename(columns=RENAME_DICT, inplace=True)

    merged_df.set_index('datetime', inplace=True)

    for col in merged_df.columns:
        if merged_df[col].dtype == 'float64':
            # Ersetze -999 durch NaN in float64 Spalten
            merged_df[col] = merged_df[col].replace(-999, np.nan).astype("float32") # TODO: float64 ?
        elif merged_df[col].dtype == 'int64':
            # Temporär auf float konvertieren für NaN Unterstützung
            temp_col = merged_df[col].astype('float64')
            # Ersetze -999 durch den letzten gültigen Wert
            temp_col.replace(-999, np.nan, inplace=True)
            temp_col.ffill(inplace=True)
            # Zurück zu int64 konvertieren
            merged_df[col] = temp_col.astype('int32') # TODO: int64 ?

    logger.debug(f"Loaded wind and extreme wind data from {filepath_wind} and {filepath_wind_extreme}!")
    return merged_df


def extract_station_metadata(station_id: str,
                             filepath_stations_list: str) -> dict:
    """
    Load and prepare station data from a text file.

    :param station_id: The id of the station.
    :param filepath_stations_list: The name of the file.
    :return: A dictionary containing station metadata.
    """

    # Iterate through the lines of the file
    with open(filepath_stations_list, 'r', encoding='latin1') as f:
        next(f)  # Skip the header line
        next(f)  # Skip the line with the separators
        for line in f:
            line_data = line.split()
            if line_data[0] == station_id:
                # Convert the found line into a dictionary
                station_metadata = {
                    "station_id": int(line_data[0]),
                    "datetime_start": line_data[1],
                    "datetime_end": line_data[2],
                    "station_height": int(line_data[3]),
                    "station_latitude": float(line_data[4]),
                    "station_longitude": float(line_data[5]),
                    "station_name": " ".join(line_data[6:-1]),
                    "bundesland": line_data[-1]
                }

                logger.debug(f"Loaded station metadata from {filepath_stations_list}: {station_metadata.__str__()}")
                return station_metadata

    raise ValueError(f"No data found for station id {station_id}")
