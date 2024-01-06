from pathlib import Path
import pandas as pd

from typing import Union

from kj_core.utils.log_manager import get_logger

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

def load_wind_df(filepath_wind: Path, filepath_wind_extreme: Path):
    """
    Loads and prepares the dataframes from the provided txt files.

    :param filepath_wind: Name of the first txt file.
    :param filepath_wind_extreme: Name of the second txt file.
    :return: Merged DataFrame with prepared data.
    """
    # Read each file into a DataFrame
    df1 = pd.read_csv(filepath_wind, sep=';', index_col=False)
    df2 = pd.read_csv(filepath_wind_extreme, sep=';', index_col=False)

    # Remove leading and trailing spaces from column names
    df1.columns = df1.columns.str.strip()
    df2.columns = df2.columns.str.strip()

    # Ensure 'MESS_DATUM' is in the correct date format
    df1['MESS_DATUM'] = pd.to_datetime(df1['MESS_DATUM'], format='%Y%m%d%H%M')
    df2['MESS_DATUM'] = pd.to_datetime(df2['MESS_DATUM'], format='%Y%m%d%H%M')

    # Merge the two DataFrames based on the 'STATIONS_ID' and 'MESS_DATUM' columns
    merged_df = pd.merge(df1, df2, on=['STATIONS_ID', 'MESS_DATUM'], suffixes=('_file1', '_file2'))

    # Remove the 'eor' columns
    merged_df = merged_df.drop(['eor_file1', 'eor_file2'], axis=1)

    merged_df.rename(columns=RENAME_DICT, inplace=True)

    # Replace -999 with None as -999 is marked as missing value
    merged_df = merged_df.replace(-999, None)
    logger.debug(f"Loaded wind and extreme wind data from {filepath_wind} and {filepath_wind_extreme}!")
    return merged_df


def load_station_metadata(station_id: str,
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
