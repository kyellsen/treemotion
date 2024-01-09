from pathlib import Path
from typing import Optional

from kj_core.core_config import CoreConfig


class Config(CoreConfig):
    """
    Configuration class for the package, extending the core configuration.
    Provides package-specific settings and default values.
    """
    # Override default working directory specific
    package_name = "treemotion"
    package_name_short = "tms"
    # Override default working directory specific
    default_working_directory = r"C:\kyellsen\006_Packages\treemotion\working_directory_tms"

    def __init__(self, working_directory: Optional[str] = None, log_level: Optional[str] = None):
        """
        Initializes the configuration settings, building upon the core configuration.

        """
        super().__init__(working_directory, log_level)

    class Measurement:
        pass

        # tms_df_time_column_name = 'Time'
        # tms_df_main_column_name = 'Absolute-Inclination - drift compensated'

    class MeasurementVersion:
        default_load_from_csv_measurement_version_name = "raw"

    class DataWindStation:
        data_directory = "data_wind_station"
        download_folder = f"{data_directory}_download"
        data_columns = ['station_id',
                        'datetime',
                        'quality_level_wind_avg',
                        'wind_speed_10min_avg',
                        'wind_direction_10min_avg',
                        'quality_level_wind_extremes',
                        'wind_speed_max_10min',
                        'wind_speed_min_10min',
                        'wind_speed_max_10min_moving_avg',
                        'wind_direction_max_wind_speed']
        # main_value = ""

    class DataTMS:
        data_directory = 'data_tms'
        data_columns = ['Time', 'East-West-Inclination', 'North-South-Inclination',
                        'Absolute-Inclination', 'Inclination direction of the tree',
                        'Temperature', 'East-West-Inclination - drift compensated',
                        'North-South-Inclination - drift compensated',
                        'Absolute-Inclination - drift compensated',
                        'Inclination direction of the tree - drift compensated']
        main_value = 'Absolute-Inclination - drift compensated'

        # find_n_peaks
        n_peaks: int = 10
        sample_rate: float = 20
        min_time_diff: float = 60
        prominence: int = None

    class DataMerge:
        data_directory = 'data_merge'
        data_columns = ['Time', 'East-West-Inclination', 'North-South-Inclination',
                        'Absolute-Inclination', 'Inclination direction of the tree',
                        'Temperature', 'East-West-Inclination - drift compensated',
                        'North-South-Inclination - drift compensated',
                        'Absolute-Inclination - drift compensated',
                        'Inclination direction of the tree - drift compensated', 'station_id',
                        'datetime', 'quality_level_wind_avg', 'wind_speed_10min_avg',
                        'wind_direction_10min_avg', 'quality_level_wind_extremes',
                        'wind_speed_max_10min', 'wind_speed_min_10min',
                        'wind_speed_max_10min_moving_avg', 'wind_direction_max_wind_speed']
        main_value_tms = 'Absolute-Inclination - drift compensated'
        main_value_wind = ''

    class DataLS3:
        data_directory = 'data_ls3'
        main_value = ""
