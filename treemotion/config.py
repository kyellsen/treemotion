from pathlib import Path
from typing import Optional
import pandas as pd

from kj_core.core_config import CoreConfig

from kj_logger import get_logger

logger = get_logger(__name__)


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

    def __init__(self, working_directory: Optional[str] = None):
        """
        Initializes the configuration settings, building upon the core configuration.
        """
        super().__init__(f"{working_directory}/{self.package_name_short}")
        logger.info(f"{self} initialized! Code: 002")

    class Measurement:
        pass

    class MeasurementVersion:
        measurement_version_name_default = "raw"

    class Data:
        data_wind_directory = "data_wind_station"
        wind_download_folder = f"{data_wind_directory}_download"
        data_tms_directory = 'data_tms'
        data_merge_directory = 'data_merge'
        data_ls3_directory = 'data_ls3'

        data_wind_columns = ['station_id',
                             # 'quality_level_wind_avg', # drop
                             'wind_speed_10min_avg',
                             'wind_direction_10min_avg',
                             # 'quality_level_wind_extremes',  # drop
                             'wind_speed_max_10min',
                             # 'wind_speed_min_10min', # drop
                             'wind_speed_max_10min_moving_avg',
                             'wind_direction_max_wind_speed']  # 'datetime' is index!

        data_wind_columns_drop = ['quality_level_wind_avg', 'quality_level_wind_extremes',
                                  'wind_speed_min_10min']  # 'station_id',

        data_wind_columns_int = []  # , 'station_id', 'quality_level_wind_avg', 'quality_level_wind_extremes']

        wind_resample_freq = "60s"

        data_tms_columns = ['East-West-Inclination',
                            'North-South-Inclination',
                            'Absolute-Inclination',
                            'Inclination direction of the tree',
                            'Temperature',
                            'East-West-Inclination - drift compensated',
                            'North-South-Inclination - drift compensated',
                            'Absolute-Inclination - drift compensated',
                            'Inclination direction of the tree - drift compensated']  # 'Time' is the index!

        data_merge_columns = data_tms_columns + data_tms_columns

        main_wind_value = 'wind_speed_max_10min_moving_avg'
        main_tms_value = 'Absolute-Inclination - drift compensated'

        time_rolling_max = '30min'  # pandas time format!
        merge_wind_value = 'wind_rolling_max_' + time_rolling_max
        merge_tms_value = 'tms_rolling_max_' + time_rolling_max

        max_shift_sec: float = 90 * 60  # Default 7200 sec or 2 hours

        tms_sample_rate_hz: int = 20
        tms_sample_rate_interval = pd.to_timedelta(1 / tms_sample_rate_hz, unit='s')

        # peak_n
        peak_n_count: int = 50
        peak_n_min_time_diff: float = 30
        peak_n_prominence: int = None

    class Series:
        default_data_class_name = "data_merge"
        cut_time_by_peaks_duration = 15 * 60  # Seconds

    class CrownMotionSimilarity:
        # shifting
        max_shift_sec: int = 10  # Seconds
        calc_shift_by_column = 'Absolute-Inclination - drift compensated'

        # analysing
        calc_similarity_by_col = 'Absolute-Inclination - drift compensated'
        window_time_around_peak = "10s"  # Pandas TimeDelta-Format
        quantil_included = 0.95

        # plotting
        columns_to_plot = ['East-West-Inclination - drift compensated',
                           'North-South-Inclination - drift compensated',
                           'Absolute-Inclination - drift compensated']
