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
        tms_df_columns = ['Time', 'East-West-Inclination', 'North-South-Inclination',
                           'Absolute-Inclination', 'Inclination direction of the tree',
                           'Temperature', 'East-West-Inclination - drift compensated',
                           'North-South-Inclination - drift compensated',
                           'Absolute-Inclination - drift compensated',
                           'Inclination direction of the tree - drift compensated']

        tms_df_time_column_name = 'Time'
        tms_df_main_column_name = 'Absolute-Inclination - drift compensated'
        default_new_version_name = "copy"

    class MeasurementVersion:
        default_load_from_csv_measurement_version_name = "raw"

