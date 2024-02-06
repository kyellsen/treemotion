# treemotion/configuration.py

from pathlib import Path
import great_expectations as ge


class Configuration:
    """
    Eine Klasse zur Verwaltung der Konfigurationseinstellungen für das Treemotion-Paket.
    """

    def __init__(self):
        """
        Initialisiert die Konfigurationswerte mit den Standardwerten.
        """

        # Utils #

        # Default Directory´s
        self.working_directory = None
        self.set_working_directory(r"C:\kyellsen\006_Tools\treemotion\working_directory")

        # noinspection PyUnresolvedReferences
        self.log_directory = self.working_directory / "logs"
        # noinspection PyUnresolvedReferences
        self.plot_directory = self.working_directory / "plots"  # working_directory / plot_directory

        # noinspection PyUnresolvedReferences
        self.dwd_data_directory = self.working_directory / "wind_data_dwd"  # working_directory / wind_data_dwd_directory

        # noinspection PyUnresolvedReferences
        self.validation_manager_directory = self.working_directory / "data_validation"  # working_directory / data_validation_directory

        # Logging
        self.log_level = "debug"  # debug, info, warning, critical, error

        # Database
        self.template_db_name = "TREEMOTION_TEMPLATE.db"  # Look in the "treemotion" directory for template.db

        # Projekt

        # Classes #

        # Messreihe

        # Messung

        # Data
        self.tms_df_columns = ['Time', 'East-West-Inclination', 'North-South-Inclination',
                           'Absolute-Inclination', 'Inclination direction of the tree',
                           'Temperature', 'East-West-Inclination - drift compensated',
                           'North-South-Inclination - drift compensated',
                           'Absolute-Inclination - drift compensated',
                           'Inclination direction of the tree - drift compensated']
        self.tms_df_time_column_name = 'Time'
        self.tms_df_main_column_name = 'Absolute-Inclination - drift compensated'
        self.default_load_from_csv_version_name = "raw"
        self.default_new_version_name = "copy"

        # WindMessreihe

        self.wind_df_columns = ['id', 'id_wind_messreihe', 'datetime', 'quality_level_wind_avg',
                                'wind_speed_10min_avg', 'wind_direction_10min_avg',
                                'quality_level_wind_extremes', 'wind_speed_max_10min',
                                'wind_speed_min_10min', 'wind_speed_max_10min_moving_avg',
                                'wind_direction_max_wind_speed']
        self.wind_df_columns_selected = ['datetime', 'wind_speed_10min_avg', 'wind_direction_10min_avg',
                                         'wind_speed_max_10min', 'wind_direction_max_wind_speed']
        self.wind_df_time_column_name = 'datetime'
        self.wind_df_main_column_name = 'wind_speed_max_10min'

        # PlotManager

        self.tms_sample_rate_hz = 20

        # TMS #

        # ValidationManager
        self.validation_manager_max_inclination = 5
        incl = self.validation_manager_max_inclination  # Shortform

        # df_expectations = ge.dataset.PandasDataset({
        #     'Time': {'expect_column_values_to_be_of_type': 'datetime64'},
        #     'East-West-Inclination': {'expect_column_values_to_be_between': {'min_value': -incl, 'max_value': incl}},
        #     'North-South-Inclination': {'expect_column_values_to_be_between': {'min_value': -incl, 'max_value': incl}},
        #     'Absolute-Inclination': {'expect_column_values_to_be_between': {'min_value': 0, 'max_value': incl}},
        #     'Inclination direction of the tree': {
        #         'expect_column_values_to_be_between': {'min_value': 0, 'max_value': 360}},
        #     'Temperature': {'expect_column_values_to_be_between': {'min_value': 0, 'max_value': 30}},
        #     'East-West-Inclination - drift compensated': {
        #         'expect_column_values_to_be_between': {'min_value': -incl, 'max_value': incl}},
        #     'North-South-Inclination - drift compensated': {
        #         'expect_column_values_to_be_between': {'min_value': -incl, 'max_value': incl}},
        #     'Absolute-Inclination - drift compensated': {
        #         'expect_column_values_to_be_between': {'min_value': 0, 'max_value': incl}},
        #     'Inclination direction of the tree - drift compensated': {
        #         'expect_column_values_to_be_between': {'min_value': 0, 'max_value': 360}},
        # })

    def set_working_directory(self, directory: str) -> bool:
        """
        Sets the working directory to the specified directory.

        Args:
            directory (str): The directory path.

        Returns:
            bool: True if the working directory was successfully set, False otherwise.
        """

        from .utils.log import get_logger
        logger = get_logger(__name__)

        try:
            directory = Path(directory)
            if not directory.exists():
                directory.mkdir(parents=True)
                logger.info(f"Das Verzeichnis {directory.__str__()} wurde erfolgreich erstellt.")
            else:
                logger.warning(f"Das Verzeichnis {directory.__str__()} existiert bereits.")

            self.working_directory = directory
            logger.info("Arbeitsverzeichnis festgelegt!")
            return True

        except Exception as e:
            logger.error(f"Fehler beim Festlegen des Arbeitsverzeichnisses: {str(e)}")
            return False
