# treemotion/classes/series.py
from sqlalchemy import distinct, Table, MetaData

from common_imports.classes_heavy import *
from utils.path_utils import validate_and_get_path, validate_and_get_file_list, extract_sensor_id
from tms.time_limits import optimal_time_frame
from tms.find_peaks import merge_peak_dicts

logger = get_logger(__name__)

from .version import Version
from .measurement import Measurement


class Series(BaseClass):
    """
    This class represents a series in the system.
    """
    __tablename__ = 'Series'
    series_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    project_id = Column(Integer, ForeignKey('Project.project_id', onupdate='CASCADE'))
    wind_measurement_id = Column(Integer, ForeignKey('WindMeasurement.wind_measurement_id', onupdate='CASCADE'))
    description = Column(String)
    datetime_start = Column(DateTime)
    datetime_end = Column(DateTime)
    location = Column(String)
    annotation = Column(String)
    filepath_tms = Column(String)

    project = relationship("Project", back_populates="series", lazy="joined", order_by="Project.project_id")
    measurement = relationship('Measurement', back_populates="series", lazy="joined",
                               cascade='all, delete-orphan', order_by='Measurement.measurement_id')
    wind_measurement = relationship('WindMeasurement', lazy="joined")

    def __init__(self, *args, **kwargs):
        super().__init__(*args)

        # in SQLite Database
        self.series_id = kwargs.get('series_id', None)
        self.project_id = kwargs.get('project_id', None)
        self.wind_measurement_id = kwargs.get('wind_measurement_id', None)
        self.description = kwargs.get('description', None)
        self.datetime_start = kwargs.get('datetime_start', None)
        self.datetime_end = kwargs.get('datetime_end', None)
        self.location = kwargs.get('location', None)
        self.annotation = kwargs.get('annotation', None)
        self.filepath_tms = kwargs.get('filepath_tms', None)

        self._version_dict = kwargs.get('_version_dict', {})

    def __str__(self):
        return f"Series(series_id={self.series_id}, series_id={self.series_id})"

    @dec_runtime
    def get_versions_by_filter(self, filter_dict: Dict[str, Any], method: str = "list_filter") \
            -> Optional[List[Version]]:
        """
        Overwrites BaseClass Method. It´s way faster.
        Finds all version objects associated with measurements related to this series based on the provided filters.

        :param filter_dict: A dictionary of attributes and their desired values.
                            For example: {'tms_table_name': tms_table_name}
        :param method: The method to use for filtering. Possible values are "list_filter" and "db_filter".
                       The default value is "list_filter".
        :return: A list of found Version instances, or None if no match is found.
        """

        if not isinstance(filter_dict, dict):
            logger.error("Input filter is not a dictionary. Please provide a valid filter dictionary.")
            return None

        try:
            if method == "list_filter":
                matching_versions = [version for measurement in self.measurement for version in measurement.version if
                                     all(getattr(version, k, None) == v for k, v in filter_dict.items())]
            elif method == "db_filter":
                session = db_manager.get_session()
                matching_versions = (session.query(Version)
                                     .join(Measurement, Version.measurement_id == Measurement.id)
                                     .filter(Measurement.series_id == self.series_id)
                                     .filter_by(**filter_dict)
                                     .all())
            else:
                logger.error("Invalid method. Please choose between 'list_filter' and 'db_filter'.")
                return None

            if not matching_versions:
                logger.debug(f"No Version instance found with the given filters: {filter_dict}.")
                return None

            logger.debug(f"{len(matching_versions)} Version instances found with the given filters: {filter_dict}.")
            return matching_versions

        except Exception as e:
            logger.error(f"An error occurred while querying the Version: {str(e)}")
            return None

    @property
    @dec_runtime
    def version_dict(self) -> Dict:
        """
        This property gets the version dictionary.

        Returns:
            dict: Version dictionary.
        """
        logger.info(f"{self} start loading version_dict!")
        if not hasattr(self, '_version_dict') or self._version_dict is None:
            self._version_dict = {}

        unique_version_names = self.get_unique_version_names_in_series()

        # Check if the unique_version_names have changed since last update
        if set(self._version_dict.keys()) != set(unique_version_names):
            for unique_version_name in unique_version_names:
                logger.debug(f"Create Series.version_dict[{unique_version_name}].")
                try:
                    versions = self.get_versions_by_filter({"version_name": unique_version_name})
                    self._version_dict[unique_version_name] = versions

                except Exception as e:
                    logger.error(
                        f"Error occurred while creating version dictionary for version_name '{unique_version_name}': {e}")
                    raise
            logger.info(f"{self} version_dict created for {unique_version_names}!")
        else:
            logger.info(f"{self} version_dict returns present version_dict for {unique_version_names}!")

        return self._version_dict

    # helper for version_dict
    def get_unique_version_names_in_series(self) -> List[str]:
        """
        This method retrieves all unique 'version_name's from 'Version' that are associated
        with 'Measurement' entities belonging to this 'Series' instance.

        Returns:
            List[str]: List of unique version names
        """
        try:
            # Assuming that session is correctly set up and initialized
            session = db_manager.get_session()

            # Get the table objects from the metadata instance of your engine
            metadata = MetaData()
            measurement_table = Table("Measurement", metadata, autoload_with=session.get_bind())
            version_table = Table("Version", metadata, autoload_with=session.get_bind())

            # Query to get distinct version_names
            unique_version_names = (session.query(distinct(version_table.c.version_name))
                                    .select_from(measurement_table)
                                    .join(version_table)
                                    .filter(measurement_table.c.series_id == self.series_id)
                                    .all())

            # Extract version names from the returned list of tuples
            unique_version_names = [name[0] for name in unique_version_names]
            logger.debug(f"Found the following unique version names '{unique_version_names}'")
            return unique_version_names

        except Exception as e:
            logger.error(f"Error occurred while retrieving unique version names: {e}")
            return []

    @dec_runtime
    def add_filenames(self, csv_path: str, auto_commit: bool = True):
        """
        Update the 'filename' and 'filepath' attributes for each measurement in this series
        by searching for CSV files in the specified path and extracting their names and paths.

        :param csv_path: The path to search for CSV files.
        :param auto_commit:

        Returns:
            None
        """
        csv_path = validate_and_get_path(csv_path)
        if csv_path is None:
            return False

        if self.filepath_tms is None:
            logger.warning(f"No filepath_tms in series {self.series_id}")
            return False

        filepath_tms = validate_and_get_path(self.filepath_tms)
        if filepath_tms is None:
            return False

        search_path = csv_path.joinpath(filepath_tms)
        if not search_path.exists():
            logger.error(f"Search path does not exist: {search_path}")
            return False

        logger.info(f"Searching for TMS files in path: {search_path}")

        files = validate_and_get_file_list(search_path)
        if files is None:
            return False

        sensor_id_list = extract_sensor_id(files)
        if sensor_id_list is None:
            return False

        for measurement in self.measurement:
            if measurement.sensor_id in sensor_id_list:
                corresponding_file = next((file for file in files if int(file.stem[-3:]) == measurement.sensor_id),
                                          None)
                if corresponding_file and corresponding_file.is_file():
                    measurement.filename = corresponding_file.name
                    measurement.filepath = str(corresponding_file)
                else:
                    logger.error(f"The file {corresponding_file} does not exist or is not a CSV file.")
                    return False

        logger.info("The 'filename' and 'filepath' attributes have been successfully updated.")
        if auto_commit:
            db_manager.commit()
        return True

    @dec_runtime
    def limit_by_time(self, version_name: str, start_time: str, end_time: str, auto_commit: bool = False) -> Optional[
        List[Version]]:
        """
        Limits the versions by time range.

        Args:
            version_name (str): Name of the version.
            start_time (str): Start time of the range.
            end_time (str): End time of the range.
            auto_commit (bool, optional): Flag indicating whether to automatically commit changes. Defaults to False.

        Returns:
            List[Version]: List of versions limited by time range, or None if an error occurs.
        """
        logger.info(f"{self} starts limit_by_time for version '{version_name}'")
        versions = self.get_versions_by_filter({"version_name": version_name})
        if versions is None:
            logger.warning(f"No versions found for version name '{version_name}'")
            return None

        try:
            result = [version.limit_by_time(start_time, end_time, auto_commit) for version in versions]
            logger.info(f"{self} successfully limited {len(result)} instances of version '{version_name}' by time")
            return result

        except Exception as e:
            logger.error(f"Error occurred during limit_by_time: {e}")
            return None

    @dec_runtime
    def limit_time_by_peaks(self, version_name, duration: int, show_peaks: bool = False,
                            values_col: str = 'Absolute-Inclination - drift compensated',
                            time_col: str = config.tms_df_time_column_name, n_peaks: int = 10, sample_rate: float = 20,
                            min_time_diff: float = 60, prominence: int = None, auto_commit: bool = False) \
            -> Optional[List[Version]]:
        """
        Begrenzt die Zeiten basierend auf Peaks in den gegebenen Daten.
        """
        logger.info(f"{self} starts limit_by_time for version '{version_name}'.")
        versions = self.get_versions_by_filter({"version_name": version_name})
        if versions is None:
            logger.warning(f"No versions found for version name '{version_name}'")
            return None

        peak_dicts = []
        for version in versions:
            peaks = version.find_n_peaks(show_peaks, values_col, time_col, n_peaks, sample_rate, min_time_diff,
                                         prominence)
            if peaks is None:
                continue
            peak_dicts.append(peaks)

        # Hier aus vielen peaks_dicts in Liste einen peaks_dict zusammensetzen
        merged_peaks = merge_peak_dicts(peak_dicts)
        try:
            timeframe_dict = optimal_time_frame(duration, merged_peaks)
            logger.info(f"Optimaler timeframe: '{timeframe_dict}'")
        except Exception as e:
            logger.error(f"Optimal timeframe konnte nicht ermittelt werden, error: {e}")
            return None

        try:
            result = [version.limit_by_time(timeframe_dict['start_time'], timeframe_dict['end_time'], auto_commit) for
                      version in versions]
            logger.info(f"{self} successfully limited {len(result)} instances of version '{version_name}' by time")

        except Exception as e:
            logger.error(f"Error occurred during limit_by_time: {e}")
            return None

        return result

    @dec_runtime
    def get_wind_df(self, version_name: str, wind_measurement_id: int, time_extension_secs: int=0) \
            -> Optional[List[Version]]:

        logger.info(f"{self} starts get_wind_df for version '{version_name}'")
        versions = self.get_versions_by_filter({"version_name": version_name})
        if versions is None:
            logger.warning(f"No versions found for version name '{version_name}'")
            return None

        try:
            result = [version.get_wind_df(wind_measurement_id, time_extension_secs) for version in versions]
            logger.info(f"{self} successfully loaded wind for {len(result)} instances of version '{version_name}'")
            return result

        except Exception as e:
            logger.error(f"Error occurred during get_wind_df: {e}")
            return None
