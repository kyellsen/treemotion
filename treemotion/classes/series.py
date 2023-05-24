# treemotion/classes/series.py
from utils.imports_classes import *
from utils.path_utils import validate_and_get_path, validate_and_get_file_list, extract_id_sensor_list
from tms.time_limits import optimal_time_frame

from .measurement import Measurement
from .wind_measurement import WindMeasurement

logger = get_logger(__name__)


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
    measurement = relationship(Measurement, back_populates="series", lazy="joined",
                               cascade='all, delete, delete-orphan', order_by=Measurement.measurement_id)
    wind_measurement = relationship(WindMeasurement, lazy="joined")

    def __init__(self, *args, series_id=None, description=None, datetime_start=None, datetime_end=None, location=None,
                 annotation=None, filepath_tms=None, wind_measurement_id=None, **kwargs):
        super().__init__(*args, **kwargs)

        # in SQLite Database
        self.series_id = series_id
        self.description = description
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self.location = location
        self.annotation = annotation
        self.filepath_tms = filepath_tms
        self.wind_measurement_id = wind_measurement_id

    def __str__(self):
        return f"Series(series_id={self.series_id}, series_id={self.series_id}"

    @classmethod
    @timing_decorator
    def load_from_db(cls, project_id=None, session=None):
        objs = super().load_from_db(filter_by={'project_id': project_id} if project_id else None, session=session)
        logger.info(f"{len(objs)} series were successfully loaded.")
        return objs

    @timing_decorator
    def load_data_from_csv(self, version=config.default_load_data_from_csv_version_name, overwrite=False,
                           auto_commit=False,
                           session=None):
        logger.info(f"Starting process to load all CSV files for {self.__str__()}")
        try:
            results = self.for_all('measurements', 'load_data_from_csv', version, overwrite, auto_commit, session)
        except Exception as e:
            logger.error(f"Error loading all CSV files for {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Process of loading CSV files for {len(results)} measurements from {self.__str__()} successfully completed.")
        return results

    @timing_decorator
    def copy(self, reset_id=False, auto_commit=False, session=None) -> 'Series':
        """
        Create a copy of the series.

        Args:
            reset_id (bool, optional): Whether to reset the ID of the new instance.
            auto_commit (bool, optional): Whether to automatically commit the new instance to the database.
            session (Session, optional): The database session to use.

        Returns:
            Series: A new instance that is a copy of the current series.
        """
        new_obj = super().copy("series_id", reset_id, auto_commit, session)
        return new_obj

    @timing_decorator
    def remove(self, auto_commit=False, session=None) -> bool:
        """
        Remove the series from the database.

        Args:
            auto_commit (bool, optional): Whether to automatically commit the changes to the database.
            session (Session, optional): The database session to use.

        Returns:
            bool: True if the series was successfully removed, False otherwise.
        """
        session = db_manager.get_session(session)
        # Call the base class method to remove this series object from the database
        result = super().remove('series_id', auto_commit, session)
        return result

    @timing_decorator
    def add_filenames(self, csv_path: str):
        """
        Update the 'filename' and 'filepath' attributes for each measurement in this series
        by searching for CSV files in the specified path and extracting their names and paths.

        Args:
            csv_path (str): The path to search for CSV files.

        Returns:
            None
        """
        csv_path = validate_and_get_path(csv_path)
        if csv_path is None:
            return None

        if self.filepath_tms is None:
            logger.warning(f"No filepath_tms in series {self.series_id}")
            return None

        filepath_tms = validate_and_get_path(self.filepath_tms)
        if filepath_tms is None:
            return None

        search_path = csv_path.joinpath(filepath_tms)
        if not search_path.exists():
            logger.error(f"Search path does not exist: {search_path}")
            return None

        logger.info(f"Searching for TMS files in path: {search_path}")

        files = validate_and_get_file_list(search_path)
        if files is None:
            return None

        id_sensor_list = extract_id_sensor_list(files)
        if id_sensor_list is None:
            return None

        for measurement in self.measurement:
            if measurement.id_sensor in id_sensor_list:
                corresponding_file = next((file for file in files if int(file.stem[-3:]) == measurement.id_sensor),
                                          None)
                if corresponding_file and corresponding_file.is_file():
                    measurement.filename = corresponding_file.name
                    measurement.filepath = str(corresponding_file)
                else:
                    logger.error(f"The file {corresponding_file} does not exist or is not a CSV file.")
                    return None

        logger.info("The 'filename' and 'filepath' attributes have been successfully updated.")

    @timing_decorator
    def get_data_by_version(self, version: str):
        """
        Get the data instances for the specified version.

        Args:
            version (str): The version of the data.

        Returns:
            List: A list of data instances matching the specified version.
        """
        try:
            results = self.for_all('measurements', 'get_data_by_version', version)
        except Exception as e:
            logger.error(
                f"Error searching for data instances with version '{version}' from {self.__str__()}, Error: {e}")
            return None
        return results

    @timing_decorator
    def load_data_by_version(self, version: str, session=None):
        """
        Load the data frames for the specified version.

        Args:
            version (str): The version of the data.
            session (Session, optional): The database session to use.

        Returns:
            List: A list of data frames loaded for the measurements.
        """
        logger.info(f"Starting process to load data frames in {self.__str__()} with version: {version}")
        try:
            results = self.for_all('measurements', 'load_data_by_version', version, session)
        except Exception as e:
            logger.error(f"Error loading data frames for {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Process of loading data frames for {len(results)} measurements from {self.__str__()} successfully completed.")
        return results

    @timing_decorator
    def copy_data_by_version(self, version_new=config.default_copy_data_by_version_name,
                             version_source=config.default_load_data_from_csv_version_name, auto_commit=False,
                             session=None):
        """
        Copy the data objects for the specified version.

        Args:
            version_new (str, optional): The new version to assign to the copied data objects.
            version_source (str, optional): The version of the source data to copy.
            auto_commit (bool, optional): Whether to automatically commit the copied data objects to the database.
            session (Session, optional): The database session to use.

        Returns:
            List: A list of copied data objects.
        """
        logger.info(f"Starting process to copy all data objects in {self.__str__()} with version: {version_source}")
        try:
            results = self.for_all('measurements', 'copy_data_by_version', version_new, version_source, auto_commit,
                                   session)
        except Exception as e:
            logger.error(f"Error copying all data objects for {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Process of copying all data objects for {len(results)} measurements from {self.__str__()} successfully completed.")
        return results

    @timing_decorator
    def commit_data_by_version(self, version, session=None):
        logger.info(f"Starte Prozess zum Commiten aller Data-Objekte in {self.__str__()} mit Version: {session}")
        try:
            results = self.for_all('messungen', 'commit_data_by_version', version, session)
        except Exception as e:
            logger.error(f"Fehler beim Commiten aller Data-Objekte für {self.__str__()}, Error: {e}")
            return False
        # Zählt die Anzahl der erfolgreichen Ergebnisse (die nicht False sind)
        successful = sum(1 for result in results if result is not False)
        logger.info(
            f"Prozess zum Commiten von Data-Objekten für {successful}/{len(results)} für Messungen aus {self.__str__()} erfolgreich.")
        return results

    @timing_decorator
    def limit_time_data_by_version(self, version, start_time: str, end_time: str, auto_commit: bool = False,
                                   session=None):
        logger.info(
            f"Starte Prozess zur Zeiteinschränkung aller Data-Objekte in {self.__str__()} mit Version: {version}")
        try:
            results = self.for_all('messungen', 'limit_time_data_by_version', version, start_time, end_time,
                                   auto_commit, session)
        except Exception as e:
            logger.error(f"Fehler bei Zeiteinschränkung aller Data-Objekte für {self.__str__()}, Error: {e}")
            return None
        # Zählt die Anzahl der erfolgreichen Ergebnisse (die nicht False sind)
        successful = sum(1 for result in results if result is not False)
        logger.info(
            f"Prozess zur Zeiteinschränkung von Data-Objekten für {successful}/{len(results)} für Messungen aus {self.__str__()} erfolgreich.")
        return results

    def limit_time_by_version_and_peaks(self, version, duration: int, show_peaks: bool = False,
                                        values_col: str = 'Absolute-Inclination - drift compensated',
                                        time_col: str = 'Time', n_peaks: int = 10, sample_rate: float = 20,
                                        min_time_diff: float = 60, prominence: int = None, auto_commit: bool = False,
                                        session=None):
        """
        Begrenzt die Zeiten basierend auf Peaks in den gegebenen Daten.
        """

        objs = self.get_data_by_version(version)
        peak_dicts = []

        for obj in objs:
            peaks = obj.find_n_peaks(show_peaks, values_col, time_col, n_peaks, sample_rate, min_time_diff, prominence)
            if peaks is None:
                continue
            peak_dicts.append(peaks)

        # Hier aus vielen peaks_dicts in Liste einen peaks_dict zusammensetzen
        merged_peaks = self.merge_peak_dicts(peak_dicts)
        try:
            timeframe_dict = optimal_time_frame(duration, merged_peaks)
        except Exception as e:
            logger.error(f"Optimaler Timeframe konnte nicht ermittelt werden, error: {e}")
            return False

        for obj in objs:
            obj.limit_by_time(timeframe_dict['start_time'], timeframe_dict['end_time'], auto_commit, session)

        return True

    @staticmethod
    def merge_peak_dicts(peak_dicts):
        """
        Führt eine Liste von 'peak' Wörterbüchern zusammen.
        """
        return {
            'peak_index': [index for peaks in peak_dicts for index in peaks['peak_index']],
            'peak_time': [time for peaks in peak_dicts for time in peaks['peak_time']],
            'peak_value': [value for peaks in peak_dicts for value in peaks['peak_value']]
        }
