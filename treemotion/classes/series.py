# treemotion/classes/series.py
from common_imports.classes_heavy import *
from utils.path_utils import validate_and_get_path, validate_and_get_file_list, extract_sensor_id

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

    @property
    def version_dict(self) -> Dict:
        if not hasattr(self, '_version_dict') or self._version_dict is None:
            self._version_dict = {}

        return self._version_dict

    def add_version_list(self, version_name):
        try:
            version_list = self.get_versions_by_version_name(version_name)
            self.version_dict[version_name] = version_list

        except Exception as e:
            raise e
        return self.version_dict[version_name]

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

    # def limit_time_by_version_and_peaks(self, version, duration: int, show_peaks: bool = False,
    #                                     values_col: str = 'Absolute-Inclination - drift compensated',
    #                                     time_col: str = 'Time', n_peaks: int = 10, sample_rate: float = 20,
    #                                     min_time_diff: float = 60, prominence: int = None, auto_commit: bool = False,
    #                                     session=None):
    #     """
    #     Begrenzt die Zeiten basierend auf Peaks in den gegebenen Daten.
    #     """
    #
    #     objs = self.get_data_by_version(version)
    #     peak_dicts = []
    #
    #     for obj in objs:
    #         peaks = obj.find_n_peaks(show_peaks, values_col, time_col, n_peaks, sample_rate, min_time_diff, prominence)
    #         if peaks is None:
    #             continue
    #         peak_dicts.append(peaks)
    #
    #     # Hier aus vielen peaks_dicts in Liste einen peaks_dict zusammensetzen
    #     merged_peaks = self.merge_peak_dicts(peak_dicts)
    #     try:
    #         timeframe_dict = optimal_time_frame(duration, merged_peaks)
    #     except Exception as e:
    #         logger.error(f"Optimaler Timeframe konnte nicht ermittelt werden, error: {e}")
    #         return False
    #
    #     for obj in objs:
    #         obj.limit_by_time(timeframe_dict['start_time'], timeframe_dict['end_time'], auto_commit, session)
    #
    #     return True
    #
    # @staticmethod
    # def merge_peak_dicts(peak_dicts):
    #     """
    #     Führt eine Liste von 'peak' Wörterbüchern zusammen.
    #     """
    #     return {
    #         'peak_index': [index for peaks in peak_dicts for index in peaks['peak_index']],
    #         'peak_time': [time for peaks in peak_dicts for time in peaks['peak_time']],
    #         'peak_value': [value for peaks in peak_dicts for value in peaks['peak_value']]
    #    }
