from ..common_imports.imports_classes import *

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
    note = Column(String)
    filepath_tms = Column(String)
    filepath_ls3 = Column(String)

    measurement = relationship(Measurement, backref="series", lazy="joined",
                               cascade='all, delete-orphan', order_by='Measurement.measurement_id')
    wind_measurement = relationship(WindMeasurement, lazy="joined")

    def __init__(self, series_id=None, project_id=None, wind_measurement_id=None, description=None, datetime_start=None,
                 datetime_end=None, location=None, note=None, filepath_tms=None, filepath_ls3=None):
        super().__init__()
        self.series_id = series_id
        self.project_id = project_id
        self.wind_measurement_id = wind_measurement_id
        self.description = description
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self.location = location
        self.note = note
        self.filepath_tms = filepath_tms
        self.filepath_ls3 = filepath_ls3

        self._version_dict = {}

    def __str__(self):
        return f"Series(series_id={self.series_id}, series_id={self.series_id})"


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
