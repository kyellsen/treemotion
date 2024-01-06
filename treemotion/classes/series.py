from kj_core.utils.path_utils import validate_and_get_file_list, extract_sensor_id, extract_last_three_digits
from ..common_imports.imports_classes import *

from .measurement import Measurement
from .data_wind_station import DataWindStation

import treemotion

logger = get_logger(__name__)


class Series(BaseClass):
    """
    This class represents a series in the system.
    """
    __tablename__ = 'Series'
    series_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    project_id = Column(Integer, ForeignKey('Project.project_id', onupdate='CASCADE'))
    description = Column(String)
    datetime_start = Column(DateTime)
    datetime_end = Column(DateTime)
    location = Column(String)
    data_wind_station_id = Column(Integer, ForeignKey('DataWindStation.data_id', onupdate='CASCADE'))
    note = Column(String)
    filepath_tms = Column(String)
    filepath_ls3 = Column(String)
    measurement = relationship(Measurement, backref="series", lazy="joined",
                               cascade='all, delete-orphan', order_by='Measurement.measurement_id')
    data_wind_station = relationship("DataWindStation", backref="series", cascade='all')

    def __init__(self, series_id=None, project_id=None, description=None, datetime_start=None,
                 datetime_end=None, location=None, note=None, filepath_tms=None, filepath_ls3=None):
        super().__init__()
        self.series_id = series_id
        self.project_id = project_id
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
        Update the 'filename_tms' for each measurement in this series by searching for CSV files in the specified path.
        Logs the number of successful updates.
        """

        if not self.filepath_tms:
            logger.warning(f"No 'filepath_tms' in series {self.series_id}.")
            return

        # Step 0: Validate csv_path
        try:
            csv_path = Path(csv_path)
            if not csv_path.is_dir():
                logger.error(f"Provided path {csv_path} is not a directory.")
                return
        except Exception as e:
            logger.error(f"Invalid path: {csv_path}, Error: {e}")
            return

        # Step 1: Validate filepath_tms
        try:
            # Ensure filepath_tms is treated as a relative path
            if self.filepath_tms.startswith("/"):
                filepath_tms = Path(self.filepath_tms[1:])
            else:
                filepath_tms = Path(self.filepath_tms)
        except Exception as e:
            logger.error(f"Invalid filepath_tms: {self.filepath_tms}, Error: {e}")
            return

        search_path = csv_path.joinpath(filepath_tms)
        if not search_path.is_dir():
            logger.error(f"search_path {search_path} does not exist.")
            return

        # Step 2: Find CSV files in the specified path
        csv_files = validate_and_get_file_list(search_path, file_type="csv")
        if csv_files is None:
            logger.error(f"No CSV files found in {search_path}.")
            return

        # Step 3: Extract sensor IDs from filenames
        sensor_ids = extract_sensor_id(csv_files)
        if sensor_ids is None:
            return

        # Step 4: Update measurements
        successful_updates = 0
        for measurement in self.measurement:
            if measurement.sensor_id in sensor_ids:
                corresponding_file = next(
                    (f for f in csv_files if extract_last_three_digits(f) == measurement.sensor_id), None)
                if corresponding_file and corresponding_file.is_file():
                    measurement.filename_tms = str(corresponding_file.name)
                    measurement.filepath_tms = str(search_path / corresponding_file.name)
                    successful_updates += 1
                else:
                    logger.warning(f"No corresponding file for sensor ID {measurement.sensor_id} found or invalid.")
            else:
                logger.warning(f"Sensor ID {measurement.sensor_id} not found in the extracted sensor IDs.")

        if auto_commit:
            self.get_database_manager().commit()

        total_measurements = len(self.measurement)
        logger.info(f"Updated filenames for {successful_updates} out of {total_measurements} measurements.")

    def add_wind_station(self,
                         station_id: str,
                         alternative_filename_wind: Optional[str] = None,
                         alternative_filename_wind_extreme: Optional[str] = None,
                         alternative_filename_stations_list: Optional[str] = None,
                         auto_commit: bool = True,
                         overwrite: bool = False):
        """
        Adds or updates a wind station in the database.

        :param station_id: Identifier for the station.
        :param alternative_filename_wind: Alternative filename for wind data.
        :param alternative_filename_wind_extreme: Alternative filename for wind extreme data.
        :param alternative_filename_stations_list: Alternative filename for stations list.
        :param auto_commit: If True, commits the transaction automatically.
        :param overwrite: If True, overwrites the existing wind station data; otherwise, retains the existing data.
        :return: DataWindStation instance that was added or found in the database.
        """
        session = self.get_database_manager().session

        try:
            # Check for an existing DataWindStation with the given station_id
            existing_station = session.query(DataWindStation).filter(DataWindStation.station_id == station_id).first()

            if existing_station and not overwrite:
                # Return the existing DataWindStation without creating a new one
                data_wind_station = existing_station
            else:
                # Create a new instance or update the existing one
                if existing_station:
                    session.delete(existing_station)
                    session.flush()

                data_wind_station = DataWindStation.create_from_dwd(station_id, alternative_filename_wind,
                                                                    alternative_filename_wind_extreme,
                                                                    alternative_filename_stations_list)
                session.add(data_wind_station)

            session.flush()
            self.data_wind_station_id = data_wind_station.data_id

            if auto_commit:
                session.commit()
            return data_wind_station

        except Exception as e:
            logger.error(f"Error in add_wind_station: {e}")
            session.rollback()  # Roll back in case of error
            raise  # Optionally re-raise the exception to notify calling functions

