# treemotion/classes/measurement.py

from utils.imports_classes import *

from .version import Version
from .tree import Tree
from .tree_treatment import TreeTreatment
from .sensor import Sensor
from .sensor_location import SensorLocation
from .measurement_status import MeasurementStatus

logger = get_logger(__name__)


class Measurement(BaseClass):
    """
    This class represents a measurement in the system.
    """
    __tablename__ = 'Measurement'
    measurement_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    series_id = Column(Integer, ForeignKey('Series.series_id', onupdate='CASCADE'))
    tree_treatment_id = Column(Integer, ForeignKey('TreeTreatment.tree_treatment_id', onupdate='CASCADE'))
    sensor_id = Column(Integer, ForeignKey('Sensor.sensor_id', onupdate='CASCADE'))
    measurement_status_id = Column(Integer, ForeignKey('MeasurementStatus.measurement_status_id',
                                                       onupdate='CASCADE'))
    filename = Column(String)
    filepath = Column(String)
    sensor_location_id = Column(Integer, ForeignKey('SensorLocation.sensor_location_id', onupdate='CASCADE'))
    sensor_height = Column(Integer)
    sensor_circumference = Column(Integer)
    sensor_compass_direction = Column(Integer)

    sensor = relationship(Sensor, backref="measurement", lazy="joined")
    tree_treatment = relationship(TreeTreatment, backref="measurement", lazy="joined")
    measurement_status = relationship(MeasurementStatus, backref="measurement", lazy="joined")
    sensor_location = relationship(SensorLocation, backref="measurement", lazy="joined")
    series = relationship('Series', back_populates='measurement')

    version = relationship(Version, back_populates="measurement", lazy="joined", cascade="all, delete, delete-orphan",
                           order_by=Version.version_id)

    def __init__(self, *args, measurement_id: int = None, series_id: int = None, tree_id: int = None,
                 tree_treatment_id: int = None, sensor_id: int = None, measurement_status_id: int = None,
                 filename: str = None, filepath: str = None, sensor_location_id: int = None, sensor_height: int = None,
                 sensor_circumference: int = None, sensor_compass_direction: int = None, **kwargs):
        """
        Initialize a new instance of the Measurement class.

        :param measurement_id: The ID of the measurement.
        :param series_id: The ID of the series.
        :param tree_id: The ID of the tree.
        :param tree_treatment_id: The ID of the tree treatment.
        :param sensor_id: The ID of the sensor.
        :param measurement_status_id: The ID of the measurement status.
        :param filename: The name of the TMS.csv file.
        :param filepath: The path to the TMS.csv file.
        :param sensor_location_id: The ID of the sensor location.
        :param sensor_height: The height of the sensor.
        :param sensor_circumference: The circumference of the tree trunk at sensor height.
        :param sensor_compass_direction: The orientation of the sensor (degrees relative to North).
        """
        super().__init__(*args, **kwargs)
        # in SQLite Database
        self.measurement_id = measurement_id
        self.series_id = series_id
        self.tree_id = self.tree_treatment.tree_id
        self.tree_treatment_id = tree_treatment_id
        self.sensor_id = sensor_id
        self.measurement_status_id = measurement_status_id
        self.filename = filename
        self.filepath = filepath
        self.sensor_location_id = sensor_location_id
        self.sensor_height = sensor_height
        self.sensor_circumference = sensor_circumference
        self.sensor_orientation = sensor_compass_direction

    def __str__(self):
        return f"Measurement(id={self.measurement_id}, series_id={self.series_id}, filename={self.filename}"

    @classmethod
    @timing_decorator
    def load_from_db(cls, measurement_id=None) -> List['Measurement']:
        if isinstance(measurement_id, list):
            objs = super().load_from_db(ids=measurement_id)
        else:
            objs = super().load_from_db(filter_by={'measurement_id': measurement_id} if measurement_id else None)
        return objs

    @timing_decorator
    def load_data_from_csv(self, version_name: str = config.default_load_data_from_csv_version_name, overwrite: bool = False,
                           auto_commit: bool = False, session=None):
        """
        Load data from a CSV file and update existing data if necessary.

        :param version_name: Version of the data.
        :param overwrite: Determines whether to overwrite existing data.
        :param auto_commit: Determines whether to save the data to the database immediately.
        :param session: SQL-Alchemy session used to interact with the database.
        :return: The updated or newly created data object, or None if an error occurs.
        """
        if self.filepath is None:
            logger.warning(f"Process for '{self.__str__()}' aborted, no filename for tms_utils.csv (filename = None).")
            return None

        logger.info(f"Start loading csv data for '{self.__str__()}'")
        table_name = Version.get_tms_table_name(version_name, self.measurement_id)
        present_data_obj = self.find_data_by_table_name(table_name)

        if present_data_obj is None or overwrite:
            if present_data_obj is not None and overwrite:
                logger.warning(f"Existing object will be overwritten (overwrite = True): {present_data_obj.__str__()}")
            obj = self.create_or_update_data_obj_from_csv(version_name, table_name, present_data_obj)
        else:
            logger.warning(
                f"Object already exists, not overwritten (overwrite = False), obj: {present_data_obj.__str__()}")
            return None

        if auto_commit:
            obj.commit(session)
            logger.info(
                f"Loading data from csv and committing to database {self.__str__()} successful (auto_commit=True)!")
        else:
            logger.info(f"Loading data from csv for {self.__str__()} successful (auto_commit=False)!")
        return obj

    # Hilfsmethode für load_data_from_csv
    def find_data_by_table_name(self, table_name: str):
        """
        Find a data object based on its table name.

        :param table_name: The name of the table being searched.
        :return: The found data object, or None if no match is found.
        """
        matching_data = [data for data in self.version if data.table_name == table_name]

        if not matching_data:
            logger.debug(f"No Version instance found with table_name {table_name}.")
            return None

        if len(matching_data) > 1:
            logger.critical(
                f"Multiple Version instances found with table_name {table_name}. Returning only the first instance.")

        logger.debug(f"Version instance found with table_name {table_name}.")
        return matching_data[0]

    # Hilfsmethode für load_data_from_csv
    def create_or_update_data_obj_from_csv(self, version: str, table_name: str, present_data_obj=None):
        """
        Create or update a data object from a CSV file.

        :param version: The version of the data.
        :param table_name: The name of the table for the data object.
        :param present_data_obj: An existing data object to update.
        :return: The created or updated data object.
        """
        obj = present_data_obj or Version.load_from_csv(filepath=self.filepath, version_id=None,
                                                        measurement_id=self.measurement_id, version_name=version,
                                                        tms_table_name=table_name)
        obj.tms_df = obj.read_csv_tms(self.filepath)
        obj.update_metadata(auto_commit=True)
        if present_data_obj:
            self.version.remove(present_data_obj)
        self.version.append(obj)
        logger.info(f"Object {obj.__str__()} successfully created/updated and attached to {self.__str__()}.")
        return obj



    def get_data_by_version(self, version):
        """
        This method finds an instance in self.data that has the given version.
        It logs a critical error and returns None if more than one instance is found.
        It logs an error and returns None if no instance is found.
        """
        matching_versions = [data for data in self.version if data.version == version]
        if len(matching_versions) > 1:
            logger.critical(
                f"Multiple Version instances with version '{version}' for '{self.__str__()}' not available. Returning first one.")
        if len(matching_versions) == 0:
            logger.warning(f"No Version instances with version '{version}' found for '{self.__str__()}'.")
            return None
        logger.debug(f"Version instance {matching_versions[0].__str__()} returned.")
        return matching_versions[0]

    def load_data_by_version(self, version, session=None):
        obj = self.get_data_by_version(version)
        if obj is None:
            return None
        result = obj.load_df(session)
        return result

    @timing_decorator
    def copy_data_by_version(self, version_new=config.default_copy_data_by_version_name,
                             version_source=config.default_load_data_from_csv_version_name, auto_commit=False,
                             session=None):
        """
        This method copies a data instance with version_source to a new instance with version_new.
        It logs an error and returns None if version_new and version_source are the same.
        It logs a critical error and returns None if the table name of the new instance matches that of the source instance.
        It logs an error and returns None if an error occurs while copying the instance or committing.
        """
        if version_new == version_source:
            logger.error(
                f"Error: version_new '{version_new}' must not be the same as version_source '{version_source}'!")
            return None

        for old_obj in self.version:
            if old_obj.version == version_new:
                logger.info(
                    f"An instance of Version with version '{version_new}' already exists. Returning old instance.")
                return old_obj

        source_obj = self.get_data_by_version(version_source)
        if source_obj is None:
            logger.warning(
                f"Process to copy '{self.__str__()}' from version '{version_source}' to version '{version_new}' aborted.")
            return None

        # Load the data for this Version instance if it hasn't been loaded yet.
        source_obj.load_df_if_missing(session=session)

        new_obj = Version.create_new_version(source_obj, version_new)

        if new_obj is None:
            return None

        if new_obj.tms_table_name == source_obj.table_name:
            logger.critical(
                f"Table name for new instance is the same as the source instance table name.")
            return None

        self.version.append(new_obj)

        if auto_commit:
            try:
                new_obj.commit(session=session)
            except Exception as e:
                logger.error(f"Error committing the new data instance: {e}")
                return None

        logger.info(f"Successful creation of '{version_new.__str__()}' (auto_commit = '{auto_commit}').")

        return new_obj

    def commit_data_by_version(self, version, session=None):
        obj = self.get_data_by_version(version)
        if obj is None:
            logger.warning(f"Commit for {self.__str__()} aborted.")
            return False
        # Load the data for this Version instance if it hasn't been loaded yet.
        obj.load_df_if_missing(session=session)
        result = obj.commit()
        return result

    def limit_time_data_by_version(self, version, start_time: str, end_time: str, auto_commit: bool = False,
                                   session=None):
        obj = self.get_data_by_version(version)
        if obj is None:
            logger.warning(f"Process for time limiting {self.__str__()} aborted.")
            return None
        # Load the data for this Version instance if it hasn't been loaded yet.
        obj.load_df_if_missing(session=session)

        result = obj.limit_by_time(start_time, end_time, auto_commit, session)
        return result
