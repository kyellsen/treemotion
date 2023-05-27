# treemotion/classes/measurement.py

from common_imports.classes_heavy import *

from .version import Version
from .sensor import Sensor
from .tree_treatment import TreeTreatment
from .measurement_status import MeasurementStatus
from .sensor_location import SensorLocation

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

    version = relationship('Version', back_populates="measurement", lazy="joined", cascade='all, delete-orphan',
                           order_by='Version.version_id')

    def __init__(self, *args, measurement_id: int = None, series_id: int = None,
                 tree_treatment_id: int = None, sensor_id: int = None, measurement_status_id: int = None,
                 filename: str = None, filepath: str = None, sensor_location_id: int = None, sensor_height: int = None,
                 sensor_circumference: int = None, sensor_compass_direction: int = None, **kwargs):
        """
        Initialize a new instance of the Measurement class.

        :param measurement_id: The ID of the measurement.
        :param series_id: The ID of the series.
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

    def __str__(self) -> str:
        """
        Represents the Measurement instance as a string.

        :return: A string representation of the Measurement instance.
        """
        return f"Measurement(id={self.measurement_id}, series_id={self.series_id}, filename={self.filename})"

    @dec_runtime
    def load_from_csv(self, version_name: str = config.default_load_from_csv_version_name,
                      overwrite: bool = False, auto_commit: bool = True) -> Optional[Version]:
        """
        Load data from a CSV file and update existing data if necessary.

        :param version_name: Version of the data.
        :param overwrite: Determines whether to overwrite existing data.
        :param auto_commit: Determines whether to save the data to the database immediately.
        :return: The updated or newly created Version instance, or None if an error occurs.
        """
        if self.filepath is None:
            logger.warning(f"Process for '{self}' aborted, no filename for tms_utils.csv (filename = None).")
            return None

        logger.info(f"Start loading TMS-CSV data for '{self}'")
        tms_table_name = Version.get_tms_table_name(version_name, self.measurement_id)
        present_version = self.get_by_table_name(tms_table_name)

        if present_version is None or overwrite:
            if present_version is not None and overwrite:
                logger.warning(f"Existing object will be overwritten (overwrite = True): {present_version}")
            obj = self.create_update_version_from_csv(version_name, present_version, auto_commit)
        else:
            logger.warning(f"Object already exists, not overwritten (overwrite = False), obj: {present_version}")
            return None

        if auto_commit:
            db_manager.commit()
        logger.debug(f"Loading TMS-CSV data for '{self}' successful")
        return obj

    # Hilfsmethode für load_from_csv
    def create_update_version_from_csv(self, version_name: str, present_version: Optional[Version] = None,
                                       auto_commit: bool = True) -> Optional[Version]:
        """
        Create or update a data object from a CSV file.

        :param version_name: The version of the data.
        :param present_version: An existing Version instance to update.
        :param auto_commit: Determines whether to save the data to the database immediately.
        :return: The created or updated Version instance, or None if an error occurs.
        """
        version_id = present_version.version_id if present_version else None
        if present_version:
            present_version.delete_from_db()

        obj = Version.load_from_csv(self.filepath, self.measurement_id, version_id, version_name, auto_commit)

        if obj is None:
            logger.error(f"Failed to create Version instance from csv file: {self.filepath}.")
            return None

        self.version.append(obj)
        logger.info(f"Object {obj.__str__()} successfully created/updated and attached to {self.__str__()}.")
        return obj

    # Hilfsmethode für load_from_csv
    def get_by_table_name(self, tms_table_name: str) -> Optional[Version]:
        """
        Find a version object based on its tms table name.

        :param tms_table_name: The name of the table being searched.
        :return: The found version instance, or None if no match is found.
        """
        matching_versions = [version for version in self.version if version.tms_table_name == tms_table_name]

        if not matching_versions:
            logger.debug(f"No Version instance found with tms table_name '{tms_table_name}'.")
            return None

        if len(matching_versions) > 1:
            logger.warning(
                f"Multiple Version instances found with table_name {tms_table_name}. Returning only the first instance.")

        logger.debug(f"Version instance found with table_name {tms_table_name}.")
        return matching_versions[0]

    def get_by_version_name(self, version_name):
        """
        This method finds an instance in self.version that has the given version_name.
        It logs a critical error and returns None if more than one instance is found.
        It logs an error and returns None if no instance is found.
        """
        matching_versions = [version for version in self.version if version.version == version_name]
        if len(matching_versions) > 1:
            logger.critical(
                f"Multiple Version instances with version '{version_name}' for '{self.__str__()}' not available. Returning first one.")
        if len(matching_versions) == 0:
            logger.warning(f"No Version instances with version '{version_name}' found for '{self.__str__()}'.")
            return None
        logger.debug(f"Version instance {matching_versions[0].__str__()} returned.")
        return matching_versions[0]

    # def load_data_by_version(self, version):
    #     obj = self.get_data_by_version(version)
    #     if obj is None:
    #         return None
    #     result = obj.load_df()
    #     return result
    #
    # @dec_runtime
    # def copy_data_by_version(self, version_new=config.default_copy_data_by_version_name,
    #                          version_source=config.default_load_from_csv_version_name, auto_commit=False):
    #     """
    #     This method copies a data instance with version_source to a new instance with version_new.
    #     It logs an error and returns None if version_new and version_source are the same.
    #     It logs a critical error and returns None if the table name of the new instance matches that of the source instance.
    #     It logs an error and returns None if an error occurs while copying the instance or committing.
    #     """
    #     if version_new == version_source:
    #         logger.error(
    #             f"Error: version_new '{version_new}' must not be the same as version_source '{version_source}'!")
    #         return None
    #
    #     for old_obj in self.version:
    #         if old_obj.version == version_new:
    #             logger.info(
    #                 f"An instance of Version with version '{version_new}' already exists. Returning old instance.")
    #             return old_obj
    #
    #     source_obj = self.get_data_by_version(version_source)
    #     if source_obj is None:
    #         logger.warning(
    #             f"Process to copy '{self.__str__()}' from version '{version_source}' to version '{version_new}' aborted.")
    #         return None
    #
    #     # Load the data for this Version instance if it hasn't been loaded yet.
    #     source_obj.load_df_if_missing()
    #
    #     new_obj = Version.create_new_version(source_obj, version_new)
    #
    #     if new_obj is None:
    #         return None
    #
    #     if new_obj.tms_table_name == source_obj.table_name:
    #         logger.critical(
    #             f"Table name for new instance is the same as the source instance table name.")
    #         return None
    #
    #     self.version.append(new_obj)
    #
    #     if auto_commit:
    #         try:
    #             new_obj.commit()
    #         except Exception as e:
    #             logger.error(f"Error committing the new data instance: {e}")
    #             return None
    #
    #     logger.info(f"Successful creation of '{version_new.__str__()}' (auto_commit = '{auto_commit}').")
    #
    #     return new_obj
    #
    # def commit_data_by_version(self, version, session=None):
    #     obj = self.get_data_by_version(version)
    #     if obj is None:
    #         logger.warning(f"Commit for {self.__str__()} aborted.")
    #         return False
    #     # Load the data for this Version instance if it hasn't been loaded yet.
    #     obj.load_df_if_missing(session=session)
    #     result = obj.commit()
    #     return result
    #
    # def limit_time_data_by_version(self, version, start_time: str, end_time: str, auto_commit: bool = False,
    #                                session=None):
    #     obj = self.get_data_by_version(version)
    #     if obj is None:
    #         logger.warning(f"Process for time limiting {self.__str__()} aborted.")
    #         return None
    #     # Load the data for this Version instance if it hasn't been loaded yet.
    #     obj.load_df_if_missing(session=session)
    #
    #     result = obj.limit_by_time(start_time, end_time, auto_commit, session)
    #     return result
