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

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        # in SQLite Database
        self.measurement_id = kwargs.get('measurement_id', None)
        self.series_id = kwargs.get('series_id', None)
        self.tree_treatment_id = kwargs.get('tree_treatment_id', None)
        self.sensor_id = kwargs.get('sensor_id', None)
        self.measurement_status_id = kwargs.get('measurement_status_id', None)
        self.filename = kwargs.get('filename', None)
        self.filepath = kwargs.get('filepath', None)
        self.sensor_location_id = kwargs.get('sensor_location_id', None)
        self.sensor_height = kwargs.get('sensor_height', None)
        self.sensor_circumference = kwargs.get('sensor_circumference', None)
        self.sensor_orientation = kwargs.get('sensor_compass_direction', None)

    def __str__(self) -> str:
        """
        Represents the Measurement instance as a string.

        :return: A string representation of the Measurement instance.
        """
        return f"Measurement(id={self.measurement_id}, series_id={self.series_id}, filename={self.filename})"

    @dec_runtime
    def load_from_csv(self, version_name: str = config.default_load_from_csv_version_name,
                      overwrite: bool = False) -> Optional[Version]:
        """
        Load data from a CSV file and update existing data if necessary.

        :param version_name: Version of the data.
        :param overwrite: Determines whether to overwrite existing data.
        :return: The updated or newly created Version instance, or None if an error occurs.
        """
        if self.filepath is None:
            logger.warning(
                f"Process for '{self}' canceled, no filename for tms_utils.csv (filename = {self.filename}).")
            return None

        logger.info(f"Start loading TMS data from CSV for '{self}'")

        tms_table_name = Version.get_tms_table_name(version_name, self.measurement_id)
        tms_wind_table_name = Version.get_tms_wind_table_name(version_name, self.measurement_id)
        versions = self.get_versions_by_filter({'tms_table_name': tms_table_name})
        present_version = versions[0] if versions is not None else None

        if present_version is not None and not overwrite:
            logger.warning(
                f"Existing version '{version_name}' will be not overwritten (overwrite = False): '{present_version}'")
            return present_version

        elif present_version is not None and overwrite:
            logger.warning(
                f"Existing version '{version_name}' will be overwritten (overwrite = True): '{present_version}'")
            version_id = present_version.version_id
            self.version.remove(present_version)

        else:  # present_version is None:
            logger.debug(f"Create new Version with version_name '{version_name}'.")
            version_id = None
        try:
            version = Version.load_from_csv(self.filepath, self.measurement_id, version_id, version_name,
                                            tms_table_name, tms_wind_table_name)
            self.version.append(version)
            logger.info(f"Loading {version} from CSV successful, attached to {self}.")
            return version

        except Exception as e:
            logger.error(f"Failed to create Version '{version_name}' from {self}, csv file:{self.filepath}, error: {e}")
            return None

    def get_versions_by_filter(self, filter_dict: Dict[str, Any], method: str = "list_filter") -> Optional[List[Version]]:
        """
        Find all version objects based on the provided filters.

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
                matching_versions = [version for version in self.version if
                                     all(getattr(version, k, None) == v for k, v in filter_dict.items())]
            elif method == "db_filter":
                session = db_manager.get_session()
                matching_versions = (session.query(Version)
                                     .filter(Version.measurement == self)
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
    #     new_obj = Version.create_copy(source_obj, version_new)
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
