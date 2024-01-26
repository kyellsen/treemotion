from ..common_imports.imports_classes import *

from .sensor import Sensor
from .tree_treatment import TreeTreatment
from .measurement_status import MeasurementStatus
from .measurement_version import MeasurementVersion
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
    filename_tms = Column(String)
    filepath_tms = Column(String)
    sensor_location_id = Column(Integer, ForeignKey('SensorLocation.sensor_location_id', onupdate='CASCADE'))
    sensor_height = Column(Integer)
    sensor_circumference = Column(Integer)
    sensor_orientation = Column(Integer)

    sensor = relationship(Sensor, backref="measurement", lazy="joined")
    tree_treatment = relationship(TreeTreatment, backref="measurement", lazy="joined")
    measurement_status = relationship(MeasurementStatus, backref="measurement", lazy="joined")
    sensor_location = relationship(SensorLocation, backref="measurement", lazy="joined")

    measurement_version = relationship(MeasurementVersion, backref="measurement", lazy="joined",
                                       cascade='all, delete-orphan',
                                       order_by='MeasurementVersion.measurement_version_id')

    def __init__(self, measurement_id=None, series_id=None, tree_treatment_id=None, sensor_id=None,
                 measurement_status_id=None, filename_tms=None, filepath_tms=None, sensor_location_id=None,
                 sensor_height=None, sensor_circumference=None, sensor_orientation=None):
        super().__init__()
        self.measurement_id = measurement_id
        self.series_id = series_id
        self.tree_treatment_id = tree_treatment_id
        self.sensor_id = sensor_id
        self.measurement_status_id = measurement_status_id
        self.filename_tms = filename_tms
        self.filepath_tms = filepath_tms
        self.sensor_location_id = sensor_location_id
        self.sensor_height = sensor_height
        self.sensor_circumference = sensor_circumference
        self.sensor_orientation = sensor_orientation

    def __str__(self) -> str:
        """
        Represents the Measurement instance as a string.

        :return: A string representation of the Measurement instance.
        """
        return f"Measurement(id={self.measurement_id}, series_id={self.series_id}, filename={self.filepath_tms})"

    @dec_runtime
    def load_from_csv(self, measurement_version_name: str = None,
                      update_existing: bool = False) -> Optional[MeasurementVersion]:
        """
        Load data from a CSV file and update existing data if necessary.

        :param measurement_version_name: MeasurementVersion of the data.
        :param update_existing: Determines whether to overwrite existing data.
        :return: The updated or newly created MeasurementVersion instance, or None if an error occurs.
        """

        if self.filepath_tms is None or not Path(self.filepath_tms).is_file():
            logger.warning(f"Process for '{self}' canceled, no filepath_tms: '{self.filepath_tms}'.")
            return None

        logger.info(f"Start loading TMS data from CSV for '{self}'")

        try:
            m_v_name = measurement_version_name or self.get_config().MeasurementVersion.default_load_from_csv_measurement_version_name

            present_m_v = (self.get_database_manager().session.query(MeasurementVersion)
                           .filter(MeasurementVersion.measurement_id == self.measurement_id,
                                   MeasurementVersion.measurement_version_name == m_v_name)
                           .first())

        except Exception as e:
            logger.error(
                f"Failed to retrieve MeasurementVersion '{measurement_version_name}' for Measurement ID '{self.measurement_id}'. Error: {e}")
            return None

        # Wenn present_m_v vorhanden und overwrite=False -> return present_m_v
        if present_m_v and not update_existing:
            logger.warning(f"Existing measurement_version '{m_v_name}' will not be overwritten: '{present_m_v}'")
            return present_m_v
        try:
            DATABASE_MANAGER = self.get_database_manager()
            session = DATABASE_MANAGER.session
            if present_m_v and update_existing:
                logger.warning(f"Existing measurement_version '{m_v_name}' will be overwritten: '{present_m_v}'")
                session.delete(present_m_v)
                session.flush()

            m_v = MeasurementVersion.load_tms_from_csv(filepath_tms=self.filepath_tms, measurement_id=self.measurement_id,
                                                       measurement_version_id=None, measurement_version_name=m_v_name)
            self.measurement_version.append(m_v)
            DATABASE_MANAGER.commit()
            logger.info(f"{'Updated' if present_m_v else 'Created'} '{m_v}' from CSV, attached to '{self}'.")
            return m_v
        except Exception as e:
            logger.error(
                f"Failed to {'update' if present_m_v else 'create'} MeasurementVersion '{m_v_name}' for '{self}', error: {e}")

            return None

    def get_measurement_version_by_filter_by_filter(self, filter_dict: Dict[str, Any], method: str = "list_filter") -> Optional[List[MeasurementVersion]]:
        """
        Find all MeasurementVersion objects based on the provided filters.

        :param filter_dict: A dictionary of attributes and their desired values.
                            For example: {'tms_table_name': tms_table_name}
        :param method: The method to use for filtering. Possible values are "list_filter" and "db_filter".
                       The default value is "list_filter".
        :return: A list of found MeasurementVersion instances, or None if no match is found.
        """

        if not isinstance(filter_dict, dict):
            logger.error("Input filter is not a dictionary. Please provide a valid filter dictionary.")
            return None

        try:
            if method == "list_filter":
                matching_mv: List = [mv for mv in self.measurement_version if
                                     all(getattr(mv, k, None) == v for k, v in filter_dict.items())]
            elif method == "db_filter":
                session = self.get_database_manager().session()
                matching_mv: List = (session.query(MeasurementVersion)
                                     .filter(MeasurementVersion.measurement == self)
                                     .filter_by(**filter_dict)
                                     .all())
            else:
                logger.error("Invalid method. Please choose between 'list_filter' and 'db_filter'.")
                return None

            if not matching_mv:
                logger.debug(f"No MeasurementVersion instance found with the given filters: {filter_dict}.")
                return None

            logger.info(f"{len(matching_mv)} MeasurementVersion instances found with the given filters: {filter_dict}.")
            return matching_mv

        except Exception as e:
            logger.error(f"An error occurred while querying the MeasurementVersion: {str(e)}")
            return None
