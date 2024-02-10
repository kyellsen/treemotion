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
        return f"Measurement(id={self.measurement_id}, series_id={self.series_id})"

    @dec_runtime
    def load_from_csv(self, measurement_version_name: str = None,
                      update_existing: bool = True) -> Optional[MeasurementVersion]:
        """
        Loads data from a CSV file into a MeasurementVersion instance.

        Attempts to find an existing MeasurementVersion based on the provided name or a default.
        If found and `update_existing` is False, returns the found instance without changes.
        If not found, creates a new MeasurementVersion from the CSV.
        If found and `update_existing` is True, updates the existing MeasurementVersion from the CSV.

        Args:
            measurement_version_name (str, optional): Name of the MeasurementVersion. Defaults to None, which uses the default name from config.
            update_existing (bool): Whether to update an existing MeasurementVersion with the same name. Defaults to True.

        Returns:
            Optional[MeasurementVersion]: The updated, newly created, or found MeasurementVersion instance, or None if an error occurs.
        """
        logger.info(f"Start loading TMS data from CSV for '{self}'")
        try:
            mv_name = measurement_version_name or self.get_config().MeasurementVersion.measurement_version_name_default

            m_v_present: MeasurementVersion = (self.get_database_manager().session.query(MeasurementVersion)
                                               .filter(MeasurementVersion.measurement_id == self.measurement_id,
                                                       MeasurementVersion.measurement_version_name == mv_name)
                                               .first())

        except Exception as e:
            logger.error(
                f"Failed to retrieve MeasurementVersion '{measurement_version_name}' for Measurement ID '{self.measurement_id}'. Error: {e}")
            return None

        if m_v_present and not update_existing:
            # Fall 1: Ein vorhandenes Objekt existiert und soll nicht aktualisiert werden.
            # Gib das vorhandene Objekt zurück.
            logger.warning(f"Existing measurement_version '{mv_name}' not updated: '{m_v_present}'")
            return m_v_present

        elif not m_v_present:
            # Fall 2: Kein vorhandenes Objekt existiert.
            # Erstelle ein neues Objekt und gib dieses zurück.
            try:
                mv_new = MeasurementVersion.create_from_csv(self.filepath_tms, self.measurement_id, mv_name)
                DATABASE_MANAGER = self.get_database_manager()
                self.measurement_version.append(mv_new)
                DATABASE_MANAGER.commit()
                logger.info(f"New measurement_version '{mv_name}' created: '{mv_new}'")
                return mv_new
            except Exception as e:
                logger.error(f"Failed to create MeasurementVersion '{mv_name}' for '{self}', error: {e}")

        elif m_v_present and update_existing:
            # Fall 3: Ein vorhandenes Objekt existiert und soll aktualisiert werden.
            # Aktualisiere das vorhandene Objekt und gib es zurück.
            try:
                mv_updated = m_v_present.update_from_csv(self.filepath_tms)
                DATABASE_MANAGER = self.get_database_manager()
                self.measurement_version.append(mv_updated)
                DATABASE_MANAGER.commit()
                logger.info(f"Existing measurement_version '{mv_name}' updated: '{mv_updated}'")
                return mv_updated
            except Exception as e:
                logger.error(f"Failed to update MeasurementVersion '{mv_name}' for '{self}', error: {e}")
        return None

    def get_measurement_version_by_filter(self, filter_dict: Dict[str, Any], method: str = "list_filter") -> Optional[
        List[MeasurementVersion]]:
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
                matching_mv_list: List = [mv for mv in self.measurement_version if
                                          all(getattr(mv, k, None) == v for k, v in filter_dict.items())]
            elif method == "db_filter":
                session = self.get_database_manager().session()
                matching_mv_list: List = (session.query(MeasurementVersion)
                                          .filter(MeasurementVersion.measurement == self)
                                          .filter_by(**filter_dict)
                                          .all())
            else:
                raise ValueError("Invalid method. Please choose between 'list_filter' and 'db_filter'.")

            if len(matching_mv_list) <= 0:
                raise ValueError(f"No measurement_version found for filter: '{filter_dict}'")

            return matching_mv_list

        except Exception as e:
            logger.error(f"An error occurred while querying the Version: {e}")
            return None
