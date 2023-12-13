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
                 measurement_status_id=None, filename_tms=None, sensor_location_id=None,
                 sensor_height=None, sensor_circumference=None, sensor_orientation=None):
        super().__init__()
        self.measurement_id = measurement_id
        self.series_id = series_id
        self.tree_treatment_id = tree_treatment_id
        self.sensor_id = sensor_id
        self.measurement_status_id = measurement_status_id
        self.filename_tms = filename_tms
        self.sensor_location_id = sensor_location_id
        self.sensor_height = sensor_height
        self.sensor_circumference = sensor_circumference
        self.sensor_orientation = sensor_orientation
