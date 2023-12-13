from ..common_imports.imports_classes import *

from .sensor_type import SensorType
from .sensor_status import SensorStatus


class Sensor(BaseClass):
    """
    This class represents a sensor in the system.
    """
    __tablename__ = 'Sensor'
    sensor_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    sensor_type_id = Column(Integer, ForeignKey('SensorType.sensor_type_id', onupdate='CASCADE'))
    sensor_status_id = Column(Integer, ForeignKey('SensorStatus.sensor_status_id', onupdate='CASCADE'))
    note = Column(String)

    sensor_type = relationship(SensorType, backref="sensor", lazy="joined")
    sensor_status = relationship(SensorStatus, backref="sensor", lazy="joined")
