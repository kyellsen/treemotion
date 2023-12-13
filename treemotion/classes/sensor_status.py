from ..common_imports.imports_classes import *


class SensorStatus(BaseClass):
    """
    This class represents the status of a sensor.
    """
    __tablename__ = 'SensorStatus'
    sensor_status_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    sensor_status = Column(String)
