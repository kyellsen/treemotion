from ..common_imports.imports_classes import *


class SensorLocation(BaseClass):
    """
    This class represents the location of a sensor in Tree.
    """
    __tablename__ = 'SensorLocation'
    sensor_location_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    sensor_location = Column(String)
