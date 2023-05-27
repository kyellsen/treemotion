from common_imports.classes_light import *

class SensorType(BaseClass):
    """
    This class represents the type of a sensor.
    """
    __tablename__ = 'SensorType'
    sensor_type_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)
