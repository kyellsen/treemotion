from utils.imports_classes import *

logger = get_logger(__name__)


class SensorLocation(BaseClass):
    """
    This class represents the location of a sensor in Tree.
    """
    __tablename__ = 'SensorLocation'
    sensor_location_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)
