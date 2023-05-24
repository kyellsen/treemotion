from utils.imports_classes import *

logger = get_logger(__name__)


class SensorStatus(BaseClass):
    """
    This class represents the status of a sensor.
    """
    __tablename__ = 'SensorStatus'
    sensor_status_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)
