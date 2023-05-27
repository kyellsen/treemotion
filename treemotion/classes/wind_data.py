# treemotion/classes/wind_data.py
from common_imports.classes_light import *

from classes.base_class import BaseClass


class WindData(BaseClass):
    """
    This class represents wind data in the system.
    """
    __tablename__ = 'WindData'
    wind_data_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    wind_measurement_id = Column(Integer, ForeignKey('WindMeasurement.wind_measurement_id', onupdate='CASCADE'))
    datetime = Column(DateTime)
    quality_level_wind_avg = Column(Float)
    wind_speed_10min_avg = Column(Float)
    wind_direction_10min_avg = Column(Float)
    quality_level_wind_extremes = Column(Float)
    wind_speed_max_10min = Column(Float)
    wind_speed_min_10min = Column(Float)
    wind_speed_max_10min_moving_avg = Column(Float)
    wind_direction_max_wind_speed = Column(Float)
