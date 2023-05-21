# treemotion/classes/wind_messung.py
from sqlalchemy import Column, Integer, Float, ForeignKey, DateTime

from utilities.base import Base


class WindMessung(Base):
    __tablename__ = 'WindMessung'
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    id_wind_messreihe = Column(Integer, ForeignKey('WindMessreihe.id', onupdate='CASCADE'))
    datetime = Column(DateTime)
    quality_level_wind_avg = Column(Float)
    wind_speed_10min_avg = Column(Float)
    wind_direction_10min_avg = Column(Float)
    quality_level_wind_extremes = Column(Float)
    wind_speed_max_10min = Column(Float)
    wind_speed_min_10min = Column(Float)
    wind_speed_max_10min_moving_avg = Column(Float)
    wind_direction_max_wind_speed = Column(Float)

