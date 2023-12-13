from ..common_imports.imports_classes import *

from sqlalchemy import func

from .wind_data import WindData

logger = get_logger(__name__)


class WindMeasurement(BaseClass):
    """
    This class represents a wind measurement series in the system.
    """
    __tablename__ = 'WindMeasurement'
    wind_measurement_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    wind_measurement_name = Column(String)
    station_id = Column(Integer)
    station_name = Column(String)
    bundesland = Column(String)
    station_height = Column(Integer)
    station_latitude = Column(Float)
    station_longitude = Column(Float)
    source = Column(String)
    datetime_added = Column(DateTime)
    datetime_last_edit = Column(DateTime)  # metadata
    datetime_start = Column(DateTime)  # metadata
    datetime_end = Column(DateTime)  # metadata
    duration = Column(Float)  # metadata
    length = Column(Integer)  # metadata

    wind_data = relationship(WindData, backref='wind_measurement', lazy="joined",
                             cascade="all, delete", order_by=WindData.wind_data_id)

    def __init__(self, wind_measurement_id: int = None, name: str = None, source: str = None):
        """
        Constructor for the WindMeasurement class.
        """
        self.wind_measurement_id = wind_measurement_id
        self.wind_measurement_name = name
        self.station_id = None
        self.station_name = None
        self.bundesland = None
        self.station_height = None
        self.station_latitude = None
        self.station_longitude = None
        self.source = source
        self.datetime_added = func.datetime('now')
        self.datetime_last_edit = func.datetime('now')
        self.datetime_start = None
        self.datetime_end = None
        self.duration = None
        self.length = None

    def __str__(self):
        """
        Returns a string representation of the WindMeasurement instance.
        """
        return f"WindMeasurement(id={self.wind_measurement_id}, name={self.wind_measurement_name})"
