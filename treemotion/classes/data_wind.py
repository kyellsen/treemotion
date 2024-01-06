from kj_core.classes.core_data_class import CoreDataClass

from ..common_imports.imports_classes import *
from .measurement_version import MeasurementVersion
from .data_wind_station import DataWindStation

logger = get_logger(__name__)


class DataWind(CoreDataClass, BaseClass):
    __tablename__ = 'DataWind'
    data_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    data_filepath = Column(String, unique=True)
    datetime_added = Column(DateTime)
    datetime_last_edit = Column(DateTime)
    measurement_version_id = Column(Integer,
                                    ForeignKey('MeasurementVersion.measurement_version_id', onupdate='CASCADE'),
                                    nullable=False)
    data_wind_station_id = Column(Integer,
                                  ForeignKey('DataWindStation.data_id', onupdate='CASCADE'),
                                  nullable=False)

    data_wind_station = relationship("DataWindStation", backref="data_wind", cascade='all')

    def __init__(self, data_id=None, data=None, data_filepath=None, datetime_added=None, datetime_last_edit=None,
                 measurement_version_id=None, data_wind_station_id=None):
        CoreDataClass.__init__(self, data_id=data_id, data=data, data_filepath=data_filepath,
                               datetime_added=datetime_added, datetime_last_edit=datetime_last_edit)

        self.measurement_version_id = measurement_version_id
        self.data_wind_station_id = data_wind_station_id

    def __str__(self):
        """
        Returns a string representation of the DataWind instance.
        """
        return f"{self.__class__.__name__}_(data_id={self.wind_measurement_id}, name={self.wind_measurement_name})"

    @property
    def datetime_start(self):
        datetime_column_name = self.get_config().DataWind.datetime_column_name
        if self.data is not None and datetime_column_name in self.data.columns:
            return self.data[datetime_column_name].min()
        return None

    @property
    def datetime_end(self):
        datetime_column_name = self.get_config().DataWind.datetime_column_name
        if self.data is not None and datetime_column_name in self.data.columns:
            return self.data[datetime_column_name].max()
        return None

    @property
    def duration(self):
        if self.datetime_start and self.datetime_end:
            return (self.datetime_end - self.datetime_start).total_seconds()
        return None

    @property
    def length(self):
        if self.data is not None:
            return len(self.data)
        return None
