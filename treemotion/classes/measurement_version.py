from ..common_imports.imports_classes import *

# from .wind_measurement import WindMeasurement
from .data_tms import DataTMS
from .data_ls3 import DataLS3
from .data_wind import DataWind

logger = get_logger(__name__)


class MeasurementVersion(BaseClass):
    __tablename__ = 'MeasurementVersion'
    measurement_version_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    measurement_version_name = Column(String)
    measurement_id = Column(Integer, ForeignKey('Measurement.measurement_id', onupdate='CASCADE'), nullable=False)
    data_tms_id = Column(Integer)
    data_ls3_id = Column(Integer)
    data_wind_id = Column(Integer)
    datetime_start = Column(DateTime)
    datetime_end = Column(DateTime)
    duration = Column(Float)
    length = Column(Integer)
    tempdrift_method = Column(String)
    peak_index = Column(Integer)
    peak_time = Column(DateTime)
    peak_value = Column(Float)

    data_tms = relationship(DataTMS, backref="measurement_version", lazy="joined", cascade='all, delete-orphan',
                            order_by='DataTMS.data_id')
    data_ls3 = relationship(DataLS3, backref="measurement_version", lazy="joined", cascade='all, delete-orphan',
                            order_by='DataLS3.data_id')
    data_wind = relationship(DataWind, backref="measurement_version", lazy="joined", cascade='all, delete-orphan',
                             order_by='DataWind.data_id')


def __init__(self, measurement_version_id=None, measurement_version_name=None, measurement_id=None, data_tms_id=None,
             data_ls3_id=None, data_wind_id=None, datetime_start=None, datetime_end=None,
             duration=None, length=None, tempdrift_method=None, peak_index=None,
             peak_time=None, peak_value=None):
    super().__init__()
    self.measurement_version_id = measurement_version_id
    self.measurement_version_name = measurement_version_name
    self.measurement_id = measurement_id
    self.data_tms_id = data_tms_id
    self.data_ls3_id = data_ls3_id
    self.data_wind_id = data_wind_id
    self.datetime_start = datetime_start
    self.datetime_end = datetime_end
    self.duration = duration
    self.length = length
    self.tempdrift_method = tempdrift_method
    self.peak_index = peak_index
    self.peak_time = peak_time
    self.peak_value = peak_value
