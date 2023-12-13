from kj_core.classes.core_data_class import CoreDataClass

from ..common_imports.imports_classes import *

logger = get_logger(__name__)


class DataWind(CoreDataClass, BaseClass):
    __tablename__ = 'DataWind'
    data_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    data_filepath = Column(String, unique=True)
    measurement_version_id = Column(Integer,
                                    ForeignKey('MeasurementVersion.measurement_version_id', onupdate='CASCADE'),
                                    nullable=False)

    def __init__(self, data_id=None, data=None, data_filepath=None, measurement_version_id=None):
        CoreDataClass.__init__(self, data_id=data_id, data=data, data_filepath=data_filepath)

        self.measurement_version_id = measurement_version_id
