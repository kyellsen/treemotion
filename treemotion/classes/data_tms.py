from kj_core.classes.core_data_class import CoreDataClass

from ..common_imports.imports_classes import *

logger = get_logger(__name__)


class DataTMS(CoreDataClass, BaseClass):
    __tablename__ = 'DataTMS'
    data_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    data_filepath = Column(String, unique=True)
    measurement_version_id = Column(Integer,
                                    ForeignKey('MeasurementVersion.measurement_version_id', onupdate='CASCADE'),
                                    nullable=False)

    def __init__(self, data_id: int = None, data: pd.DataFrame = None, data_filepath: str = None, measurement_version_id: int = None):
        CoreDataClass.__init__(self, data_id=data_id, data=data, data_filepath=data_filepath)

        self.measurement_version_id = measurement_version_id
