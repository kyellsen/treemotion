from ..common_imports.imports_classes import *


class MeasurementStatus(BaseClass):
    __tablename__ = 'MeasurementStatus'
    measurement_status_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    measurement_status = Column(String)
