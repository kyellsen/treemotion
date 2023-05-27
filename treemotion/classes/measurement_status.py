from common_imports.classes_heavy import *


class MeasurementStatus(BaseClass):
    __tablename__ = 'MeasurementStatus'
    measurement_status_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    status_name = Column(String)
