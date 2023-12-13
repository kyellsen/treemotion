from ..common_imports.imports_classes import *

logger = get_logger(__name__)


class TreeTreatmentType(BaseClass):
    __tablename__ = 'TreeTreatmentType'
    tree_treatment_type_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    tree_treatment_type = Column(String)
    description = Column(String)
