from utils.imports_classes import *

logger = get_logger(__name__)


class TreeTreatmentVariant(BaseClass):
    __tablename__ = 'TreeTreatmentVariant'
    tree_treatment_variant_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)
    description = Column(String)
