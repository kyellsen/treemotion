from common_imports.classes_light import *

class TreeTreatmentVariant(BaseClass):
    __tablename__ = 'TreeTreatmentVariant'
    tree_treatment_variant_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)
    description = Column(String)
