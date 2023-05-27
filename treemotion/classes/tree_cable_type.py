from common_imports.classes_light import *

class TreeCableType(BaseClass):
    __tablename__ = 'TreeCableType'
    tree_cable_type_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)
