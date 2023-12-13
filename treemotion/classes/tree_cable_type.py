from ..common_imports.imports_classes import *

logger = get_logger(__name__)


class TreeCableType(BaseClass):
    __tablename__ = 'TreeCableType'
    tree_cable_type_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    tree_cable_type = Column(String)
