from ..common_imports.imports_classes import *

logger = get_logger(__name__)


class TreeSpecies(BaseClass):
    __tablename__ = 'TreeSpecies'
    tree_species_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    tree_species = Column(String)
