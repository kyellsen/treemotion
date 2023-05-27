from common_imports.classes_light import *

class TreeSpecies(BaseClass):
    __tablename__ = 'TreeSpecies'
    tree_species_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    name = Column(String)
