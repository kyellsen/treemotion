from ..common_imports.imports_classes import *
from .tree_species import TreeSpecies
from .measurement_version import MeasurementVersion

logger = get_logger(__name__)


class Tree(BaseClass):
    """
    This class represents a tree in the system.
    """
    __tablename__ = 'Tree'
    tree_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    datetime_survey = Column(DateTime)
    tree_species_id = Column(Integer, ForeignKey('TreeSpecies.tree_species_id', onupdate='CASCADE'))
    circumference = Column(Integer)
    height = Column(Integer)
    fork_height = Column(Integer)

    tree_species = relationship(TreeSpecies, backref="tree", lazy="joined")

    def __str__(self):
        """
        Returns a string representation of the Tree object.
        """
        return f"Tree(id={self.tree_id}, height={self.height})"
