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

    # def get_related_measurement_version_by_filter(self, ) -> Optional[
    #     List[MeasurementVersion]]:
    #
    #     tt_id = self.tree_treatment
    #     logger.info(f"tt_id: '{tt_id}'")
    #
    #     try:
    #         session = self.get_database_manager().session
    #         matching_mv_list: List = (session.query(MeasurementVersion)
    #                                   .filter(MeasurementVersion.measurement == self)
    #                                   .all())
    #
    #         return matching_mv_list
    #     except Exception as e:
    #         logger.error(f"An error occurred while querying the Tree: {e}")
    #         return None
