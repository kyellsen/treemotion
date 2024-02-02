from ..common_imports.imports_classes import *
from .tree import Tree
from .tree_treatment_type import TreeTreatmentType
from .tree_cable import TreeCable

from .measurement_version import MeasurementVersion

logger = get_logger(__name__)


class TreeTreatment(BaseClass):
    __tablename__ = 'TreeTreatment'
    tree_treatment_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    tree_id = Column(Integer, ForeignKey('Tree.tree_id', onupdate='CASCADE'))
    datetime_start = Column(DateTime)
    datetime_end = Column(DateTime)
    tree_treatment_type_id = Column(Integer, ForeignKey('TreeTreatmentType.tree_treatment_type_id',
                                                        onupdate='CASCADE'))
    tree_cable_id = Column(Integer, ForeignKey('TreeCable.tree_cable_id', onupdate='CASCADE'))

    tree = relationship(Tree, backref="tree_treatment", lazy="joined")
    tree_treatment_variant = relationship(TreeTreatmentType, backref="tree_treatment", lazy="joined")
    tree_cable = relationship(TreeCable, backref="tree_treatment", lazy="joined")
