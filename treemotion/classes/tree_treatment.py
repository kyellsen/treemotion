from utils.imports_classes import *

from .tree import Tree
from .tree_treatment_variant import TreeTreatmentVariant
from .tree_cable import TreeCable

logger = get_logger(__name__)


class TreeTreatment(BaseClass):
    __tablename__ = 'TreeTreatment'
    tree_treatment_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    tree_id = Column(Integer, ForeignKey('Tree.tree_id', onupdate='CASCADE'))
    datetime_start = Column(DateTime)
    datetime_end = Column(DateTime)
    tree_treatment_variant_id = Column(Integer, ForeignKey('TreeTreatmentVariant.tree_treatment_variant_id',
                                                           onupdate='CASCADE'))
    tree_cable_id = Column(Integer, ForeignKey('TreeCable.tree_cable_id', onupdate='CASCADE'))

    tree = relationship(Tree, backref="tree_treatment", lazy="joined")
    tree_treatment_variant = relationship(TreeTreatmentVariant, backref="tree_treatment", lazy="joined")
    tree_cable = relationship(TreeCable, backref="tree_treatment", lazy="joined")
