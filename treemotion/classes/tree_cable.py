from ..common_imports.imports_classes import *
from .tree_cable_type import TreeCableType

logger = get_logger(__name__)


class TreeCable(BaseClass):
    __tablename__ = 'TreeCable'
    tree_cable_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    tree_cable_type_id = Column(Integer, ForeignKey('TreeCableType.tree_cable_type_id',
                                                    onupdate='CASCADE'))
    height = Column(Integer)
    length = Column(Integer)
    trunk_a_circumference = Column(Integer)
    trunk_b_circumference = Column(Integer)
    slack = Column(Integer)

    tree_cable_type = relationship(TreeCableType, backref="tree_cable", lazy="joined")
