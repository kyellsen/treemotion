from kj_core import get_logger
from kj_core.classes.core_base_class import CoreBaseClass

import treemotion

logger = get_logger(__name__)


class BaseClass(CoreBaseClass):
    """
    Base class built upon CoreBaseClass, using specific managers from treemotion.
    """
    __abstract__ = True

    def __init__(self):
        # Es wird angenommen, dass treemotion.CONFIG, treemotion.DATA_MANAGER usw. bereits initialisiert wurden
        # Initialisiere CoreBaseClass mit treemotion-Managern
        super().__init__(config=treemotion.CONFIG,
                         data_manager=treemotion.DATA_MANAGER,
                         database_manager=treemotion.DATABASE_MANAGER,
                         plot_manager=treemotion.PLOT_MANAGER)
