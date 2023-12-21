from kj_core import get_logger
from kj_core.classes.core_base_class import CoreBaseClass
from typing import Type, Dict, Tuple, List, Optional, Union, Any

import treemotion

logger = get_logger(__name__)


class BaseClass(CoreBaseClass):
    __abstract__ = True

    def __init__(self):
        super().__init__()

    @classmethod
    def get_config(cls):
        return treemotion.CONFIG

    @classmethod
    def get_data_manager(cls):
        return treemotion.DATA_MANAGER

    @classmethod
    def get_database_manager(cls):
        return treemotion.DATABASE_MANAGER

    @classmethod
    def get_plot_manager(cls):
        return treemotion.PLOT_MANAGER
