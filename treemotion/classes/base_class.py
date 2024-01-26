from kj_core import get_logger
from kj_core.utils.runtime_manager import dec_runtime
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

    def get_measurement_version_by_filter(self, filter_dict: Dict[str, Any], method: str = "list_filter") -> (
            Optional)[List[Any]]:
        """
        Executes 'get_measurement_version_by_filter' method in all 'Measurement' class children with given filter.

        :param filter_dict: Dictionary with filter keys and values
        :param method: The method to use for filtering. Possible values are "list_filter" and "db_filter".
               The default value is "list_filter".
               list_filter is way faster, but not searching in database.
        :return: List with measurement_versions of method execution on all 'Measurement' class children,
                 or None if an error occurred.
        """
        logger.info(f"Start 'get_measurement_version_by_filter' with filter '{filter_dict}' for instance of '{self}'")
        try:
            mv: List = self.method_for_all_of_class(class_name="Measurement",
                                                    method_name='get_measurement_version_by_filter',
                                                    filter_dict=filter_dict,
                                                    method=method)
            logger.info(f"Finished 'get_measurement_version_by_filter' for instance of '{self}'")
            logger.info(f"Found '{len(mv)}' MeasurementVersions")

            return mv
        except Exception as e:
            logger.error(
                f"Error in '{self.__class__.__name__}'.get_measurement_version_by_filter from '{self}', Error: {e}")
            return None
