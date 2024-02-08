from typing import Type, Dict, Tuple, List, Optional, Union, Any

import treemotion
from kj_core.utils.runtime_manager import dec_runtime
from kj_core.classes.core_base_class import CoreBaseClass

from kj_logger import get_logger

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

    def get_child_attr_name(self) -> Optional[str]:
        """
        Get the attribute name of the children based on the class name.
        Should be overridden in specific packages due to the specific hierarchie.

        Returns
        -------
        str or None
            The attribute name if the class name is found, otherwise None.
        """
        mapping = {
            "Project": "series",
            "Series": "measurement",
            "Measurement": "measurement_version",
            "MeasurementVersion": "data_merge"  # e. g. data_tms
        }

        # Store the attribute name corresponding to the class in a variable
        child_attr_name = mapping.get(self.__class__.__name__)

        return child_attr_name

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
        logger.debug(f"Start 'get_measurement_version_by_filter' with filter '{filter_dict}' for instance of '{self}'")
        try:
            matching_mv_list: List = self.method_for_all_of_class(class_name="Measurement",
                                                                  method_name='get_measurement_version_by_filter',
                                                                  filter_dict=filter_dict,
                                                                  method=method)
            if len(matching_mv_list) <= 0:
                raise ValueError(f"No measurement_version found for filter: '{filter_dict}'")

            logger.debug(
                f"{self} finished 'get_measurement_version_by_filter', found '{len(matching_mv_list)}' MeasurementVersions.")

            return matching_mv_list
        except Exception as e:
            logger.error(f"{self}.get_measurement_version_by_filter from, Error: {e}")
            return None
