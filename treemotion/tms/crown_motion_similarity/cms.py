from typing import Type, Dict, Tuple, List, Optional, Union, Any
import numpy as np
import pandas as pd
from itertools import count
from sqlalchemy.orm import Session
from sqlalchemy.orm import joinedload
from sqlalchemy import and_

from ...classes.base_class import BaseClass
from treemotion import Series, Measurement, MeasurementVersion, TreeTreatment, Tree

from kj_core.plotting.multiple_dfs import plot_multiple_dfs
from kj_logger import get_logger

logger = get_logger(__name__)


class CrownMotionSimilarity(BaseClass):
    __abstract__ = True
    _id_counter = count(1)

    def __init__(self, series_id: int, measurement_version_name: str, tree_treatment: TreeTreatment, **kwargs):
        super().__init__()
        # Weist die nächste ID aus dem Zähler zu

        self.cms_id = next(self._id_counter)
        self.series_id = series_id
        self.measurement_version_name = measurement_version_name
        self.tree_treatment = tree_treatment
        # Dynamische Zuweisung der Attribute aus kwargs
        for key in ['base', 'trunk', 'trunk_a', 'trunk_b', 'trunk_c', 'trunk_cable']:
            setattr(self, key, kwargs.get(key))

    def __str__(self):
        return (f"{self.__class__.__name__}(id={self.cms_id}, series_id={self.series_id}, "
                f"mv_name={self.measurement_version_name}, tree_treatment_id={self.tree_treatment.tree_treatment_id})")

    @classmethod
    def load_and_process_data(cls, series_id: int, measurement_version_name: str) -> List[Dict[str, Any]]:
        """
        Loads and processes data for further use.

        Parameters:
            series_id (int): The ID of the series to query.
            measurement_version_name (str): The name of the measurement version.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each containing grouped measurement versions by tree treatment and sensor location.
        """
        try:
            session = cls.get_database_manager().session
            measurement_versions = cls.query_measurement_versions(series_id, measurement_version_name, session)
            grouped = cls.group_by_tree_treatment(measurement_versions)
            return cls.categorize_by_sensor_location(grouped)
        except Exception as e:
            logger.error(f"Error loading and processing data: {e}")
            return []

    @staticmethod
    def query_measurement_versions(series_id: int, measurement_version_name: str, session: Session) -> List[
        MeasurementVersion]:
        """
        Queries MeasurementVersion objects from the database.

        Parameters:
            series_id (int): The series ID to filter by.
            measurement_version_name (str): The measurement version name to filter by.
            session (Session): The database session.

        Returns:
            List[MeasurementVersion]: A list of MeasurementVersion objects.
        """
        try:
            return session.query(MeasurementVersion) \
                .join(MeasurementVersion.measurement) \
                .join(Measurement.tree_treatment) \
                .filter(
                and_(
                    Measurement.series_id == series_id,
                    MeasurementVersion.measurement_version_name == measurement_version_name
                )
            ) \
                .options(joinedload(MeasurementVersion.measurement).joinedload(Measurement.tree_treatment)) \
                .order_by(TreeTreatment.tree_treatment_id, MeasurementVersion.measurement_version_id) \
                .all()
        except Exception as e:
            logger.error(f"Error querying measurement versions: {e}")
            return []

    @staticmethod
    def group_by_tree_treatment(measurement_versions: List[MeasurementVersion]) -> List[Dict[str, Any]]:
        """
        Groups MeasurementVersion objects by TreeTreatment.

        Parameters:
            measurement_versions (List[MeasurementVersion]): The measurement versions to group.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with grouped MeasurementVersion objects.
        """
        grouped = {}
        for mv in measurement_versions:
            tt_id = mv.measurement.tree_treatment.tree_treatment_id
            if tt_id not in grouped:
                grouped[tt_id] = {'tree_treatment': mv.measurement.tree_treatment, 'measurement_versions': []}
            grouped[tt_id]['measurement_versions'].append(mv)
        return list(grouped.values())

    @staticmethod
    def categorize_by_sensor_location(grouped_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Categorizes MeasurementVersion objects by their sensor location within the grouped data.

        Parameters:
            grouped_data (List[Dict[str, Any]]): The grouped data by tree treatment.

        Returns:
            List[Dict[str, Any]]: The updated grouped data with additional keys for each sensor location.
        """
        for group in grouped_data:
            sensor_locations = {}
            for mv in group['measurement_versions']:
                sensor_loc = mv.measurement.sensor_location.sensor_location
                sensor_locations.setdefault(sensor_loc, []).append(mv)
            group.update(sensor_locations)
        return grouped_data

    @classmethod
    def create_all_cms(cls, series_id: int, measurement_version_name: str) -> List[Any]:
        """
        Creates CrownMotionSimilarity objects based on loaded data.

        Parameters:
            series_id (int): The unique identifier of the series.
            measurement_version_name (str): The name of the measurement version.

        Returns:
            List[CrownMotionSimilarity]: A list of CrownMotionSimilarity objects initialized with the loaded data.

        Note:
            This method assumes that the `load_and_process_data` method returns a list of dictionaries,
            each containing data used to initialize a CrownMotionSimilarity object. The `tree_treatment`
            key is expected to be present in each dictionary.
        """
        cms_objects = []
        try:
            for entry in cls.load_and_process_data(series_id, measurement_version_name):
                if 'tree_treatment' in entry:
                    tree_treatment = entry.pop('tree_treatment')
                    # Initialize CrownMotionSimilarity with the specific and additional attributes
                    obj = CrownMotionSimilarity(series_id=series_id, measurement_version_name=measurement_version_name,
                                                tree_treatment=tree_treatment, **entry)
                    cms_objects.append(obj)
                    logger.info(f"Created {obj}")
                else:
                    raise ValueError("Missing 'tree_treatment' in the loaded data entry.")

        except Exception as e:
            logger.error(f"Failed to create CrownMotionSimilarity objects: {e}", exc_info=True)
        return cms_objects

    def plot_cms(self, columns: List[str] = None):
        if hasattr(self, "trunk_a") and hasattr(self, "trunk_b"):
            df_a = self.trunk_a[0].data_merge.data
            df_b = self.trunk_b[0].data_merge.data

            if columns is None:
                columns = ['East-West-Inclination - drift compensated',
                           'North-South-Inclination - drift compensated',
                           'Absolute-Inclination - drift compensated'
                           ]

            dfs_and_columns: [List[Tuple[str, pd.DataFrame, List[str]]]] = [
                ("trunk_a", df_a, columns),
                ("trunk_b", df_b, columns)
            ]

            fig = plot_multiple_dfs(dfs_and_columns)
            plot_manager = self.get_plot_manager()
            filename = f'series_id_{self.series_id}_cms_id_{self.cms_id}'
            subdir = f"cms/compare_trunks/{self.measurement_version_name}"

            plot_manager.save_plot(fig, filename, subdir)
        else:
            logger.warning(f"Self has not trunk_a and trunk_b")

    def plot_cms_shift(self, columns: List[str] = None):
        if self.trunk_a and self.trunk_b:
            df_a = self.trunk_a[0].data_merge.data.copy()
            df_b = self.trunk_b[0].data_merge.data.copy()
            df_b_2 = self.trunk_b[0].data_merge.data.copy()

            # Merge_asof ausführen, um die DataFrames auf Basis des nächsten Datums zu mergen
            df_merged = pd.merge_asof(df_a, df_b, left_index=True, right_index=True, direction='nearest',
                                      suffixes=('_a', '_b'))

            from ..df_merge_by_time import calc_optimal_shift

            max_correlation, optimal_shift = calc_optimal_shift(df_merged,
                                               'Absolute-Inclination - drift compensated_a',
                                               'Absolute-Inclination - drift compensated_b',
                                               max_shift=20*20, step=1)

            logger.info(f"EINS: max_correlation {max_correlation}, optimal_shift {optimal_shift}")

            sample_rate_hz = 20  # Hz

            # Timedelta für die Verschiebung erstellen
            shift_timedelta = pd.Timedelta(seconds=optimal_shift/sample_rate_hz)

            # DataFrame-Index verschieben
            df_b.index += shift_timedelta

            # Merge_asof ausführen, um die DataFrames auf Basis des nächsten Datums zu mergen
            df_merged = pd.merge_asof(df_a, df_b, left_index=True, right_index=True, direction='nearest',
                                      suffixes=('_a', '_b'))


            max_correlation, optimal_shift = calc_optimal_shift(df_merged,
                                               'Absolute-Inclination - drift compensated_a',
                                               'Absolute-Inclination - drift compensated_b',
                                               max_shift=20*20, step=1)

            logger.info(f"ZWEI: max_correlation {max_correlation}, optimal_shift {optimal_shift}")

            if columns is None:
                columns = ['East-West-Inclination - drift compensated',
                           'North-South-Inclination - drift compensated',
                           'Absolute-Inclination - drift compensated'
                           ]

            dfs_and_columns: [List[Tuple[str, pd.DataFrame, List[str]]]] = [
                ("trunk_a", df_a, columns),
                ("trunk_b", df_b, columns),
                ("trunk_b_2", df_b_2, columns)
            ]

            fig = plot_multiple_dfs(dfs_and_columns)
            plot_manager = self.get_plot_manager()
            filename = f'series_id_{self.series_id}_cms_id_{self.cms_id}'
            subdir = f"cms/compare_trunks/{self.measurement_version_name}"

            plot_manager.save_plot(fig, filename, subdir)
        else:
            logger.warning(f"Self has not trunk_a and trunk_b")
