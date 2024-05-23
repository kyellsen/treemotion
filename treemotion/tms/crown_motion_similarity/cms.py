from typing import Type, Dict, Tuple, List, Optional, Union, Any
import numpy as np
import pandas as pd
from itertools import count
from sqlalchemy.orm import Session
from sqlalchemy.orm import joinedload
from sqlalchemy import and_

import plotly.graph_objects as go

from kj_core.classes.similarity_metrics import SimilarityMetrics
from kj_core.plotting.multiple_dfs import plot_multiple_lines
from kj_logger import get_logger

from treemotion import Series, Measurement, MeasurementVersion, TreeTreatment, Tree

from ...classes.base_class import BaseClass
from ..df_merge_by_time import calc_optimal_shift

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

        self._shifted_data = None

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
            raise

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

    @property
    def tree_cable_type(self):
        tree_cable = self.tree_treatment.tree_cable
        if tree_cable:
            cable_type = tree_cable.tree_cable_type.tree_cable_type
        else:
            cable_type = "free"
        return cable_type

    @property
    def trunk_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Retrieves trunk data from both sources.

        Returns:
            A tuple of pandas DataFrames for trunk_a and trunk_b.

        Raises:
            ValueError: If trunk_a or trunk_b attributes are not set or None.
        """
        try:
            df_a = self.trunk_a[0].data_merge.data.copy()
            df_b = self.trunk_b[0].data_merge.data.copy()
            return df_a, df_b
        except AttributeError as e:
            logger.error(f"{self} has no attribute trunk_a and trunk_b or it's None. Exception: {e}")

    @property
    def shifted_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Provides shifted trunk data, caching the result for subsequent calls.

        Returns:
            A tuple of DataFrames: df_a, df_b (shifted), and df_b_reference (original df_b).
        """
        # if self._shifted_data:
        #     logger.info("Using existing shifted_data.")
        # else:
        #     logger.warning("Creating new shifted_data with default parameters.")
        #     self._shifted_data = self.get_shifted_data()
        return self.get_shifted_data()

    def _calc_optimal_shift(self, df_a: pd.DataFrame, df_b: pd.DataFrame, column_name: str) -> Tuple[
        float, float, float]:
        """
        Calculates the optimal shift for given DataFrame columns.

        Args:
            df_a: DataFrame of the first trunk.
            df_b: DataFrame of the second trunk.
            column_name: Name of the column to calculate the shift for.

        Returns:
            A tuple containing the optimal shift, correlation without shift, and correlation with optimal shift.
        """
        config = self.get_config()
        sample_rate_hz = config.Data.tms_sample_rate_hz  # Sample rate in Hz, e.g., 20 Hz
        max_shift_sec = config.CrownMotionSimilarity.max_shift_sec  # Maximum shift in seconds

        optimal_shift, correlation_no_shift, correlation_optimal_shift = calc_optimal_shift(
            df_a[column_name],
            df_b[column_name],
            max_shift=sample_rate_hz * max_shift_sec)  # Calculate as an integer, e.g., 20 Hz * 5 sec

        logger.info(f"For Column '{column_name}' before shifting df_b:\n"
                    f"optimal_shift: {optimal_shift}, "
                    f"correlation_no_shift: {correlation_no_shift:.4f}, "
                    f"correlation_optimal_shift: {correlation_optimal_shift:.4f}")

        return optimal_shift, correlation_no_shift, correlation_optimal_shift

    def get_shifted_data(self, calc_shift_by_column: Optional[str] = None, debug: bool = False) -> Tuple[
        pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Shifts data in `df_b` based on the optimal shift calculations for a specified column.

        Args:
            calc_shift_by_column (Optional[str]): Column to use for mean shift calculation. Defaults to configuration setting if None.
            debug (bool): If True, performs a validation check to ensure the optimal shift mean is approximately 0.0.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple containing `df_a`, the shifted `df_b`, and the original `df_b` as reference.
        """
        config = self.get_config()
        col = calc_shift_by_column or config.CrownMotionSimilarity.calc_shift_by_column
        sample_rate_hz = config.Data.tms_sample_rate_hz  # Sample rate in Hz, e.g., 20 Hz

        df_a, df_b = self.trunk_data
        df_b_reference = df_b.copy()

        # Calculate optimal shifts for specified columns
        optimal_shift, _, _ = self._calc_optimal_shift(df_a, df_b, col)

        logger.info(f"Optimal shift: {optimal_shift}")

        df_b.index += pd.DateOffset(seconds=optimal_shift / sample_rate_hz)

        if debug:
            # Re-calculate shifts for validation if debug mode is enabled
            optimal_shift, _, _ = self._calc_optimal_shift(df_a, df_b, col)
            logger.info(f"VALIDATION: Optimal shift: {optimal_shift}")

        self._shifted_data = (df_a, df_b, df_b_reference)
        return self._shifted_data

    def plot_shifted_data(self, columns_to_plot: Optional[List[str]] = None, calc_shift_by_column: Optional[str] = None,
                          debug: bool = False):
        """
        Plots shifted data for a specified column.

        Args:
            columns_to_plot (Optional[List[str]]): Columns to include in the plot. Defaults to configuration setting if None.
            calc_shift_by_column (Optional[str]): Column used for mean shift calculation. This affects the data if it's not already cached.
            debug (bool): If True, includes validation data in the plot for debugging purposes.

        Notes:
            This method assumes that `get_shifted_data` has been called to prepare the data.
        """
        columns_to_plot = columns_to_plot or self.get_config().CrownMotionSimilarity.columns_to_plot
        try:

            df_a, df_b, df_b_reference = self.get_shifted_data(calc_shift_by_column, debug)

            # Prepare data for plotting
            dfs_and_columns = [
                ("Trunk A", df_a, columns_to_plot),
                ("Trunk B (Shifted)", df_b, columns_to_plot),
                ("Trunk B (Reference)", df_b_reference, columns_to_plot)
            ]

            fig = plot_multiple_lines(dfs_and_columns)
            plot_manager = self.get_plot_manager()
            filename = f'series_id_{self.series_id}_cms_id_{self.cms_id}'
            subdir = f"cms/shifted_data/{self.measurement_version_name}"

            plot_manager.save_plot(fig, filename, subdir)
            logger.info(f"{self} plotting successfully")

        except Exception as e:
            logger.warning(f"{self} plotting not possible, e: {e}")

    def _get_data_for_analyse_similarity(self, calc_similarity_by_col: Optional[str] = None,
                                         window_time_around_peak: Optional[pd.Timedelta] = None,
                                         quantile_included: Optional[float] = None) -> Tuple[
        pd.Series, pd.Series, pd.Series]:
        """
        Prepares data for similarity analysis by identifying extreme data points based on the given thresholds.

        Args:
            calc_similarity_by_col (Optional[str]): The column name to analyze for similarity. Defaults to configuration setting if None.
            window_time_around_peak (Optional[pd.Timedelta]): Time window around the peak for rolling maximum calculation. Defaults to configuration setting if None.
            quantile_included (Optional[float]): Quantile threshold to define extreme values. Defaults to configuration setting if None.

        Returns:
            Tuple[pd.Series, pd.Series, pd.Series]: Extreme values in `df_a`, `df_b`, and a combined mask indicating the locations of these extreme values.
        """
        config = self.get_config().CrownMotionSimilarity
        col = calc_similarity_by_col or config.calc_similarity_by_col
        window_time_around_peak = window_time_around_peak or config.window_time_around_peak
        quantile_included = quantile_included or config.quantil_included
        try:
            df_a, df_b, _ = self.shifted_data
            df_b = df_b.reindex(df_a.index, method='nearest')

            a, b = df_a[col].copy(), df_b[col].copy()

            a_roll, b_roll = (s.abs().rolling(window=window_time_around_peak, center=True).max() for s in (a, b))

            threshold_a, threshold_b = (np.quantile(s.dropna(), q=quantile_included) for s in (a_roll, b_roll))

            mask_a, mask_b = (s >= threshold for s, threshold in zip((a_roll, b_roll), (threshold_a, threshold_b)))

            combined_mask = mask_a | mask_b
            data_a, data_b = a[combined_mask], b[combined_mask]

            return data_a, data_b, combined_mask

        except Exception as e:
            raise ValueError(f"{self}, column: {col} failed: {e}")

    def analyse_similarity(self, calc_similarity_by_col: Optional[str] = None,
                           window_time_around_peak: Optional[pd.Timedelta] = None,
                           quantile_included: Optional[float] = None) -> Dict:
        """
        Analyzes similarity between `df_a` and `df_b` based on extreme data points identified within a specified time window and quantile threshold.

        Args:
            calc_similarity_by_col (Optional[str]): The column name to analyze. Defaults to configuration setting if None.
            window_time_around_peak (Optional[pd.Timedelta]): Time window around the peak for rolling max calculation. Defaults to configuration setting if None.
            quantile_included (Optional[float]): Quantile threshold to define extreme values. Defaults to configuration setting if None.

        Returns:
            Dict: A dictionary containing similarity metrics and configuration metadata.
        """
        config = self.get_config().CrownMotionSimilarity
        col = calc_similarity_by_col or config.calc_similarity_by_col
        try:
            data_a, data_b, combined_mask = self._get_data_for_analyse_similarity(calc_similarity_by_col,
                                                                                  window_time_around_peak,
                                                                                  quantile_included)
            cms_metrics = SimilarityMetrics.calc(data_a, data_b)
            metrics_dict = {
                "cms_id": self.cms_id,
                "tree_cable_type": self.tree_cable_type,
                **cms_metrics.to_dict()
            }

            logger.debug(f"Similarity analysis successful for column: {col}.")
            return metrics_dict

        except Exception as e:
            logger.warning(f"{self}, column: {col} failed: {e}")

    def plot_analyse_similarity(self, calc_similarity_by_col: Optional[str] = None,
                                window_time_around_peak: Optional[pd.Timedelta] = None,
                                quantile_included: Optional[float] = None):
        """
        Plots data and the results of similarity analysis for a specified column.

        Args:
            calc_similarity_by_col (Optional[str]): The column name to analyze for similarity. Defaults to configuration setting if None.
            window_time_around_peak (Optional[pd.Timedelta]): Time window around the peak for rolling maximum calculation. Defaults to configuration setting if None.
            quantile_included (Optional[float]): Quantile threshold to define extreme values. Defaults to configuration setting if None.

        Notes:
            This method plots both the original shifted data and the filtered data based on the similarity analysis. It saves or displays the plot using the configured plot manager.
        """
        config = self.get_config().CrownMotionSimilarity
        col = calc_similarity_by_col or config.calc_similarity_by_col
        columns_to_plot = config.columns_to_plot
        try:
            df_a, df_b, df_b_reference = self.get_shifted_data()
            dfas_a, dfas_b, _ = self._get_data_for_analyse_similarity(col,
                                                                      window_time_around_peak,
                                                                      quantile_included)
            # Prepare data for plotting
            dfs_and_columns = [
                ("Trunk A", df_a, columns_to_plot),
                ("Trunk B (Shifted)", df_b, columns_to_plot),
                ("Trunk B (Reference)", df_b_reference, columns_to_plot)
            ]

            fig = plot_multiple_lines(dfs_and_columns)

            # Gefilterte Daten plotten
            fig.add_trace(
                go.Scatter(x=dfas_a.index, y=dfas_a, mode='markers', name=f'Data for Analyse A', marker=dict(size=5)))
            fig.add_trace(
                go.Scatter(x=dfas_b.index, y=dfas_b, mode='markers', name=f'Data for Analyse B', marker=dict(size=5)))

            # Plot speichern oder anzeigen
            plot_manager = self.get_plot_manager()
            filename = f'series_id_{self.series_id}_cms_id_{self.cms_id}_filtered'
            subdir = f"cms/analyse_similarity/{self.measurement_version_name}"

            plot_manager.save_plot(fig, filename, subdir)
            logger.info(f"{self} plotting successfully")

            return fig

        except Exception as e:
            logger.warning(f"Plotting not possible: {e}")
