from typing import Type, Dict, Tuple, List, Optional, Union, Any
import numpy as np
import pandas as pd
from itertools import count
from sqlalchemy.orm import Session
from sqlalchemy.orm import joinedload
from sqlalchemy import and_

from scipy.stats import pearsonr
from sklearn.metrics import mean_squared_error, mean_absolute_error

from kj_core.plotting.multiple_dfs import plot_multiple_dfs
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

        self._shifted_trunk_data = None

    def __str__(self):
        return (f"{self.__class__.__name__}(id={self.cms_id}, series_id={self.series_id}, "
                f"mv_name={self.measurement_version_name}, tree_treatment_id={self.tree_treatment.tree_treatment_id})")

    @property
    def tree_cable_typ(self):
        tree_cable = self.tree_treatment.tree_cable
        if tree_cable:
            cable_typ = tree_cable.tree_cable_type.tree_cable_type
        else:
            cable_typ = "free"
        return cable_typ

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
    def shifted_trunk_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Provides shifted trunk data, caching the result for subsequent calls.

        Returns:
            A tuple of DataFrames: df_a, df_b (shifted), and df_b_reference (original df_b).
        """
        if self._shifted_trunk_data:
            logger.info("Using existing shifted_trunk_data.")
        else:
            logger.warning("Creating new shifted_trunk_data with default parameters.")
            self._shifted_trunk_data = self.get_shifted_trunk_data()
        return self._shifted_trunk_data

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
                    f"Optimal get_shifted_trunk_data: {optimal_shift}, "
                    f"Correlation without get_shifted_trunk_data: {correlation_no_shift:.4f}, "
                    f"Correlation at optimal get_shifted_trunk_data: {correlation_optimal_shift:.4f}")

        return optimal_shift, correlation_no_shift, correlation_optimal_shift

    def get_shifted_trunk_data(self, use_for_mean_columns: Optional[List[str]] = None, debug: bool = False) -> Tuple[
        pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Shifts trunk_b data based on optimal shift calculations for specified columns.

        Args:
            use_for_mean_columns: Columns to use for mean shift calculation. Defaults to config setting.
            debug: If True, perform validation check to ensure optimal shift mean is approximately 0.0.

        Returns:
            A tuple of DataFrames: df_a, df_b (shifted), and df_b_reference (original df_b).
        """
        config = self.get_config()
        columns = use_for_mean_columns or config.CrownMotionSimilarity.use_for_mean_columns
        sample_rate_hz = config.Data.tms_sample_rate_hz  # Sample rate in Hz, e.g., 20 Hz

        df_a, df_b = self.trunk_data
        df_b_reference = df_b.copy()

        # Calculate optimal shifts for specified columns
        optimal_shift_list = [self._calc_optimal_shift(df_a, df_b, column)[0] for column in columns]

        # Calculate mean of optimal shifts and apply to df_b
        optimal_shift_mean = np.mean(optimal_shift_list)
        logger.info(f"Mean of optimal shifts: {optimal_shift_mean}, List of shifts: {optimal_shift_list}")

        df_b.index += pd.DateOffset(seconds=optimal_shift_mean / sample_rate_hz)

        if debug:
            # Re-calculate shifts for validation if debug mode is enabled
            validation_shift_list = [self._calc_optimal_shift(df_a, df_b, column)[0] for column in columns]
            validation_shift_mean = np.mean(validation_shift_list)
            logger.debug(
                f"VALIDATION: Mean of optimal shifts: {validation_shift_mean}, expected to be close to 0.0. List of shifts: {validation_shift_list}")

        self._shifted_trunk_data = (df_a, df_b, df_b_reference)
        return self._shifted_trunk_data

    def plot_shifted_trunk_data(self, columns_to_plot: Optional[List[str]] = None,
                                use_for_mean_columns: Optional[List[str]] = None, debug: bool = False):
        """
        Plots shifted trunk data for specified columns.

        Args:
            columns_to_plot: Columns to include in the plot. Defaults to config setting.
            use_for_mean_columns: Columns used for mean shift calculation. Affects the data if not already cached.
            debug: If True, includes validation data in the plot.
        """
        config = self.get_config()
        columns_to_plot = columns_to_plot or config.CrownMotionSimilarity.columns_to_plot
        try:

            df_a, df_b, df_b_reference = self.get_shifted_trunk_data(use_for_mean_columns, debug)

            # Prepare data for plotting
            dfs_and_columns = [
                ("Trunk A", df_a, columns_to_plot),
                ("Trunk B (Shifted)", df_b, columns_to_plot),
                ("Trunk B (Reference)", df_b_reference, columns_to_plot)
            ]

            fig = plot_multiple_dfs(dfs_and_columns)
            plot_manager = self.get_plot_manager()
            filename = f'series_id_{self.series_id}_cms_id_{self.cms_id}'
            subdir = f"cms/compare_trunks/{self.measurement_version_name}"

            plot_manager.save_plot(fig, filename, subdir)

        except Exception as e:
            logger.warning(f"Plotting not possible: e: {e}")

    def get_data_for_analysis(self, window_time: pd.Timedelta = "6s", q: float = 0.90):
        df_a, df_b, _ = self.shifted_trunk_data
        df_b = df_b.reindex(df_a.index, method='nearest')

        col = 'Absolute-Inclination - drift compensated'

        a: pd.Series = df_a[col].copy()
        b: pd.Series = df_b[col].copy()

        # Anwenden der Rolling-Maximum-Funktion mit einem Fenster von 10 Sekunden
        a_roll = a.abs().rolling(window=window_time, center=True).max()
        b_roll = b.abs().rolling(window=window_time, center=True).max()

        # Berechne den Schwellenwert für die oberen 5%
        threshold_a = np.quantile(a_roll.dropna(), q=q)
        threshold_b = np.quantile(b_roll.dropna(), q=q)

        # Erstelle eine Maske für die Extremwerte
        mask_a = a_roll >= threshold_a
        mask_b = b_roll >= threshold_b

        # Kombiniere die Masken, um Bereiche zu markieren, die in mindestens einer der beiden Serien Extremwerte enthalten
        combined_mask = mask_a | mask_b

        # Wende die kombinierte Maske auf die ursprünglichen Serien an
        filtered_a = a[combined_mask]
        filtered_b = b[combined_mask]

        return filtered_a, filtered_b, combined_mask

    def analyse_similarity(self):

        filtered_a, filtered_b, combined_mask = self.get_data_for_analysis()
        correlation, p_value = pearsonr(filtered_a, filtered_b)
        logger.info(f"Pearson-Korrelationskoeffizient: {correlation:.4f}, p_value: {p_value:.4f}")

        rmse = np.sqrt(mean_squared_error(filtered_a, filtered_b))
        logger.info(f"Root Mean Square Error (RMSE): {rmse:.4f}")

        mae = mean_absolute_error(filtered_a, filtered_b)
        logger.info(f"Mean Absolute Error (MAE): {mae:.4f}")

        tree_cable_typ = self.tree_cable_typ
        return correlation, p_value, rmse, mae, tree_cable_typ

    def plot_filtered_trunk_data(self, columns_to_plot: Optional[List[str]] = None,
                                 use_for_mean_columns: Optional[List[str]] = None, debug: bool = False):
        import plotly.graph_objects as go

        config = self.get_config()
        columns_to_plot = columns_to_plot or config.CrownMotionSimilarity.columns_to_plot
        try:
            df_a, df_b, df_b_reference = self.get_shifted_trunk_data(use_for_mean_columns, debug)

            # Aufruf von analyse_similarity, um die gefilterten Daten zu erhalten
            filtered_a, filtered_b, combined_mask = self.get_data_for_analysis()

            # Bereite Daten für das Plotten vor, einschließlich der gefilterten Bereiche
            dfs_and_columns = [
                ("Trunk A", df_a, columns_to_plot),
                ("Trunk B (Shifted)", df_b, columns_to_plot),
                ("Trunk B (Reference)", df_b_reference, columns_to_plot),
                ("Filtered A", filtered_a, columns_to_plot),
                ("Filtered B", filtered_b, columns_to_plot)
            ]

            fig = go.Figure()

            for name, df, columns in dfs_and_columns[:-2]:  # Normale Daten plotten
                for column in columns:
                    if column in df.columns:
                        fig.add_trace(go.Scatter(x=df.index, y=df[column], mode='lines', name=f'{name}: {column}'))

            for name, series in [("Filtered A", filtered_a), ("Filtered B", filtered_b)]:  # Gefilterte Daten plotten
                fig.add_trace(go.Scatter(x=series.index, y=series, mode='markers', name=f'{name}', marker=dict(size=5)))

            fig.update_layout(
                title='Compare DFs with Filtered Data',
                xaxis_title='DateTime',
                yaxis_title='Value',
                legend_title='Variable'
            )

            plot_manager = self.get_plot_manager()
            filename = f'series_id_{self.series_id}_cms_id_{self.cms_id}_filtered'
            subdir = f"cms/compare_trunks_filtered/{self.measurement_version_name}"

            plot_manager.save_plot(fig, filename, subdir)

        except Exception as e:
            logger.warning(f"Plotting not possible: e: {e}")
