import numpy as np
import pandas as pd
from typing import Type, Dict, Tuple, List, Optional, Union, Any, Callable

from kj_core.df_utils.time_cut import validate_time_format, time_cut_by_datetime_index
from kj_core.plotting.multiple_dfs import plot_multiple_lines, plot_multiple_scatter, plot_multiple_polar

from ..common_imports.imports_classes import *
from ..classes import BaseClass
from kj_logger import get_logger

from ..tms.find_peaks import find_max_peak, find_n_peaks
from ..tms.tempdrift import temp_drift_comp_lin_reg, temp_drift_comp_lin_reg_2, temp_drift_comp_mov_avg, \
    temp_drift_comp_emd
from ..tms.tempdrift import fft_freq_filter, butter_lowpass_filter
from ..tms.rotate import rotate_pca
from ..tms.inclination import calc_abs_inclino, calc_inclination_direction

logger = get_logger(__name__)


class BaseClassDataTMS(BaseClass):
    __abstract__ = True

    tempdrift_methods: Dict[str, Optional[Callable]] = {
        "original": None,
        "linear": temp_drift_comp_lin_reg,
        "linear_2": temp_drift_comp_lin_reg_2,
        "moving_average": temp_drift_comp_mov_avg,
        "emd": temp_drift_comp_emd,
    }

    filter_methods: Dict[str, Optional[Callable]] = {
        "no_filter": None,
        "butter_lowpass": butter_lowpass_filter,
        "fft": fft_freq_filter,
    }

    rotation_methods: Dict[str, Optional[Callable]] = {
        "no_rotation": None,
        "rotate_pca": rotate_pca
    }

    def __init__(self, data: pd.DataFrame = None, measurement_version_id: int = None, tempdrift_method: str = None,
                 filter_method: str = None, rotation_method: str = None):
        super().__init__()
        self.data = data
        self.measurement_version_id = measurement_version_id
        self.tempdrift_method = tempdrift_method
        self.filter_method = filter_method
        self.rotation_method = rotation_method

    def correct_tms_data(self, method: str = "linear", freq_filter: str = "butter_lowpass",
                         rotation: str = "no_rotation", inplace: bool = False,
                         auto_commit: bool = False, **kwargs: Any) -> pd.DataFrame:
        """
        Corrects temperature distortion of inclination data using a specified method and optionally updates
        the instance's data attribute in place.

        Parameters:
            method (str): Method for temperature drift compensation. Defaults to "linear".
            freq_filter (str): Filtering method. Defaults to "butter_lowpass".
            rotation (str): Rotation method. Defaults to "no_rotation".
            inplace (bool): If True, updates the instance's data attribute. Defaults to False.
            auto_commit (bool): If True, auto-commits changes to the database. Defaults to False.
            **kwargs: Additional keyword arguments for the temperature drift compensation function.

        Returns:
            pd.DataFrame: Corrected data.

        Raises:
            ValueError: If an unauthorized method is passed.
        """
        if method not in self.tempdrift_methods:
            error_msg = f"Unauthorized method {method} for temperature drift compensation."
            logger.error(error_msg)
            raise ValueError(error_msg)

        if freq_filter not in self.filter_methods:
            error_msg = f"Unauthorized method {freq_filter} for filtering."
            logger.error(error_msg)
            raise ValueError(error_msg)

        if rotation not in self.rotation_methods:
            error_msg = f"Unauthorized method {rotation} for data rotation."
            logger.error(error_msg)
            raise ValueError(error_msg)

        temp_drift_comp_func = self.tempdrift_methods[method]
        filter_func = self.filter_methods[freq_filter]
        rotation_func = self.rotation_methods[rotation]

        if method in ["linear", "linear_2"]:
            kwargs["temperature"] = self.data["Temperature"]

        data_copy = self.data.copy()
        try:
            for axis in ['East-West-Inclination', 'North-South-Inclination']:
                inclino_series = data_copy[axis]
                compensated = temp_drift_comp_func(inclino_series, **kwargs)
                if filter_func and method in ["linear", "linear_2", "moving_average"]:
                    compensated = filter_func(inclino=compensated)
                data_copy[f"{axis} - drift compensated"] = compensated

            if rotation_func:
                data_copy = self.rotate(data_copy, rotation_func)

            data_copy = self.calc_inclino_abs_and_dir(data_copy)

            if inplace:
                self.data = data_copy
                self.tempdrift_method = method
                self.filter_method = freq_filter

            if auto_commit:
                self.get_database_manager().commit()

            logger.info(
                f"Temperature drift compensation successfully performed. "
                f"Method: {method}, freq_filter: {freq_filter}, rotation: {rotation}, inplace: {inplace}")
            return data_copy
        except Exception as e:
            logger.error(f"Error in performing temperature drift compensation: {e}")
            raise

    @staticmethod
    def rotate(data: pd.DataFrame, rotation_func: Callable) -> pd.DataFrame:
        x_axs = 'East-West-Inclination - drift compensated'
        y_axs = 'North-South-Inclination - drift compensated'
        try:

            data[x_axs], data[y_axs], angle_rad, angle_deg = rotation_func(data[x_axs], data[y_axs])
            logger.info(f"Rotate - angle_rad: {angle_rad:.4f}, angle_deg {angle_deg:.4f}")
            return data
        except Exception as e:
            logger.error(f"Rotation failed, e: {e}")

    @staticmethod
    def calc_inclino_abs_and_dir(data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the total inclination and direction based on compensated data.

        Parameters:
        - data: DataFrame containing East-West and North-South inclination data.

        Returns:
        - DataFrame: The input DataFrame with added columns for absolute inclination
                     and inclination direction, both drift compensated.
        """
        data['Absolute-Inclination - drift compensated'] = calc_abs_inclino(
            data['East-West-Inclination - drift compensated'],
            data['North-South-Inclination - drift compensated'])

        data['Inclination direction of the tree - drift compensated'] = calc_inclination_direction(
            data['East-West-Inclination - drift compensated'],
            data['North-South-Inclination - drift compensated'])
        return data

    def compare_correct_tms_data_methods(self) -> Dict[str, pd.DataFrame]:
        """
        Compares different methods for correcting TMS data for temperature drift, filtering, and rotation.

        Returns:
            A dictionary with keys as method descriptions and values as the compensated data DataFrames.
        """
        results = {"original": self.data.copy()}

        tempdrift_methods = ["linear"]  # Placeholder for additional methods: "moving_average", "emd", "linear_2"
        filter_methods = ["butter_lowpass"]  # Placeholder for additional methods: "no_filter", "fft"
        rotation_methods = ["no_rotation", "rotate_pca"]  # Placeholder for additional method: "rotate_pca"

        # Generating combinations of methods, filters, and rotations
        combinations = [(method, filter_, rotation) for method in tempdrift_methods
                        for filter_ in (filter_methods if method not in ["emd"] else ["no_filter"])
                        for rotation in rotation_methods]

        # Iterating over each combination to correct TMS data and store in results
        for method, freq_filter, rotation in combinations:
            compensated_data = self.correct_tms_data(method=method, freq_filter=freq_filter, rotation=rotation,
                                                     inplace=False)
            key = f"{method}_{freq_filter}_{rotation}" if method != "emd" else f"{method}_{rotation}"  # Special handling for "emd" method
            results[key] = compensated_data

        return results

    def plot_compare_correct_tms_data_methods(self, start_time=None, end_time=None):
        results = self.compare_correct_tms_data_methods()

        original_columns = ['East-West-Inclination',
                            'North-South-Inclination',
                            'Absolute-Inclination',
                            #'Temperature',
                            ]

        compensated_columns = ['East-West-Inclination - drift compensated',
                               'North-South-Inclination - drift compensated',
                               'Absolute-Inclination - drift compensated'
                               ]

        dfs_and_columns: [List[Tuple[str, pd.DataFrame, List[str]]]] = [
            ("Original", results["original"], original_columns)
        ]

        for method, df in results.items():
            if method != "original":
                if (start_time is not None) and (end_time is not None):
                    df = df.loc[start_time:end_time].copy()
                dfs_and_columns.append((method, df, compensated_columns))

        fig = plot_multiple_lines(dfs_and_columns)

        plot_manager = self.get_plot_manager()
        filename = f'mv_id_{self.measurement_version_id}'
        subdir = f"correct_tms/compare_methods_lines/{self.__class__.__name__}"
        plot_manager.save_plot(fig, filename, subdir)

        polar_cols = ['East-West-Inclination - drift compensated', 'North-South-Inclination - drift compensated']

        fig_polar = plot_multiple_scatter(dfs_and_columns, polar_cols)
        filename = f'mv_id_{self.measurement_version_id}'
        subdir = f"correct_tms/compare_methods_cartesian/{self.__class__.__name__}"
        plot_manager.save_plot(fig_polar, filename, subdir)

        fig_polar = plot_multiple_polar(dfs_and_columns, polar_cols)
        filename = f'mv_id_{self.measurement_version_id}'
        subdir = f"correct_tms/compare_methods_polar/{self.__class__.__name__}"
        plot_manager.save_plot(fig_polar, filename, subdir)

    def random_sample(self, n: int, inplace: bool = False, auto_commit: bool = False) -> pd.DataFrame:
        """
        Selects a random sample of data while preserving the original order of the selected data points.

        Parameters
        ----------
        n : int
            Number of data points to select.
        inplace : bool, default True
            If True, updates the object's data in place. Otherwise, returns a new DataFrame.
        auto_commit : bool, default False
            If True, automatically commits changes to the database (if applicable).

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the randomly selected data points.

        Raises
        ------
        ValueError
            If an error occurs during sample selection.
        """

        logger.info(f"Selecting a random sample of {n} data points.")

        if n > len(self.data):
            logger.warning(
                f"The requested sample size ({n}) exceeds the data length ({len(self.data)}). Using all data.")
            n = len(self.data)

        try:
            # Sample without replacement, sort indices to preserve order
            sampled_indices = self.data.sample(n, random_state=None).index
            sampled_indices_sorted = sorted(sampled_indices)

            # Use loc to maintain the datetime index
            sampled_data = self.data.loc[sampled_indices_sorted]

            if inplace:
                self.data = sampled_data

            if auto_commit:
                self.get_database_manager().commit()

            logger.debug("Random sample selected successfully.")
            return sampled_data
        except Exception as e:
            logger.error(f"Failed to select a random sample: {e}")

    def cut_by_time(self, start_time: str, end_time: str, inplace: bool = False, auto_commit: bool = False) -> Union[
        pd.DataFrame, None]:
        """
        Limits the data to a specific time range and optionally updates the instance data in-place.

        Parameters
        ----------
        start_time : str
            The start time of the range, in a format compatible with `validate_time_format`.
        end_time : str
            The end time of the range, in a format compatible with `validate_time_format`.
        inplace : bool, optional
            If True, updates the instance's data in-place. Defaults to False.
        auto_commit : bool, optional
            If True, automatically commits changes to the database. Defaults to False.

        Returns
        -------
        Version
            Self-reference for method chaining.
        """
        # Validate time formats
        validated_start_time = validate_time_format(start_time)
        validated_end_time = validate_time_format(end_time)

        if isinstance(validated_start_time, ValueError) or isinstance(validated_end_time, ValueError):
            logger.error("Invalid time format provided.")
            return

        # Attempt to limit data within the specified time range
        try:
            data = self.data.copy()
            data = time_cut_by_datetime_index(data, start_time=start_time, end_time=end_time)

            if inplace:
                self.data = data

            if auto_commit:
                self.get_database_manager().commit()

            logger.info(
                f"Successfully limited the data of '{self}' between '{start_time}' and '{end_time}', inplace: '{inplace}', auto_commit: '{auto_commit}'.")

            return data
        except Exception as e:
            logger.error(f"Error limiting the data of '{self}': {e}")
            return

    @property
    def peak_max(self) -> Optional[Tuple]:

        column: str = self.get_config().Data.main_tms_value
        try:
            index, value = find_max_peak(self.data[column])
        except Exception as e:
            logger.warning(f"No peak found for {self}, error: {e}")
            return None

        # For debugging
        if False:
            logger.debug(f"Peak in {self}: index '{index}', value '{value}'")
        return index, value

    @property
    def peak_n(self) -> pd.Series:
        config = self.get_config().Data

        column = config.main_tms_value
        n_peaks: int = config.peak_n_count
        sample_rate: float = config.tms_sample_rate_hz
        min_time_diff: float = config.peak_n_min_time_diff
        prominence: int = config.peak_n_prominence

        try:
            peaks: pd.Series = find_n_peaks(self.data[column], n_peaks, sample_rate, min_time_diff, prominence)

        except Exception as e:
            raise ValueError(f"No peaks found for {self}, error: {e}")

        # For debugging
        if False:
            logger.debug(f"Peaks found in {self}:\n {peaks}\n")
        return peaks
